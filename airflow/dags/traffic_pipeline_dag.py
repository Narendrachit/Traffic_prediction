"""
traffic_pipeline_dag.py

End-to-end Airflow DAG for the MSc "Predictive Analytics for Traffic Congestion in Urban Areas: A London Case Study".
Runs scripts 01→07 inside the `traffic-runner:latest` container and writes outputs to the shared Docker volume
`traffic_data_lake` mounted at /data_lake.

Assumptions (matches your manual docker commands):
- Docker volume `traffic_data_lake` exists and is mounted to /data_lake
- Docker volume `traffic_scripts` exists and contains scripts 01..07 mounted to /opt/user_scripts
- Image `traffic-runner:latest` exists (built by your project)

You can trigger with conf like:
{"region":"London","hotspot_quantile":0.60,"hotspot_threshold":0.20,"from_node":"A1009","max_targets":30}

"""

from __future__ import annotations

from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


# ---------- Defaults ----------
DAG_ID = "traffic_congestion_london_pipeline"

DEFAULT_REGION = "London"
DEFAULT_HOTSPOT_QUANTILE = 0.60
DEFAULT_HOTSPOT_THRESHOLD = 0.20
DEFAULT_FROM_NODE = "A1009"
DEFAULT_MAX_TARGETS = 30
DEFAULT_ALPHA = 2.0

DEFAULT_IN_CSV_DIR = "/data_lake/bronze/dft_raw_counts_csv/region=London"

# Paths in the data lake
SILVER_CLEANED_PARQUET = "/data_lake/silver/cleaned_parquet"
GOLD_FEATURES_HOURLY = "/data_lake/gold/features_hourly"
GOLD_MODELS_WALKFORWARD = "/data_lake/gold/models_walkforward"
GOLD_DASHBOARD_DATA = "/data_lake/gold/dashboard_data"
GOLD_ROUTING_GRAPH_WEIGHTED = "/data_lake/gold/routing_graph_weighted"
GOLD_EXPORTS = "/data_lake/gold/exports"

# Container + mounts (named Docker volumes)
RUNNER_IMAGE = "traffic-runner:latest"
MOUNTS = [
    Mount(source="traffic_data_lake", target="/data_lake", type="volume"),
    Mount(source="traffic_scripts", target="/opt/user_scripts", type="volume"),
]


def runner_task(
    *,
    task_id: str,
    command: list[str],
    retries: int = 1,
    retry_delay_minutes: int = 2,
) -> DockerOperator:
    """Standardised DockerOperator for traffic-runner scripts."""
    return DockerOperator(
        task_id=task_id,
        image=RUNNER_IMAGE,
        api_version="auto",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        command=command,
        mounts=MOUNTS,
        auto_remove=True,
        mount_tmp_dir=False,  # important for Windows Docker Desktop setups
        tty=True,
        do_xcom_push=False,
        retries=retries,
        retry_delay=timedelta(minutes=retry_delay_minutes),
        environment={"PYTHONUNBUFFERED": "1"},
    )


with DAG(
    dag_id=DAG_ID,
    description="London traffic congestion pipeline (01→07) + stable exports",
    start_date=pendulum.datetime(2025, 10, 15, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args={"owner": "narendra", "retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["traffic", "london", "msc", "airflow", "spark"],
) as dag:
    # --- Jinja-templated runtime params ---
    REGION = "{{ dag_run.conf.get('region', 'London') }}"
    HOTSPOT_QUANTILE = "{{ dag_run.conf.get('hotspot_quantile', 0.60) }}"
    HOTSPOT_THRESHOLD = "{{ dag_run.conf.get('hotspot_threshold', 0.20) }}"
    FROM_NODE = "{{ dag_run.conf.get('from_node', 'A1009') }}"
    MAX_TARGETS = "{{ dag_run.conf.get('max_targets', 30) }}"
    ALPHA = "{{ dag_run.conf.get('alpha', 2.0) }}"
    IN_CSV_DIR = "{{ dag_run.conf.get('in_csv_dir', '/data_lake/bronze/dft_raw_counts_csv/region=London') }}"

    # ---------------------------------------------------------------------
    # Step 01: Clean CSV → Parquet (silver)
    # ---------------------------------------------------------------------
    step01_clean_csv_to_parquet = runner_task(
        task_id="step01_clean_csv_to_parquet",
        command=[
            "/opt/user_scripts/01_xlsx_to_csv_london.py",
            "--in_csv",
            IN_CSV_DIR,
            "--out_parquet",
            SILVER_CLEANED_PARQUET,
        ],
        retries=0,
    )

    # ---------------------------------------------------------------------
    # Step 02–03: Gold features + model training (walk-forward)
    # ---------------------------------------------------------------------
    with TaskGroup(group_id="gold_features_and_models") as gold_features_and_models:
        step02_build_gold_features = runner_task(
            task_id="step02_build_gold_features",
            command=[
                "/opt/user_scripts/02_build_gold_features_spark.py",
                "--in_parquet",
                "/data_lake/silver/cleaned_parquet/region={{ dag_run.conf.get('region', 'London') }}",
                "--out_gold",
                GOLD_FEATURES_HOURLY,
                "--region",
                REGION,
                "--horizon_hours",
                "1",
                "--driver_memory",
                "6g",
                "--shuffle_partitions",
                "200",
            ],
        )

        step03_train_models_walkforward = runner_task(
            task_id="step03_train_models_walkforward",
            command=[
                "/opt/user_scripts/03_train_models_walkforward.py",
                "--gold_parquet",
                "/data_lake/gold/features_hourly/region={{ dag_run.conf.get('region', 'London') }}",
                "--out_dir",
                "/data_lake/gold/models_walkforward/region={{ dag_run.conf.get('region', 'London') }}",
                "--label_col",
                "y_t_plus_1h",
                "--n_folds",
                "3",
                "--test_days",
                "3",
                "--driver_memory",
                "6g",
                "--shuffle_partitions",
                "16",
                "--enable_gbt",
                "0",
                "--enable_rf",
                "0",
                "--max_train_rows",
                "50000",
            ],
        )

        step02_build_gold_features >> step03_train_models_walkforward

    # ---------------------------------------------------------------------
    # Step 04: Build routing graph (writes directly to /data_lake/gold/exports)
    # ---------------------------------------------------------------------
    with TaskGroup(group_id="routing_graph") as routing_graph:
        step04_build_routing_graph = runner_task(
            task_id="step04_build_routing_graph",
            command=[
                "/opt/user_scripts/04_build_routing_graph.py",
                "--parquet_path",
                "/data_lake/gold/features_hourly/region={{ dag_run.conf.get('region', 'London') }}",
                "--out_dir",
                GOLD_EXPORTS,
            ],
        )

    # ---------------------------------------------------------------------
    # Step 05: Dashboard outputs (edge hotspot rate, time summary, etc.)
    # ---------------------------------------------------------------------
    with TaskGroup(group_id="dashboard_outputs") as dashboard_outputs:
        step05_make_dashboard_data = runner_task(
            task_id="step05_make_dashboard_data",
            command=[
                "/opt/user_scripts/05_make_dashboard_data.py",
                "--gold_parquet",
                "/data_lake/gold/features_hourly/region={{ dag_run.conf.get('region', 'London') }}",
                "--out_dir",
                "/data_lake/gold/dashboard_data/region={{ dag_run.conf.get('region', 'London') }}",
                "--hotspot_quantile",
                HOTSPOT_QUANTILE,
                "--score_days",
                "7",
                "--max_train_rows",
                "50000",
                "--gbt_maxIter",
                "20",
                "--gbt_maxDepth",
                "4",
                "--driver_memory",
                "6g",
                "--shuffle_partitions",
                "64",
            ],
        )

    # ---------------------------------------------------------------------
    # Step 06: Weighted routing edges (penalise hotspot edges)
    # ---------------------------------------------------------------------
    with TaskGroup(group_id="weighted_routing") as weighted_routing:
        step06_build_weighted_routing_graph = runner_task(
            task_id="step06_build_weighted_routing_graph",
            command=[
                "/opt/user_scripts/06_build_weighted_routing_graph.py",
                "--routing_edges_csv",
                "/data_lake/gold/exports/routing_edges.csv",
                "--edge_hotspot_rate_csv",
                "/data_lake/gold/dashboard_data/region={{ dag_run.conf.get('region', 'London') }}/edge_hotspot_rate.csv",
                "--out_dir",
                "/data_lake/gold/routing_graph_weighted/region={{ dag_run.conf.get('region', 'London') }}",
                "--alpha",
                ALPHA,
            ],
        )

    # ---------------------------------------------------------------------
    # Step 07: Route demo (bulk) + copy latest outputs to exports
    # ---------------------------------------------------------------------
    with TaskGroup(group_id="route_demo") as route_demo:
        step07_route_demo_bulk = runner_task(
            task_id="step07_route_demo_bulk",
            command=[
                "/opt/user_scripts/07_route_demo.py",
                "--region",
                REGION,
                "--data_lake",
                "/data_lake",
                "--from_node",
                FROM_NODE,
                "--all_to_nodes",
                "--max_targets",
                MAX_TARGETS,
                "--hotspot_threshold",
                HOTSPOT_THRESHOLD,
                "--export_edges",
            ],
        )

        export_latest_routes_to_exports = runner_task(
            task_id="export_latest_routes_to_exports",
            command=[
                "-c",
                r"""
import os, glob, shutil
base = "/data_lake/gold/route_analysis/region=London"
runs = sorted(glob.glob(os.path.join(base, "run_utc=*")))
if not runs:
    raise SystemExit(f"No run_utc folders under {base}. Did step07 run?")
latest = runs[-1]
os.makedirs("/data_lake/gold/exports", exist_ok=True)

pairs = [
    ("routes_summary.csv", "routes_summary.csv"),
    ("routes_edges.csv", "routes_edges.csv"),
    ("route_summary.csv", "route_summary.csv"),
    ("route_edges.csv", "route_edges.csv"),
]
copied = []
for src_name, dst_name in pairs:
    src = os.path.join(latest, src_name)
    dst = os.path.join("/data_lake/gold/exports", dst_name)
    if os.path.exists(src):
        shutil.copy2(src, dst)
        copied.append(dst_name)

print("Latest run:", latest)
print("Copied to /data_lake/gold/exports:", ", ".join(copied) if copied else "(nothing)")
""",
            ],
            retries=0,
        )

        step07_route_demo_bulk >> export_latest_routes_to_exports

    # ---------------------------------------------------------------------
    # Dependencies
    # ---------------------------------------------------------------------
    step01_clean_csv_to_parquet >> gold_features_and_models
    gold_features_and_models >> routing_graph
    routing_graph >> dashboard_outputs
    dashboard_outputs >> weighted_routing
    weighted_routing >> route_demo
