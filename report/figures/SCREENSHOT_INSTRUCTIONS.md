# Screenshot Instructions for Dissertation

## Required Screenshots

To complete the dissertation, you need to add the following screenshots to this `report/figures/` directory:

### 1. Airflow DAG Interface
**Filename:** `airflow_dag_interface.png`
**How to capture:**
1. Start Airflow: `docker-compose up` in the `airflow/` directory
2. Open browser: http://localhost:8080
3. Login with credentials (airflow/airflow)
4. Navigate to the "traffic_congestion_london_pipeline" DAG
5. Take a screenshot showing:
   - The DAG graph view with all 7 steps visible
   - Task dependencies clearly shown
   - Task status indicators
6. Save as `airflow_dag_interface.png` (1920x1080 recommended)

### 2. Mapbox Dashboard - Hotspots View
**Filename:** `mapbox_dashboard_hotspots.png`
**How to capture:**
1. Open the dashboard: https://akankshagowri.github.io/Traffic_prediction/
2. Ensure the hotspots layer is visible
3. Zoom to show London road network with color-coded hotspot rates
4. Take a screenshot showing:
   - Map with hotspot visualization
   - Legend showing hotspot rate scale
   - UI controls (threshold slider, layer toggles)
5. Save as `mapbox_dashboard_hotspots.png` (1920x1080 recommended)

### 3. Mapbox Dashboard - Routes View
**Filename:** `mapbox_dashboard_routes.png`
**How to capture:**
1. On the same dashboard, switch to routes view
2. Show comparison between distance-only and congestion-aware routes
3. Take a screenshot showing:
   - Both route types overlaid on map
   - Route statistics panel
   - Clear visual difference between route types
4. Save as `mapbox_dashboard_routes.png` (1920x1080 recommended)

### 4. Power BI Dashboard
**Filename:** `powerbi_dashboard_overview.png`
**How to capture:**
1. Open Power BI Desktop
2. Load the template: `airflow/powerbi_inputs/Narendra.pbit`
3. Connect to data sources:
   - `routing_nodes_London.csv`
   - `dashboard_data_London/` folder
   - `route_analysis_London/` folder
4. Take a screenshot showing:
   - Main dashboard view with multiple visualizations
   - Time series charts
   - Spatial visualizations
   - Key metrics/KPIs
5. Save as `powerbi_dashboard_overview.png` (1920x1080 recommended)

## Image Specifications

- **Format:** PNG (preferred) or JPG
- **Resolution:** 1920x1080 or higher
- **File size:** Keep under 5MB per image
- **Quality:** High quality, clear text and labels
- **Annotations:** Optional - add arrows or callouts to highlight key features

## After Adding Screenshots

Once you've added the screenshots to this directory, uncomment the figure blocks in the LaTeX files:

### In `report/chapters/05_system_design.tex`:

Find and uncomment these blocks:

```latex
% \begin{figure}[H]
% \centering
% \includegraphics[width=0.9\textwidth]{figures/airflow_dag_interface.png}
% \caption{Airflow DAG interface showing the 7-step traffic pipeline orchestration.}
% \label{fig:airflow_dag}
% \end{figure}

% \begin{figure}[H]
% \centering
% \includegraphics[width=0.9\textwidth]{figures/mapbox_dashboard_hotspots.png}
% \caption{Mapbox dashboard showing traffic hotspots across London road network.}
% \label{fig:mapbox_hotspots}
% \end{figure}

% \begin{figure}[H]
% \centering
% \includegraphics[width=0.9\textwidth]{figures/mapbox_dashboard_routes.png}
% \caption{Route comparison: distance-only vs congestion-aware routing.}
% \label{fig:mapbox_routes}
% \end{figure}

% \begin{figure}[H]
% \centering
% \includegraphics[width=0.9\textwidth]{figures/powerbi_dashboard_overview.png}
% \caption{Power BI dashboard providing desktop analytics interface.}
% \label{fig:powerbi_dashboard}
% \end{figure}
```

## Verification

After adding screenshots and uncommenting the figure blocks:
1. Recompile the PDF
2. Check that all figures appear correctly
3. Verify figure numbering is sequential
4. Ensure captions are descriptive
5. Check that figures are referenced in the text

## Alternative: Placeholder Images

If screenshots are not immediately available, you can create placeholder images:
1. Create blank images with text indicating "Screenshot placeholder"
2. Use the same filenames as above
3. Replace with actual screenshots before final submission
