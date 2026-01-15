/* global mapboxgl, MAPBOX_TOKEN */
const DATA = {
  hotspots: "./data/hotspots.geojson",
  routes: "./data/routes.geojson",
  metrics: "./data/walkforward_metrics.csv"
};

function el(id){ return document.getElementById(id); }
function fmt(x, d=2){
  if (x === null || x === undefined || Number.isNaN(x)) return "–";
  const n = Number(x);
  return Number.isFinite(n) ? n.toFixed(d) : String(x);
}

async function fetchJSON(url){
  const r = await fetch(url);
  if(!r.ok) throw new Error(`Failed to load ${url}: ${r.status}`);
  return await r.json();
}

async function fetchText(url){
  const r = await fetch(url);
  if(!r.ok) throw new Error(`Failed to load ${url}: ${r.status}`);
  return await r.text();
}

function parseCSV(text){
  const lines = text.trim().split(/\r?\n/);
  if(lines.length < 2) return [];
  const hdr = lines[0].split(",").map(s=>s.trim());
  const rows = [];
  for(let i=1;i<lines.length;i++){
    const parts = lines[i].split(","); // simple CSV (no embedded commas expected here)
    const obj = {};
    hdr.forEach((h,idx)=> obj[h]= parts[idx] ?? "");
    rows.push(obj);
  }
  return rows;
}

function buildMetricsUI(rows){
  if(!rows.length){
    el("modelSummary").textContent = "Metrics file not found (walkforward_metrics.csv).";
    return;
  }
  const byModel = {};
  rows.forEach(r=>{
    const m = r.model || r.Model || r.MODEL || "model";
    if(!byModel[m]) byModel[m]=[];
    byModel[m].push(r);
  });

  // Compute averages
  const summary = Object.entries(byModel).map(([m,arr])=>{
    const toNum = (k)=> arr.map(x=>Number(x[k])).filter(n=>Number.isFinite(n));
    const mean = (xs)=> xs.reduce((a,b)=>a+b,0)/Math.max(xs.length,1);
    return {
      model: m,
      folds: arr.length,
      avg_rmse: mean(toNum("rmse")),
      avg_mae: mean(toNum("mae")),
      avg_r2: mean(toNum("r2")),
      avg_geh: mean(toNum("geh_pct_lt5"))
    };
  }).sort((a,b)=>a.avg_rmse-b.avg_rmse);

  const best = summary[0];
  el("modelSummary").innerHTML = `Best avg RMSE: <b>${best.model}</b> (RMSE ${fmt(best.avg_rmse,2)}, R² ${fmt(best.avg_r2,3)}).`;

  const tbl = el("metricsTable");
  tbl.innerHTML = `
    <tr>
      <th>Model</th><th>Folds</th><th>RMSE</th><th>MAE</th><th>R²</th><th>GEH&lt;5 (%)</th>
    </tr>
    ${summary.map(s=>`
      <tr>
        <td>${s.model}</td>
        <td>${s.folds}</td>
        <td>${fmt(s.avg_rmse,2)}</td>
        <td>${fmt(s.avg_mae,2)}</td>
        <td>${fmt(s.avg_r2,3)}</td>
        <td>${fmt(s.avg_geh,2)}</td>
      </tr>`).join("")}
  `;
}

function calcHotKPI(hotspots, thr){
  const rates = hotspots.features.map(f => Number(f.properties?.hotspot_rate)).filter(Number.isFinite);
  const max = rates.length ? Math.max(...rates) : null;
  const count = rates.filter(r => r >= thr).length;
  return {max, count};
}

function populateRouteDropdowns(routes){
  const toNodes = Array.from(new Set(routes.features.map(f=>f.properties?.to_node).filter(Boolean))).sort();
  const routeTypes = Array.from(new Set(routes.features.map(f=>f.properties?.route).filter(Boolean))).sort();

  const toSel = el("toNodeSelect");
  toSel.innerHTML = toNodes.map(t=>`<option value="${t}">${t}</option>`).join("");

  const rtSel = el("routeTypeSelect");
  // keep ALL + append
  routeTypes.forEach(t=>{
    const opt=document.createElement("option");
    opt.value=t; opt.textContent=t;
    rtSel.appendChild(opt);
  });

  // pick first to-node by default
  toSel.value = toNodes[0] || "";
  rtSel.value = "ALL";
}

function routeFilterExpression(selectedTo, selectedType, onlySelected){
  let expr = ["all"];
  if(selectedTo){
    expr.push(["==", ["get","to_node"], selectedTo]);
  }
  if(selectedType && selectedType !== "ALL"){
    expr.push(["==", ["get","route"], selectedType]);
  }
  if(onlySelected){
    // still allow two route types if "ALL"
    // already handled above
  }
  return expr;
}

(async function main(){
  if(!MAPBOX_TOKEN || MAPBOX_TOKEN.includes("PASTE_")){
    alert("Set your Mapbox token in mapbox_dashboard/config.js (MAPBOX_TOKEN).");
    return;
  }
  mapboxgl.accessToken = MAPBOX_TOKEN;

  const [hotspots, routes] = await Promise.all([fetchJSON(DATA.hotspots), fetchJSON(DATA.routes)]).catch(err=>{
    alert(err.message);
    throw err;
  });

  // Load metrics (optional)
  fetchText(DATA.metrics).then(parseCSV).then(buildMetricsUI).catch(()=>{
    el("modelSummary").textContent = "Metrics file not found (walkforward_metrics.csv).";
  });

  // Map init
  const map = new mapboxgl.Map({
    container: "map",
    style: "mapbox://styles/mapbox/streets-v12",
    center: [-0.1276, 51.5072],
    zoom: 10.3
  });
  map.addControl(new mapboxgl.NavigationControl());
  const resizeMap = () => map.resize();
  window.addEventListener("resize", resizeMap);
  setTimeout(resizeMap, 0);

  const thrInput = el("thr");
  const thrLabel = el("thrLabel");
  const toggleHotspots = el("toggleHotspots");
  const toggleOnlySelected = el("toggleOnlySelected");
  const toSel = el("toNodeSelect");
  const rtSel = el("routeTypeSelect");

  // KPIs
  const renderHotKPIs = ()=>{
    const thr = Number(thrInput.value);
    const {max, count} = calcHotKPI(hotspots, thr);
    el("kpiHotCount").textContent = String(count);
    el("kpiMaxHot").textContent = fmt(max,2);
  };

  const setHotspotFilter = ()=>{
    const thr = Number(thrInput.value);
    thrLabel.textContent = fmt(thr,2);
    renderHotKPIs();
    if(map.getLayer("hotspots")){
      map.setFilter("hotspots", [">=", ["get","hotspot_rate"], thr]);
    }
  };

  const setRoutesFilter = ()=>{
    const toNode = toSel.value;
    const routeType = rtSel.value;
    const onlySel = toggleOnlySelected.checked;
    const expr = routeFilterExpression(toNode, routeType, onlySel);
    if(map.getLayer("routes")){
      map.setFilter("routes", expr);
    }
    // Update KPIs using first matching route feature
    const match = routes.features.find(f=>{
      const p=f.properties||{};
      if(toNode && p.to_node !== toNode) return false;
      if(routeType!=="ALL" && p.route !== routeType) return false;
      return true;
    });
    if(match){
      el("kpiLen").textContent = fmt(match.properties.length_km,2);
      el("kpiWeight").textContent = fmt(match.properties.weight_km,2);
    }else{
      el("kpiLen").textContent = "–";
      el("kpiWeight").textContent = "–";
    }
  };

  populateRouteDropdowns(routes);

  map.on("load", ()=>{
    // Sources
    map.addSource("hotspots-src", { type:"geojson", data: hotspots });
    map.addSource("routes-src", { type:"geojson", data: routes });

    // Hotspots layer
    map.addLayer({
      id: "hotspots",
      type: "line",
      source: "hotspots-src",
      paint: {
        "line-width": [
          "interpolate", ["linear"], ["get","hotspot_rate"],
          0.0, 1.0,
          1.0, 5.0
        ],
        "line-color": [
          "interpolate", ["linear"], ["get","hotspot_rate"],
          0.0, "#2dd4bf",
          0.5, "#a78bfa",
          1.0, "#fb7185"
        ],
        "line-opacity": 0.85
      }
    });

    // Routes layer (constant colour, thicker)
    map.addLayer({
      id: "routes",
      type: "line",
      source: "routes-src",
      paint: {
        "line-width": 4,
        "line-color": "#60a5fa",
        "line-opacity": 0.85
      }
    });

    // Interactions: popups
    const popup = new mapboxgl.Popup({ closeButton: false, closeOnClick: false });

    map.on("mousemove", "hotspots", (e)=>{
      map.getCanvas().style.cursor = "pointer";
      const f = e.features?.[0];
      if(!f) return;
      const p = f.properties || {};
      popup.setLngLat(e.lngLat)
        .setHTML(`<b>Hotspot edge</b><br/>${p.from_node} → ${p.to_node}<br/>hotspot_rate: <b>${fmt(p.hotspot_rate,2)}</b>`)
        .addTo(map);
    });
    map.on("mouseleave", "hotspots", ()=>{
      map.getCanvas().style.cursor = "";
      popup.remove();
    });

    map.on("click", "routes", (e)=>{
      const f = e.features?.[0];
      if(!f) return;
      const p=f.properties||{};
      new mapboxgl.Popup()
        .setLngLat(e.lngLat)
        .setHTML(`<b>Route</b><br/>${p.from_node} → ${p.to_node}<br/>type: <b>${p.route}</b><br/>len_km: ${fmt(p.length_km,2)}<br/>weight_km: ${fmt(p.weight_km,2)}`)
        .addTo(map);
    });

    // Wire controls
    renderHotKPIs();
    setHotspotFilter();
    setRoutesFilter();

    thrInput.addEventListener("input", setHotspotFilter);
    toggleHotspots.addEventListener("change", ()=>{
      if(map.getLayer("hotspots")){
        map.setLayoutProperty("hotspots","visibility", toggleHotspots.checked ? "visible" : "none");
      }
    });
    toSel.addEventListener("change", setRoutesFilter);
    rtSel.addEventListener("change", setRoutesFilter);
    toggleOnlySelected.addEventListener("change", setRoutesFilter);
  });
})();
