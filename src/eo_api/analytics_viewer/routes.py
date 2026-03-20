"""Pluggable time-aware analytics viewer routes."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse

from ..publications import services as publication_services
from ..publications.schemas import PublishedResourceKind
from ..shared.api_errors import api_error

router = APIRouter()


@router.get("/publications/{resource_id}")
def get_publication_analytics_config(resource_id: str) -> dict[str, Any]:
    """Return viewer configuration for one published resource."""
    resource = publication_services.get_published_resource(resource_id)
    if resource is None:
        raise HTTPException(
            status_code=404,
            detail=api_error(
                error="published_resource_not_found",
                error_code="PUBLISHED_RESOURCE_NOT_FOUND",
                message=f"Unknown resource_id '{resource_id}'",
                resource_id=resource_id,
            ),
        )
    if resource.kind != PublishedResourceKind.FEATURE_COLLECTION or resource.path is None:
        raise HTTPException(
            status_code=409,
            detail=api_error(
                error="analytics_target_invalid",
                error_code="ANALYTICS_TARGET_INVALID",
                message=f"Resource '{resource_id}' is not a feature collection viewer target",
                resource_id=resource_id,
            ),
        )

    data_url = _data_url_for_path(resource.path)
    return {
        "resource_id": resource.resource_id,
        "title": resource.title,
        "description": resource.description,
        "dataset_id": resource.dataset_id,
        "workflow_id": resource.workflow_id,
        "job_id": resource.job_id,
        "data_url": data_url,
        "ogc_items_url": f"/pygeoapi/collections/{resource.resource_id}/items",
        "links": {
            "ogc_home": "/ogcapi",
            "publication": f"/publications/{resource.resource_id}",
            "collection": f"/pygeoapi/collections/{resource.resource_id}",
            "items": f"/pygeoapi/collections/{resource.resource_id}/items",
        },
    }


@router.get("/publications/{resource_id}/viewer", response_class=HTMLResponse)
def get_publication_analytics_viewer(resource_id: str, embed: bool = False) -> HTMLResponse:
    """Return an interactive time-aware analytics viewer for one published resource."""
    config = get_publication_analytics_config(resource_id)
    return HTMLResponse(_render_viewer_html(config, embed=embed))


def _data_url_for_path(path_value: str) -> str:
    path = Path(path_value).resolve()
    downloads_root = publication_services.DOWNLOAD_DIR.resolve()
    if downloads_root not in path.parents:
        raise HTTPException(
            status_code=409,
            detail=api_error(
                error="published_asset_invalid",
                error_code="PUBLISHED_ASSET_INVALID",
                message="Published resource path is outside mounted download storage",
            ),
        )
    relative_path = path.relative_to(downloads_root).as_posix()
    return f"/data/{relative_path}"


def _render_viewer_html(config: dict[str, Any], *, embed: bool = False) -> str:
    config_json = json.dumps(config)
    shell_padding = "0" if embed else "28px 24px 40px"
    shell_max_width = "100%" if embed else "1440px"
    shell_margin = "0" if embed else "0 auto"
    body_background = (
        "transparent"
        if embed
        else """
        radial-gradient(circle at top left, rgba(221, 141, 85, 0.18), transparent 32%),
        radial-gradient(circle at right, rgba(65, 130, 180, 0.16), transparent 28%),
        linear-gradient(180deg, #f8f4ee 0%, var(--bg) 100%)
    """
    )
    hero_html = (
        ""
        if embed
        else f"""
      <nav class="topnav" aria-label="Viewer navigation">
        <a href="{config["links"]["ogc_home"]}">OGC Home</a>
        <a href="{config["links"]["collection"]}">Collection</a>
        <a href="{config["links"]["items"]}">Items</a>
        <a href="{config["links"]["publication"]}">Publication</a>
      </nav>
      <div class="eyebrow">Analytics Viewer</div>
      <h1>{config["title"]}</h1>
      <p class="subhead">
        Time-aware choropleth view over the published workflow output. This viewer is intentionally isolated from the
        OGC/publication core so it can be swapped or removed without changing the publication contract.
      </p>
        """
    )
    return f"""<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{config["title"]} Analytics Viewer</title>
    <link
      rel="stylesheet"
      href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
      integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY="
      crossorigin=""
    >
    <script
      src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
      integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo="
      crossorigin=""
    ></script>
    <style>
      :root {{
        --bg: #f4efe6;
        --panel: rgba(255, 250, 242, 0.94);
        --ink: #172033;
        --muted: #5f6c80;
        --line: rgba(23, 32, 51, 0.14);
        --accent: #ab3b1f;
        --accent-soft: #dd8d55;
      }}
      * {{ box-sizing: border-box; }}
      body {{
        margin: 0;
        font-family: "IBM Plex Sans", "Avenir Next", "Segoe UI", sans-serif;
        color: var(--ink);
        background: {body_background};
      }}
      .shell {{
        max-width: {shell_max_width};
        margin: {shell_margin};
        padding: {shell_padding};
      }}
      .eyebrow {{
        display: inline-flex;
        gap: 8px;
        align-items: center;
        padding: 6px 12px;
        border-radius: 999px;
        background: rgba(171, 59, 31, 0.08);
        color: var(--accent);
        font-size: 12px;
        font-weight: 700;
        letter-spacing: 0.08em;
        text-transform: uppercase;
      }}
      .topnav {{
        display: flex;
        flex-wrap: wrap;
        gap: 10px;
        margin-bottom: 14px;
      }}
      .topnav a {{
        display: inline-flex;
        align-items: center;
        padding: 8px 12px;
        border-radius: 999px;
        background: rgba(255, 255, 255, 0.78);
        border: 1px solid var(--line);
        color: var(--ink);
        text-decoration: none;
        font-size: 0.92rem;
        font-weight: 600;
      }}
      h1 {{
        margin: 14px 0 8px;
        font-size: clamp(2rem, 4vw, 3.6rem);
        line-height: 1.02;
        letter-spacing: -0.04em;
      }}
      .subhead {{
        max-width: 900px;
        color: var(--muted);
        font-size: 1rem;
      }}
      .layout {{
        display: grid;
        grid-template-columns: minmax(0, 1.6fr) minmax(320px, 0.9fr);
        gap: 18px;
        margin-top: 24px;
      }}
      .panel {{
        background: var(--panel);
        border: 1px solid var(--line);
        border-radius: 24px;
        box-shadow: 0 24px 80px rgba(23, 32, 51, 0.08);
        overflow: hidden;
      }}
      .map-wrap {{
        display: grid;
        grid-template-rows: auto 1fr;
        min-height: 760px;
      }}
      .controls {{
        display: grid;
        grid-template-columns: auto 1fr auto auto;
        gap: 12px;
        align-items: center;
        padding: 16px 18px;
        border-bottom: 1px solid var(--line);
        background: rgba(255, 255, 255, 0.58);
      }}
      .controls button, .controls select {{
        border: 1px solid var(--line);
        background: white;
        color: var(--ink);
        border-radius: 12px;
        padding: 10px 14px;
        font: inherit;
      }}
      .controls button {{
        background: linear-gradient(135deg, var(--accent) 0%, var(--accent-soft) 100%);
        color: white;
        border: none;
        font-weight: 700;
      }}
      .controls input[type="range"] {{
        width: 100%;
      }}
      #map {{
        min-height: 640px;
      }}
      .side {{
        display: grid;
        grid-template-rows: auto auto 1fr;
      }}
      .section {{
        padding: 18px 20px;
        border-bottom: 1px solid var(--line);
      }}
      .section:last-child {{
        border-bottom: none;
      }}
      .stats {{
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 12px;
      }}
      .stat {{
        padding: 14px;
        border-radius: 18px;
        background: rgba(255, 255, 255, 0.72);
        border: 1px solid var(--line);
      }}
      .stat .label {{
        font-size: 0.78rem;
        color: var(--muted);
        text-transform: uppercase;
        letter-spacing: 0.08em;
      }}
      .stat .value {{
        margin-top: 6px;
        font-size: 1.5rem;
        font-weight: 700;
      }}
      .legend-bar {{
        height: 16px;
        border-radius: 999px;
        background: linear-gradient(90deg, #eef6ff 0%, #b8dcff 35%, #5fa6f2 68%, #124fa3 100%);
        border: 1px solid var(--line);
      }}
      .legend-scale {{
        display: flex;
        justify-content: space-between;
        margin-top: 8px;
        color: var(--muted);
        font-size: 0.88rem;
      }}
      table {{
        width: 100%;
        border-collapse: collapse;
      }}
      th, td {{
        padding: 10px 0;
        border-bottom: 1px solid var(--line);
        text-align: left;
        vertical-align: top;
      }}
      th {{
        color: var(--muted);
        font-size: 0.82rem;
        text-transform: uppercase;
        letter-spacing: 0.08em;
      }}
      .table-wrap {{
        max-height: 320px;
        overflow: auto;
      }}
      .empty {{
        color: var(--muted);
        font-style: italic;
      }}
      .links {{
        display: flex;
        flex-wrap: wrap;
        gap: 10px;
        margin-top: 10px;
      }}
      .links a {{
        color: var(--accent);
        text-decoration: none;
        font-weight: 600;
      }}
      @media (max-width: 1040px) {{
        .layout {{
          grid-template-columns: 1fr;
        }}
        .map-wrap {{
          min-height: 620px;
        }}
        #map {{
          min-height: 500px;
        }}
        .controls {{
          grid-template-columns: 1fr;
        }}
      }}
    </style>
  </head>
  <body>
    <div class="shell">
      {hero_html}
      <div class="layout">
        <section class="panel map-wrap">
          <div class="controls">
            <button id="playButton" type="button">Play</button>
            <input id="periodSlider" type="range" min="0" max="0" step="1" value="0">
            <select id="periodSelect"></select>
            <div id="periodLabel"></div>
          </div>
          <div id="map"></div>
        </section>
        <aside class="panel side">
          <section class="section">
            <div class="stats">
              <div class="stat"><div class="label">Dataset</div><div class="value" id="datasetId">-</div></div>
              <div class="stat"><div class="label">Periods</div><div class="value" id="periodCount">-</div></div>
              <div class="stat"><div class="label">Current Mean</div><div class="value" id="meanValue">-</div></div>
              <div class="stat"><div class="label">Current Max</div><div class="value" id="maxValue">-</div></div>
            </div>
          </section>
          <section class="section">
            <div class="label">Color Scale</div>
            <div class="legend-bar"></div>
            <div class="legend-scale">
              <span id="legendMin">-</span>
              <span id="legendMax">-</span>
            </div>
            <div class="links">
              <a href="{config["links"]["collection"]}">OGC Collection</a>
              <a href="{config["links"]["items"]}">OGC Items</a>
              <a href="{config["links"]["publication"]}">Publication Record</a>
            </div>
          </section>
          <section class="section">
            <div class="label">Current Period Top Values</div>
            <div class="table-wrap">
              <table>
                <thead>
                  <tr><th>Org Unit</th><th>Period</th><th>Value</th></tr>
                </thead>
                <tbody id="summaryTable"></tbody>
              </table>
            </div>
          </section>
        </aside>
      </div>
    </div>
    <script>
      const VIEWER_CONFIG = {config_json};
      const map = L.map("map", {{ zoomSnap: 0.25, preferCanvas: true }});
      L.tileLayer("https://tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png", {{
        attribution: "&copy; OpenStreetMap contributors"
      }}).addTo(map);

      let playbackTimer = null;
      let geoJsonLayer = null;
      let grouped = new Map();
      let periods = [];
      let globalMin = 0;
      let globalMax = 1;

      const playButton = document.getElementById("playButton");
      const periodSlider = document.getElementById("periodSlider");
      const periodSelect = document.getElementById("periodSelect");
      const periodLabel = document.getElementById("periodLabel");

      function formatValue(value) {{
        return Number(value).toLocaleString(undefined, {{ maximumFractionDigits: 2 }});
      }}

      function colorFor(value) {{
        if (globalMax <= globalMin) return "#5fa6f2";
        const t = (value - globalMin) / (globalMax - globalMin);
        if (t < 0.2) return "#eef6ff";
        if (t < 0.4) return "#b8dcff";
        if (t < 0.6) return "#7fc0ff";
        if (t < 0.8) return "#5fa6f2";
        return "#124fa3";
      }}

      function updateSummary(features, period) {{
        document.getElementById("datasetId").textContent = VIEWER_CONFIG.dataset_id || "Derived";
        document.getElementById("periodCount").textContent = String(periods.length);
        periodLabel.textContent = period || "No period";
        document.getElementById("legendMin").textContent = formatValue(globalMin);
        document.getElementById("legendMax").textContent = formatValue(globalMax);

        if (!features.length) {{
          document.getElementById("meanValue").textContent = "-";
          document.getElementById("maxValue").textContent = "-";
          document.getElementById("summaryTable").innerHTML =
            '<tr><td colspan="3" class="empty">No data for selected period</td></tr>';
          return;
        }}

        const values = features.map((feature) => Number(feature.properties.value));
        const mean = values.reduce((sum, value) => sum + value, 0) / values.length;
        const max = Math.max(...values);
        document.getElementById("meanValue").textContent = formatValue(mean);
        document.getElementById("maxValue").textContent = formatValue(max);

        const topRows = [...features]
          .sort((a, b) => Number(b.properties.value) - Number(a.properties.value))
          .slice(0, 8)
          .map((feature) => {{
            const name = feature.properties.org_unit_name || feature.properties.org_unit;
            const value = formatValue(feature.properties.value);
            return `<tr><td>${{name}}</td><td>${{feature.properties.period}}</td><td>${{value}}</td></tr>`;
          }})
          .join("");
        document.getElementById("summaryTable").innerHTML = topRows;
      }}

      function renderPeriod(index) {{
        const period = periods[index];
        const features = grouped.get(period) || [];
        periodSlider.value = String(index);
        periodSelect.value = period || "";

        if (geoJsonLayer) {{
          geoJsonLayer.remove();
        }}

        geoJsonLayer = L.geoJSON({{ type: "FeatureCollection", features }}, {{
          style: (feature) => ({{
            color: "#3478f6",
            weight: 1.4,
            fillColor: colorFor(Number(feature.properties.value)),
            fillOpacity: 0.76,
          }}),
          onEachFeature: (feature, layer) => {{
            const name = feature.properties.org_unit_name || feature.properties.org_unit;
            const value = formatValue(feature.properties.value);
            layer.bindTooltip(
              `<strong>${{name}}</strong><br>Period: ${{feature.properties.period}}<br>Value: ${{value}}`,
              {{ sticky: true }}
            );
          }},
        }}).addTo(map);

        if (features.length) {{
          map.fitBounds(geoJsonLayer.getBounds(), {{ padding: [16, 16], maxZoom: 8 }});
        }}

        updateSummary(features, period);
      }}

      function togglePlayback() {{
        if (playbackTimer) {{
          window.clearInterval(playbackTimer);
          playbackTimer = null;
          playButton.textContent = "Play";
          return;
        }}
        playButton.textContent = "Pause";
        playbackTimer = window.setInterval(() => {{
          const nextIndex = (Number(periodSlider.value) + 1) % periods.length;
          renderPeriod(nextIndex);
        }}, 1100);
      }}

      async function boot() {{
        const response = await fetch(VIEWER_CONFIG.data_url);
        const collection = await response.json();
        const features = collection.features || [];

        globalMin = Math.min(...features.map((feature) => Number(feature.properties.value)));
        globalMax = Math.max(...features.map((feature) => Number(feature.properties.value)));

        for (const feature of features) {{
          const period = feature.properties.period || "unknown";
          if (!grouped.has(period)) grouped.set(period, []);
          grouped.get(period).push(feature);
        }}

        periods = [...grouped.keys()].sort();
        periodSlider.max = String(Math.max(0, periods.length - 1));
        periodSelect.innerHTML = periods.map((period) => `<option value="${{period}}">${{period}}</option>`).join("");

        periodSlider.addEventListener("input", (event) => renderPeriod(Number(event.target.value)));
        periodSelect.addEventListener("change", (event) => renderPeriod(periods.indexOf(event.target.value)));
        playButton.addEventListener("click", togglePlayback);

        if (!periods.length) {{
          map.setView([8.4, -11.7], 6);
          updateSummary([], null);
          return;
        }}
        renderPeriod(0);
      }}

      boot();
    </script>
  </body>
</html>"""
