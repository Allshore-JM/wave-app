(function () {
  "use strict";

  const FT_PER_M = 3.280839895;
  const API_URL = "/api/live-buoys/global?sources=ndbc,ireland,canada&max_age_hours=120";

  let allBuoys = [];
  let enabledSources = new Set();
  let liveLayer = null;
  let controlRoot = null;
  let lastWorldOffset = null;

  function getMap() {
    if (window.allshoreMap) return window.allshoreMap;

    try {
      if (typeof map !== "undefined") return map;
    } catch (e) {}

    return null;
  }

  function escapeHtml(value) {
    if (value === null || value === undefined) return "";
    return String(value)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#039;");
  }

  function selectedUnitSystem() {
    const unit = document.getElementById("unit")?.value || "US";
    return unit === "Metric" ? "Metric" : "US";
  }

  function fmtNumber(value, digits = 1) {
    const n = Number(value);
    if (!Number.isFinite(n)) return "—";
    return n.toFixed(digits);
  }

  function fmtHeightM(meters) {
    const n = Number(meters);
    if (!Number.isFinite(n)) return "—";

    if (selectedUnitSystem() === "Metric") {
      return `${n.toFixed(2)} m`;
    }

    return `${(n * FT_PER_M).toFixed(2)} ft`;
  }

  function fmtWindMps(mps) {
    const n = Number(mps);
    if (!Number.isFinite(n)) return "—";

    if (selectedUnitSystem() === "Metric") {
      return `${n.toFixed(1)} m/s`;
    }

    return `${(n * 1.94384449).toFixed(1)} kt`;
  }

  function fmtAge(ageMinutes) {
    const age = Number(ageMinutes);
    if (!Number.isFinite(age)) return "unknown age";

    if (age < 60) return `${Math.round(age)} min old`;

    const hours = age / 60;
    if (hours < 48) return `${hours.toFixed(1)} hr old`;

    return `${(hours / 24).toFixed(1)} days old`;
  }

  function fmtDate(iso) {
    if (!iso) return "—";

    try {
      const d = new Date(iso);
      return d.toLocaleString();
    } catch (e) {
      return iso;
    }
  }

  function statusClass(buoy) {
    if (buoy.status === "active") return "fresh";
    if (buoy.status === "stale") return "stale";
    if (buoy.status === "old") return "old";
    return "unknown";
  }

  function markerStyle(buoy) {
    const status = statusClass(buoy);

    let color = "#19c37d";
    if (status === "stale") color = "#f5a623";
    if (status === "old") color = "#8b949e";
    if (status === "unknown") color = "#58a6ff";

    const hs = Number(buoy.wave?.significant_height_m);
    let radius = 5;

    if (Number.isFinite(hs)) {
      radius = Math.max(4, Math.min(12, 4 + hs * 1.8));
    }

    return {
      radius,
      color: "#ffffff",
      fillColor: color,
      fillOpacity: 0.95,
      weight: 1.5,
    };
  }

  function currentWorldOffset(mapObj) {
    return Math.round(mapObj.getCenter().lng / 360) * 360;
  }

  function wrappedLongitudes(lon, mapObj) {
    const base = Number(lon);
    if (!Number.isFinite(base)) return [];

    const offset = currentWorldOffset(mapObj);
    return [base + offset - 360, base + offset, base + offset + 360];
  }

  function popupHtml(buoy) {
    const wave = buoy.wave || {};
    const wind = buoy.wind || {};
    const dir =
      wave.peak_direction_deg !== null && wave.peak_direction_deg !== undefined
        ? `${fmtNumber(wave.peak_direction_deg, 0)}° ${escapeHtml(wave.direction_convention || "from")}`
        : wave.mean_direction_deg !== null && wave.mean_direction_deg !== undefined
          ? `${fmtNumber(wave.mean_direction_deg, 0)}° ${escapeHtml(wave.direction_convention || "from")}`
          : "—";

    const period =
      wave.peak_period_s !== null && wave.peak_period_s !== undefined
        ? `${fmtNumber(wave.peak_period_s, 1)} s`
        : wave.dominant_period_s !== null && wave.dominant_period_s !== undefined
          ? `${fmtNumber(wave.dominant_period_s, 1)} s`
          : "—";

    const meanPeriod =
      wave.mean_period_s !== null && wave.mean_period_s !== undefined
        ? `${fmtNumber(wave.mean_period_s, 1)} s`
        : "—";

    const windText =
      wind.speed_mps !== null && wind.speed_mps !== undefined
        ? `${fmtWindMps(wind.speed_mps)} from ${fmtNumber(wind.direction_deg, 0)}°`
        : "—";

    const waterTemp =
      buoy.water_temp_c !== null && buoy.water_temp_c !== undefined
        ? selectedUnitSystem() === "Metric"
          ? `${fmtNumber(buoy.water_temp_c, 1)} °C`
          : `${fmtNumber(buoy.water_temp_f, 1)} °F`
        : "—";

    const sourceLink = buoy.source_url
      ? `<a href="${escapeHtml(buoy.source_url)}" target="_blank" rel="noopener">source</a>`
      : "";

    return `
      <div class="global-buoy-popup">
        <div class="global-buoy-title">${escapeHtml(buoy.station_id)} — ${escapeHtml(buoy.name || buoy.station_id)}</div>
        <div class="global-buoy-subtitle">${escapeHtml(buoy.source)} ${sourceLink}</div>

        <table class="global-buoy-table">
          <tr><td>Last obs</td><td>${escapeHtml(fmtDate(buoy.last_observation_utc))}</td></tr>
          <tr><td>Status</td><td>${escapeHtml(buoy.status || "unknown")} · ${escapeHtml(fmtAge(buoy.age_minutes))}</td></tr>
          <tr><td>Hs</td><td>${escapeHtml(fmtHeightM(wave.significant_height_m))}</td></tr>
          <tr><td>Period</td><td>${escapeHtml(period)}</td></tr>
          <tr><td>Mean period</td><td>${escapeHtml(meanPeriod)}</td></tr>
          <tr><td>Direction</td><td>${escapeHtml(dir)}</td></tr>
          <tr><td>Wind</td><td>${escapeHtml(windText)}</td></tr>
          <tr><td>Water temp</td><td>${escapeHtml(waterTemp)}</td></tr>
          <tr><td>Lat/Lon</td><td>${fmtNumber(buoy.lat, 3)}, ${fmtNumber(buoy.lon, 3)}</td></tr>
        </table>

        ${buoy.notes ? `<div class="global-buoy-notes">${escapeHtml(buoy.notes)}</div>` : ""}
      </div>
    `;
  }

  function uniqueSources() {
    const map = new Map();

    for (const b of allBuoys) {
      if (!b.source_key) continue;
      map.set(b.source_key, b.source || b.source_key);
    }

    return Array.from(map.entries()).sort((a, b) => a[1].localeCompare(b[1]));
  }

  function sourceCount(sourceKey) {
    return allBuoys.filter((b) => b.source_key === sourceKey).length;
  }

  function setStatus(text) {
    if (!controlRoot) return;

    const el = controlRoot.querySelector(".global-buoy-status");
    if (el) el.textContent = text;
  }

  function renderSourceFilters() {
    if (!controlRoot) return;

    const wrap = controlRoot.querySelector(".global-buoy-source-list");
    if (!wrap) return;

    const sources = uniqueSources();

    if (enabledSources.size === 0) {
      sources.forEach(([key]) => enabledSources.add(key));
    }

    wrap.innerHTML = "";

    for (const [key, label] of sources) {
      const id = `global-buoy-source-${key}`;

      const row = document.createElement("label");
      row.className = "global-buoy-source-row";
      row.innerHTML = `
        <input type="checkbox" id="${escapeHtml(id)}" ${enabledSources.has(key) ? "checked" : ""}>
        <span>${escapeHtml(label)}</span>
        <span class="global-buoy-count">${sourceCount(key)}</span>
      `;

      const input = row.querySelector("input");
      input.addEventListener("change", () => {
        if (input.checked) {
          enabledSources.add(key);
        } else {
          enabledSources.delete(key);
        }
        rebuildMarkers();
      });

      wrap.appendChild(row);
    }
  }

  function rebuildMarkers() {
    const mapObj = getMap();
    if (!mapObj || !liveLayer) return;

    liveLayer.clearLayers();

    const visible = allBuoys.filter((b) => enabledSources.has(b.source_key));
    const worldOffset = currentWorldOffset(mapObj);
    lastWorldOffset = worldOffset;

    for (const buoy of visible) {
      const lat = Number(buoy.lat);
      const lon = Number(buoy.lon);

      if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;

      for (const wrappedLon of wrappedLongitudes(lon, mapObj)) {
        const marker = L.circleMarker([lat, wrappedLon], markerStyle(buoy));
        marker.bindTooltip(`${buoy.station_id} · ${buoy.source}`, {
          permanent: false,
          direction: "top",
          offset: [0, -2],
        });
        marker.bindPopup(popupHtml(buoy), { maxWidth: 360 });
        marker.addTo(liveLayer);
      }
    }

    setStatus(`${visible.length} live public buoys shown`);
  }

  async function loadGlobalBuoys() {
    setStatus("Loading global live buoys...");
  
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 25000);
  
    try {
      const response = await fetch(API_URL, {
        cache: "no-store",
        signal: controller.signal
      });
  
      clearTimeout(timeoutId);
  
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
  
      const payload = await response.json();
  
      allBuoys = Array.isArray(payload.buoys) ? payload.buoys : [];
  
      renderSourceFilters();
      rebuildMarkers();
  
      const errorCount = Array.isArray(payload.errors) ? payload.errors.length : 0;
      const loadedSources = Array.isArray(payload.sources_loaded)
        ? payload.sources_loaded.join(", ")
        : "unknown sources";
  
      const msg =
        `${allBuoys.length} public buoys loaded from ${loadedSources}` +
        (errorCount ? ` · ${errorCount} source issue(s)` : "");
  
      setStatus(msg);
    } catch (err) {
      clearTimeout(timeoutId);
  
      if (err.name === "AbortError") {
        setStatus("Global buoy request timed out after 25 seconds.");
      } else {
        setStatus(`Global buoy load failed: ${err.message || err}`);
      }
    }
  }

  function injectStyles() {
    if (document.getElementById("global-live-buoy-styles")) return;

    const style = document.createElement("style");
    style.id = "global-live-buoy-styles";
    style.textContent = `
      .global-buoy-control {
        background: rgba(255,255,255,0.96);
        border: 1px solid rgba(0,0,0,0.2);
        border-radius: 8px;
        padding: 10px;
        min-width: 230px;
        max-width: 290px;
        box-shadow: 0 8px 22px rgba(0,0,0,0.18);
        font-size: 13px;
      }

      .global-buoy-control-title {
        font-weight: 700;
        margin-bottom: 6px;
      }

      .global-buoy-status {
        color: #555;
        margin-bottom: 8px;
        font-size: 12px;
      }

      .global-buoy-source-list {
        max-height: 190px;
        overflow: auto;
        border-top: 1px solid #eee;
        border-bottom: 1px solid #eee;
        padding: 5px 0;
        margin-bottom: 8px;
      }

      .global-buoy-source-row {
        display: grid;
        grid-template-columns: 18px 1fr auto;
        gap: 6px;
        align-items: center;
        margin: 4px 0;
        cursor: pointer;
      }

      .global-buoy-count {
        color: #777;
        font-size: 12px;
      }

      .global-buoy-refresh {
        width: 100%;
        border: 1px solid #0d6efd;
        background: #0d6efd;
        color: #fff;
        border-radius: 6px;
        padding: 5px 8px;
        font-weight: 600;
      }

      .global-buoy-popup {
        min-width: 265px;
      }

      .global-buoy-title {
        font-weight: 700;
        font-size: 14px;
        margin-bottom: 2px;
      }

      .global-buoy-subtitle {
        font-size: 12px;
        color: #666;
        margin-bottom: 7px;
      }

      .global-buoy-table {
        border-collapse: collapse;
        width: 100%;
        font-size: 12px;
      }

      .global-buoy-table td {
        padding: 3px 5px;
        border-bottom: 1px solid #eee;
        vertical-align: top;
      }

      .global-buoy-table td:first-child {
        font-weight: 700;
        color: #555;
        white-space: nowrap;
        width: 92px;
      }

      .global-buoy-notes {
        margin-top: 6px;
        font-size: 11px;
        color: #666;
      }
    `;

    document.head.appendChild(style);
  }

  function createControl(mapObj) {
    const ctrl = L.control({ position: "topright" });

    ctrl.onAdd = function () {
      const div = L.DomUtil.create("div", "global-buoy-control");
      div.innerHTML = `
        <div class="global-buoy-control-title">Global live buoys</div>
        <div class="global-buoy-status">Starting...</div>
        <div class="global-buoy-source-list"></div>
        <button type="button" class="global-buoy-refresh">Refresh live buoys</button>
      `;

      L.DomEvent.disableClickPropagation(div);
      L.DomEvent.disableScrollPropagation(div);

      div.querySelector(".global-buoy-refresh").addEventListener("click", loadGlobalBuoys);

      controlRoot = div;
      return div;
    };

    ctrl.addTo(mapObj);
  }

  function init() {
    injectStyles();

    const mapObj = getMap();

    if (!mapObj || typeof L === "undefined") {
      setTimeout(init, 500);
      return;
    }

    liveLayer = L.layerGroup().addTo(mapObj);
    createControl(mapObj);

    mapObj.on("moveend zoomend", () => {
      const newOffset = currentWorldOffset(mapObj);
      if (newOffset !== lastWorldOffset) {
        rebuildMarkers();
      }
    });

    const unitSelect = document.getElementById("unit");
    if (unitSelect) {
      unitSelect.addEventListener("change", rebuildMarkers);
    }

    loadGlobalBuoys();

    // Refresh every 10 minutes while the page is open.
    window.setInterval(loadGlobalBuoys, 10 * 60 * 1000);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
