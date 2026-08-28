/* Transit Pulse static site. All data is build-time warehouse snapshots in
   ./data; the deck.gl maps and charts re-render on theme change so the validated
   light/dark series palettes both get used, never auto-flipped. */

"use strict";

// ---------- city registry (entity-fixed hues, validated light+dark) ----------
const CITIES = {
  boston:   { name: "Boston",        src: "MBTA · subway / bus / rail / ferry",  view: [42.34, -71.07, 11.2],  light: "#e34948", dark: "#e66767" },
  nyc:      { name: "New York",      src: "MTA · 8 subway feeds",                view: [40.73, -73.96, 10.6],  light: "#2a78d6", dark: "#3987e5" },
  zurich:   { name: "Zurich",        src: "opentransportdata.swiss · ZVV",       view: [47.38, 8.54, 11.0],    light: "#eb6834", dark: "#d95926" },
  helsinki: { name: "Helsinki",      src: "HSL · bus / tram / metro / rail",     view: [60.20, 24.93, 10.8],   light: "#1baf7a", dark: "#199e70" },
  dc:       { name: "Washington DC", src: "WMATA · rail + bus",                  view: [38.90, -77.03, 11.0],  light: "#eda100", dark: "#c98500" },
  toronto:  { name: "Toronto",       src: "TTC · streetcar + bus + subway",      view: [43.72, -79.38, 11.15], light: "#e87ba4", dark: "#d55181" },
  sf:       { name: "SF Bay Area",   src: "511.org · 30+ agencies",              view: [37.70, -122.30, 9.6],  light: "#008300", dark: "#008300" },
};
const VP_ABSENT = {
  helsinki: "HSL publishes no vehicle positions on its core GTFS-RT feed, so Helsinki shows routes and stop delays only.",
  zurich: "The Swiss LA API is trip-updates only — no vehicle positions — so Zurich shows routes and stop delays only.",
};

const MAP_STYLES = {
  light: "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
  dark: "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
};
const SEMANTIC = {
  light: { good: "#059669", warn: "#d97700", bad: "#cc0000", early: "#2563eb", veh: "26,26,26", glow: "37,99,235", routeAlpha: 95, heroAlpha: 175 },
  dark:  { good: "#a3be8c", warn: "#ebc88d", bad: "#bf616a", early: "#85c1fc", veh: "216,222,233", glow: "133,193,252", routeAlpha: 80, heroAlpha: 150 },
};

// ---------- theme ----------
function storedTheme() {
  try { return localStorage.getItem("theme"); } catch { return null; }
}
function currentTheme() {
  const t = document.documentElement.getAttribute("data-theme");
  if (t) return t;
  return matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}
function applyStoredTheme() {
  const t = storedTheme();
  if (t) document.documentElement.setAttribute("data-theme", t);
}
function cityColor(key) { return CITIES[key][currentTheme()]; }

// ---------- helpers ----------
const $ = (id) => document.getElementById(id);
const fmt = (n) => {
  n = Number(n);
  if (n >= 1e6) return (n / 1e6).toFixed(1) + "M";
  if (n >= 1e4) return Math.round(n / 1e3) + "k";
  return n.toLocaleString("en-US");
};
const esc = (s) => String(s).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
const cache = new Map();
async function loadJSON(path) {
  if (!cache.has(path)) {
    cache.set(path, fetch(path).then((r) => {
      if (!r.ok) throw new Error(`${path}: ${r.status}`);
      return r.json();
    }));
  }
  return cache.get(path);
}
function tile(value, label) {
  return `<div class="tile"><b>${value}</b><span>${label}</span></div>`;
}
function cssVar(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

// ---------- header stamp + hero/pipeline tiles ----------
async function renderTiles() {
  const s = await loadJSON("data/summary.json");
  const c = s.census;
  const day = s.as_of.slice(0, 10);
  $("as-of").textContent = day;
  $("as-of-hero").textContent = s.as_of;
  $("as-of-footer").textContent = day;
  $("hero-tiles").innerHTML = [
    tile(fmt(c.stop_events), "stop events scored"),
    tile(fmt(s.trips.observed), "trips observed"),
    tile("7 · 4", "cities · countries"),
    tile(fmt(c.silver_rows), "rows in silver"),
    tile(fmt(c.gold_rows), "rows in gold"),
    tile(c.service_days, "service days live"),
  ].join("");
  $("pipeline-tiles").innerHTML = [
    tile(fmt(c.silver_rows), "silver rows (iceberg)"),
    tile(fmt(c.gold_rows), "gold rows (dbt marts)"),
    tile("2h", "drain → dbt cadence"),
    tile("30s", "poll floor per feed"),
  ].join("");
}

// ---------- standings ----------
function sparkline(points, color) {
  if (points.length < 2) return `<span class="spark-empty">1 judged day</span>`;
  const W = 140, H = 30, P = 3;
  const ys = points.map((p) => p.otp_pct);
  let lo = Math.min(...ys), hi = Math.max(...ys);
  if (hi - lo < 4) { // don't let sub-point noise render as a full-amplitude swing
    const mid = (hi + lo) / 2;
    lo = mid - 2; hi = mid + 2;
  }
  const x = (i) => P + (i * (W - 2 * P)) / (points.length - 1);
  const y = (v) => P + ((hi - v) * (H - 2 * P)) / (hi - lo);
  const d = points.map((p, i) => `${i ? "L" : "M"}${x(i).toFixed(1)},${y(p.otp_pct).toFixed(1)}`).join("");
  const dots = points.map((p, i) =>
    `<circle cx="${x(i).toFixed(1)}" cy="${y(p.otp_pct).toFixed(1)}" r="2.4" fill="${color}">` +
    `<title>${esc(p.service_date)}: ${p.otp_pct}% on time</title></circle>`).join("");
  return `<svg class="spark" width="${W}" height="${H}" role="img" aria-label="daily on-time trend">` +
    `<path d="${d}" fill="none" stroke="${color}" stroke-width="2" stroke-linecap="round"/>${dots}</svg>`;
}

async function renderStandings() {
  const [s, daily] = await Promise.all([loadJSON("data/summary.json"), loadJSON("data/daily.json")]);
  const byCity = {};
  for (const r of daily.rows) (byCity[r.city_key] ??= []).push(r);
  const head = `<div class="standing-row head"><span></span><span>city</span>` +
    `<span class="optional">daily on-time</span><span class="num">on-time</span>` +
    `<span class="num">excess wait</span><span class="num optional">bunching</span>` +
    `<span class="num optional">cancelled</span><span class="num">judged</span></div>`;
  const rows = s.standings.map((r, i) => {
    const color = cityColor(r.city_key);
    const name = CITIES[r.city_key]?.name ?? r.city_key;
    return `<div class="standing-row">
      <span class="rank mono">${i + 1}</span>
      <span class="city"><span class="dot" style="background:${color}"></span>${name}</span>
      <span class="optional">${sparkline(byCity[r.city_key] ?? [], color)}</span>
      <span class="num mono otp">${r.otp_pct == null ? "—" : Number(r.otp_pct).toFixed(1) + "%"}</span>
      <span class="num mono">${r.ewt_sec == null ? "—" : r.ewt_sec + "s"}</span>
      <span class="num mono optional">${r.bunching_pct == null ? "—" : r.bunching_pct + "%"}</span>
      <span class="num mono optional">${r.cancel_pct == null ? "—" : r.cancel_pct + "%"}</span>
      <span class="num mono">${r.judged_days} / 20</span>
    </div>`;
  }).join("");
  $("standings-rows").innerHTML = head + rows;
}

// ---------- line charts (shared for hourly + daily) ----------
const hidden = new Set();

async function renderChartLegend() {
  const s = await loadJSON("data/summary.json");
  $("hourly-legend").innerHTML = s.standings.map((r) => {
    const key = r.city_key;
    return `<button class="chip" data-city="${key}" aria-pressed="${!hidden.has(key)}">` +
      `<span class="dot" style="background:${cityColor(key)}"></span>${CITIES[key].name}</button>`;
  }).join("");
  $("hourly-legend").querySelectorAll(".chip").forEach((chip) => {
    chip.addEventListener("click", () => {
      const key = chip.dataset.city;
      const visible = [...Object.keys(CITIES)].filter((c) => !hidden.has(c));
      if (hidden.has(key)) hidden.delete(key);
      else if (visible.length === 1 && visible[0] === key) Object.keys(CITIES).forEach((c) => hidden.delete(c)); // un-isolate
      else if (visible.length > 1 && !hidden.size) { Object.keys(CITIES).forEach((c) => c !== key && hidden.add(c)); } // isolate
      else hidden.add(key);
      renderChartLegend();
      renderLineCharts();
    });
  });
}

/* series: {cityKey: [{x, y} ...]} on an integer x grid; xLabel maps x → tick text. */
function drawLineChart({ svgId, tipId, series, xMax, xTickStep, xLabel, tipTitle }) {
  const svg = $(svgId);
  if (!svg) return;
  const W = svg.clientWidth || 640, H = svg.clientHeight || 280;
  const M = { top: 12, right: 14, bottom: 28, left: 40 };
  svg.setAttribute("viewBox", `0 0 ${W} ${H}`);

  const visible = Object.keys(CITIES).filter((c) => series[c]?.some(Boolean) && !hidden.has(c));
  const vals = visible.flatMap((c) => series[c].filter(Boolean).map((p) => p.y));
  if (!vals.length) { svg.innerHTML = ""; return; }
  const lo = Math.max(0, Math.floor((Math.min(...vals) - 5) / 10) * 10);
  const hi = Math.min(100, Math.ceil((Math.max(...vals) + 3) / 10) * 10);
  const x = (i) => M.left + (i * (W - M.left - M.right)) / Math.max(1, xMax);
  const y = (v) => M.top + ((hi - v) * (H - M.top - M.bottom)) / (hi - lo);

  const gridCol = cssVar("--border"), mutedCol = cssVar("--muted-foreground");
  let g = "";
  for (let v = lo; v <= hi; v += 10) {
    g += `<line x1="${M.left}" x2="${W - M.right}" y1="${y(v)}" y2="${y(v)}" stroke="${gridCol}" stroke-width="0.5"/>` +
      `<text x="${M.left - 8}" y="${y(v) + 4}" text-anchor="end" font-size="10.5" fill="${mutedCol}" font-family="Geist Mono,monospace">${v}%</text>`;
  }
  for (let i = 0; i <= xMax; i += xTickStep) {
    g += `<text x="${x(i)}" y="${H - 8}" text-anchor="middle" font-size="10.5" fill="${mutedCol}" font-family="Geist Mono,monospace">${xLabel(i)}</text>`;
  }
  for (const c of visible) {
    let d = "", prev = false;
    for (let i = 0; i <= xMax; i++) {
      const p = series[c][i];
      if (!p) { prev = false; continue; }
      d += `${prev ? "L" : "M"}${x(i).toFixed(1)},${y(p.y).toFixed(1)}`;
      prev = true;
    }
    g += `<path d="${d}" fill="none" stroke="${cityColor(c)}" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>`;
    if (xMax <= 10) { // few points (daily chart): mark them
      for (let i = 0; i <= xMax; i++) {
        const p = series[c][i];
        if (p) g += `<circle cx="${x(i)}" cy="${y(p.y)}" r="2.6" fill="${cityColor(c)}"/>`;
      }
    }
  }
  g += `<line class="cross" y1="${M.top}" y2="${H - M.bottom}" stroke="${mutedCol}" stroke-width="1" stroke-dasharray="3 3" visibility="hidden"/>`;
  g += `<g class="hover-dots"></g>`;
  g += `<rect class="hit" x="${M.left}" y="${M.top}" width="${W - M.left - M.right}" height="${H - M.top - M.bottom}" fill="transparent"/>`;
  svg.innerHTML = g;

  const tip = $(tipId);
  const hit = svg.querySelector(".hit");
  const cross = svg.querySelector(".cross");
  const dotsG = svg.querySelector(".hover-dots");
  const move = (ev) => {
    const rect = svg.getBoundingClientRect();
    const px = ((ev.clientX - rect.left) / rect.width) * W;
    const i = Math.max(0, Math.min(xMax, Math.round(((px - M.left) * xMax) / (W - M.left - M.right))));
    cross.setAttribute("x1", x(i)); cross.setAttribute("x2", x(i));
    cross.setAttribute("visibility", "visible");
    const at = visible.map((c) => ({ c, p: series[c][i] })).filter((d) => d.p).sort((a, b) => b.p.y - a.p.y);
    dotsG.innerHTML = at.map((d) =>
      `<circle cx="${x(i)}" cy="${y(d.p.y)}" r="4" fill="${cityColor(d.c)}" stroke="${cssVar("--surface")}" stroke-width="2"/>`).join("");
    tip.innerHTML = `<div class="t-title mono">${tipTitle(i)}</div>` + at.map((d) =>
      `<div class="t-row"><span class="dot" style="background:${cityColor(d.c)}"></span>${CITIES[d.c].name}<b class="mono">${d.p.y.toFixed(1)}%</b></div>`).join("");
    tip.hidden = false;
    const wrap = svg.parentElement.getBoundingClientRect();
    const tx = ev.clientX - wrap.left;
    tip.style.left = Math.min(tx + 16, wrap.width - tip.offsetWidth - 8) + "px";
    tip.style.top = Math.max(0, ev.clientY - wrap.top - tip.offsetHeight - 12) + "px";
  };
  hit.addEventListener("mousemove", move);
  hit.addEventListener("mouseleave", () => {
    tip.hidden = true;
    cross.setAttribute("visibility", "hidden");
    dotsG.innerHTML = "";
  });
}

async function renderLineCharts() {
  const [hourly, daily] = await Promise.all([loadJSON("data/hourly.json"), loadJSON("data/daily.json")]);

  const hSeries = {};
  for (const r of hourly.rows) (hSeries[r.city_key] ??= [])[r.local_hour] = { y: +r.otp_pct };
  drawLineChart({
    svgId: "hourly-chart", tipId: "hourly-tooltip", series: hSeries,
    xMax: 23, xTickStep: 3,
    xLabel: (h) => String(h).padStart(2, "0"),
    tipTitle: (h) => `${String(h).padStart(2, "0")}:00 local`,
  });

  const dates = [...new Set(daily.rows.map((r) => r.service_date))].sort();
  const idx = new Map(dates.map((d, i) => [d, i]));
  const dSeries = {};
  for (const r of daily.rows) (dSeries[r.city_key] ??= [])[idx.get(r.service_date)] = { y: +r.otp_pct };
  drawLineChart({
    svgId: "daily-chart", tipId: "daily-tooltip", series: dSeries,
    xMax: dates.length - 1, xTickStep: 1,
    xLabel: (i) => dates[i]?.slice(5).replace("-", "/") ?? "",
    tipTitle: (i) => dates[i] ?? "",
  });
}

// ---------- delay distribution small multiples ----------
async function renderDist() {
  const [dist, summary] = await Promise.all([loadJSON("data/dist.json"), loadJSON("data/summary.json")]);
  const byCity = {};
  for (const r of dist.rows) (byCity[r.city_key] ??= new Map()).set(+r.bucket_sec, +r.n);
  const order = summary.standings.map((r) => r.city_key).filter((c) => byCity[c]);
  const W = 300, H = 46, B0 = -300, B1 = 900, STEP = 30;
  const nb = (B1 - B0) / STEP + 1;
  const sem = SEMANTIC[currentTheme()];
  const mutedCol = cssVar("--muted-foreground");

  $("dist-grid").innerHTML = order.map((c) => {
    const m = byCity[c];
    const total = [...m.values()].reduce((a, b) => a + b, 0);
    const shares = [];
    let peak = 0;
    for (let i = 0; i < nb; i++) {
      const s = (m.get(B0 + i * STEP) ?? 0) / total;
      shares.push(s);
      peak = Math.max(peak, s);
    }
    const bw = W / nb;
    const bars = shares.map((s, i) => {
      const sec = B0 + i * STEP;
      const col = sec < -60 ? sem.early : sec < 60 ? sem.good : sec < 300 ? sem.warn : sem.bad;
      const h = Math.max(s > 0 ? 1 : 0, (s / peak) * (H - 12));
      return `<rect x="${(i * bw).toFixed(1)}" y="${(H - h).toFixed(1)}" width="${(bw - 0.6).toFixed(1)}" height="${h.toFixed(1)}" fill="${col}"><title>${sec >= 0 ? "+" : ""}${sec}s to ${sec + STEP >= 0 ? "+" : ""}${sec + STEP}s: ${(s * 100).toFixed(1)}% of events</title></rect>`;
    }).join("");
    const zeroX = ((0 - B0) / STEP) * bw;
    const share5 = shares.slice(0, nb).reduce((a, s, i) => (B0 + i * STEP >= 300 ? a + s : a), 0);
    return `<div class="dist-cell">
      <div class="dist-label"><span class="dot" style="background:${cityColor(c)}"></span>${CITIES[c].name}
        <span class="mono">${(share5 * 100).toFixed(1)}% beyond +5 min</span></div>
      <svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="none" role="img" aria-label="${CITIES[c].name} delay distribution">
        ${bars}<line x1="${zeroX}" x2="${zeroX}" y1="0" y2="${H}" stroke="${mutedCol}" stroke-width="0.8" stroke-dasharray="2 2"/>
      </svg>
    </div>`;
  }).join("") + `<div class="footnote" style="grid-column:1/-1">Dashed line = exactly on schedule; blue early · green on time
    · amber 1–5 min late · red beyond. Tails clamp at −5 and +15 minutes.</div>`;
}

// ---------- mode split table ----------
async function renderModes() {
  const [modes, summary] = await Promise.all([loadJSON("data/modes.json"), loadJSON("data/summary.json")]);
  const MODES = ["metro", "rail", "tram", "bus", "ferry", "other"];
  const byCity = {};
  const seen = new Set();
  for (const r of modes.rows) {
    (byCity[r.city_key] ??= {})[r.mode] = { otp: +r.otp_pct, events: +r.events };
    seen.add(r.mode);
  }
  const cols = MODES.filter((m) => seen.has(m));
  const order = summary.standings.map((r) => r.city_key).filter((c) => byCity[c]);
  const head = `<tr><th>city</th>${cols.map((m) => `<th class="num">${m}</th>`).join("")}</tr>`;
  const rows = order.map((c) => {
    const cells = cols.map((m) => {
      const v = byCity[c][m];
      return `<td class="num mono">${v ? `<span title="${fmt(v.events)} scored events">${v.otp.toFixed(1)}%</span>` : ""}</td>`;
    }).join("");
    return `<tr><td><span class="city-cell"><span class="dot" style="background:${cityColor(c)}"></span>${CITIES[c].name}</span></td>${cells}</tr>`;
  }).join("");
  $("modes-table").innerHTML = `<table class="data-table">${head}${rows}</table>` +
    `<p class="footnote">Cells under 5,000 scored events are suppressed. Hover a cell for its evidence count.</p>`;
}

// ---------- best / worst routes ----------
function routeTable(rows) {
  const body = rows.map((r) => {
    const what = r.long_name && r.long_name !== r.label ? esc(String(r.long_name).slice(0, 30).toLowerCase()) : r.mode;
    const name = `${CITIES[r.city_key].name.toLowerCase()} · ${what}`;
    return `<tr>
      <td><span class="city-cell"><span class="dot" style="background:${cityColor(r.city_key)}"></span>
        <span><b>${esc(r.label)}</b> <span class="route-name">${name}</span></span></span></td>
      <td class="num mono">${fmt(r.events)}</td>
      <td class="num mono">${Number(r.otp_pct).toFixed(1)}%</td>
    </tr>`;
  }).join("");
  return `<table class="data-table"><tr><th>route</th><th class="num">events</th><th class="num">on-time</th></tr>${body}</table>`;
}
async function renderRoutes() {
  const r = await loadJSON("data/routes.json");
  $("routes-best").innerHTML = routeTable(r.best);
  $("routes-worst").innerHTML = routeTable(r.worst);
}

// ---------- deck.gl maps ----------
const MODE_COLOR = { 0: [255, 184, 76], 1: [126, 166, 255], 2: [186, 134, 255], 3: [86, 156, 214], 4: [81, 207, 208], 5: [255, 140, 120], 7: [255, 140, 120], 11: [86, 156, 214], 12: [186, 134, 255] };
const hex2rgb = (h) => {
  if (!h) return null;
  h = h.replace("#", "");
  if (h.length < 6) return null;
  return [parseInt(h.slice(0, 2), 16), parseInt(h.slice(2, 4), 16), parseInt(h.slice(4, 6), 16)];
};
const parsePath = (s) => s.split(",").map((p) => { const a = p.split(" "); return [+a[0], +a[1]]; });

function mapLayers(id, data, theme, hero) {
  const sem = SEMANTIC[theme];
  const OK = hex2rgb(sem.good), WARN = hex2rgb(sem.warn), BAD = hex2rgb(sem.bad), EARLY = hex2rgb(sem.early);
  const delayColor = (d) => d == null ? [128, 128, 128] : d < -60 ? EARLY : d < 60 ? OK : d < 300 ? WARN : BAD;
  const routeColor = (r) => {
    const info = data.routes[r] || {};
    return hex2rgb(info.route_color) || MODE_COLOR[+info.route_type] || [96, 140, 190];
  };
  const veh = sem.veh.split(",").map(Number), glow = sem.glow.split(",").map(Number);
  const shapes = data.shapes.map((s) => ({ path: parsePath(s.path), color: routeColor(s.route_id) }));
  return [
    new deck.PathLayer({
      id: id + "-routes", data: shapes, getPath: (d) => d.path,
      getColor: (d) => [...d.color, hero ? sem.heroAlpha : sem.routeAlpha],
      getWidth: hero ? 3.1 : 1.7, widthUnits: "pixels", capRounded: true, jointRounded: true,
    }),
    new deck.ScatterplotLayer({
      id: id + "-stops", data: data.stops, getPosition: (d) => [+d.lon, +d.lat],
      getFillColor: (d) => [...delayColor(d.delay == null ? null : +d.delay), 215],
      getRadius: hero ? 3.9 : 2.4, radiusUnits: "pixels", pickable: !hero,
    }),
    new deck.ScatterplotLayer({
      id: id + "-veh-glow", data: data.vehicles, getPosition: (d) => [+d.lon, +d.lat],
      getFillColor: [...glow, 48], getRadius: 5.5, radiusUnits: "pixels",
    }),
    new deck.ScatterplotLayer({
      id: id + "-veh", data: data.vehicles, getPosition: (d) => [+d.lon, +d.lat],
      getFillColor: [...veh, 235], getRadius: 2.2, radiusUnits: "pixels",
    }),
  ];
}

function legendHTML(theme, vehicles) {
  const sem = SEMANTIC[theme];
  return `<span><span class="dot" style="background:${sem.early}"></span>early</span>
    <span><span class="dot" style="background:${sem.good}"></span>on time</span>
    <span><span class="dot" style="background:${sem.warn}"></span>1–5 min late</span>
    <span><span class="dot" style="background:${sem.bad}"></span>&gt;5 min late</span>` +
    (vehicles ? `<span><span class="dot" style="background:rgb(${sem.veh});box-shadow:0 0 5px rgb(${sem.glow})"></span>vehicle · latest fix</span>` : "");
}

let heroDeck = null, cityDeck = null, activeCity = "nyc";

async function renderHero() {
  const theme = currentTheme();
  const data = await loadJSON("data/maps/nyc.json");
  const props = {
    mapStyle: MAP_STYLES[theme],
    layers: mapLayers("hero", data, theme, true),
    initialViewState: { latitude: 40.717, longitude: -73.925, zoom: 11.6, pitch: 52, bearing: -18 },
    controller: false,
  };
  if (heroDeck) heroDeck.setProps(props);
  else heroDeck = new deck.DeckGL({ container: "hero-map", mapLib: maplibregl, ...props });
  $("hero-legend").innerHTML = legendHTML(theme, data.vehicles.length > 0);
}

async function renderCityMap(key, recenter) {
  activeCity = key;
  const theme = currentTheme();
  const meta = CITIES[key];
  const frame = document.querySelector(".map-frame");
  let loading = frame.querySelector(".map-loading");
  if (!cache.has(`data/maps/${key}.json`) && !loading) {
    loading = document.createElement("div");
    loading.className = "map-loading";
    loading.textContent = "loading network…";
    frame.appendChild(loading);
  }
  const [data, summary] = await Promise.all([loadJSON(`data/maps/${key}.json`), loadJSON("data/summary.json")]);
  frame.querySelector(".map-loading")?.remove();
  if (activeCity !== key) return; // a later tab click won the race
  const [lat, lon, zoom] = meta.view;
  const props = {
    mapStyle: MAP_STYLES[theme],
    layers: mapLayers("city", data, theme, false),
    getTooltip: ({ layer, object }) =>
      layer?.id === "city-stops" && object ? { text: `${object.delay}s mean delay` } : null,
  };
  if (recenter) props.initialViewState = { latitude: lat, longitude: lon, zoom, pitch: 0, bearing: 0 };
  if (cityDeck) cityDeck.setProps(props);
  else cityDeck = new deck.DeckGL({ container: "city-map", mapLib: maplibregl, controller: true, initialViewState: { latitude: lat, longitude: lon, zoom, pitch: 0, bearing: 0 }, ...props });

  const st = summary.standings.find((r) => r.city_key === key) ?? {};
  $("city-panel").innerHTML = `<h3>${meta.name}</h3><div class="src">${esc(meta.src)}</div>
    <div class="tile-grid">
      ${tile(st.otp_pct != null ? Number(st.otp_pct).toFixed(1) + "%" : "—", "on time")}
      ${tile(fmt(st.events ?? 0), "stop events")}
      ${tile(data.shapes.length, "route paths")}
      ${tile(data.vehicles.length || "—", "vehicles · last fix")}
    </div>`;
  $("city-legend").innerHTML = legendHTML(theme, data.vehicles.length > 0);
  $("city-note").textContent = VP_ABSENT[key] ?? "";
  document.querySelectorAll("#city-tabs button").forEach((b) =>
    b.setAttribute("aria-selected", String(b.dataset.city === key)));
}

function renderTabs() {
  $("city-tabs").innerHTML = Object.entries(CITIES).map(([key, c]) =>
    `<button role="tab" data-city="${key}" aria-selected="${key === activeCity}">${c.name.toLowerCase()}</button>`).join("");
  $("city-tabs").querySelectorAll("button").forEach((b) =>
    b.addEventListener("click", () => renderCityMap(b.dataset.city, true)));
}

// ---------- reveal on scroll ----------
function setupReveal() {
  const io = new IntersectionObserver((entries) => {
    for (const e of entries) {
      if (e.isIntersecting) { e.target.classList.add("is-visible"); io.unobserve(e.target); }
    }
  }, { rootMargin: "0px 0px -40px 0px" });
  document.querySelectorAll(".reveal").forEach((el) => io.observe(el));
}

// ---------- theme toggle + boot ----------
function rethemeAll() {
  renderStandings();
  renderChartLegend();
  renderLineCharts();
  renderDist();
  renderModes();
  renderRoutes();
  renderHero();
  renderCityMap(activeCity, false);
}

applyStoredTheme();
$("theme-toggle").addEventListener("click", () => {
  const next = currentTheme() === "dark" ? "light" : "dark";
  document.documentElement.setAttribute("data-theme", next);
  try { localStorage.setItem("theme", next); } catch { /* private mode */ }
  rethemeAll();
});
matchMedia("(prefers-color-scheme: dark)").addEventListener("change", () => {
  if (!document.documentElement.getAttribute("data-theme")) rethemeAll();
});
let resizeT = null;
addEventListener("resize", () => {
  clearTimeout(resizeT);
  resizeT = setTimeout(renderLineCharts, 150);
});

renderTiles();
renderStandings();
renderChartLegend().then(renderLineCharts);
renderDist();
renderModes();
renderRoutes();
renderHero();
renderTabs();
renderCityMap("nyc", true);
setupReveal();
