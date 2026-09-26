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
  // violet: the one hue the seven above leave free (toronto owns pink here)
  tokyo:    { name: "Tokyo",         src: "ODPT · Toei subway + tram + bus",     view: [35.68, 139.76, 10.8],  light: "#8a56d6", dark: "#9a6ee0" },
};
const VP_ABSENT = {
  helsinki: "HSL publishes no vehicle positions on its core GTFS-RT feed, so Helsinki shows routes and stop delays only.",
  zurich: "The Swiss LA API is trip-updates only — no vehicle positions — so Zurich shows routes and stop delays only.",
};

// modes are a second axis, so they get their own hues — none of them a city's
const MODE_COLORS = {
  light: { metro: "#4f46e5", rail: "#0e7490", tram: "#b45309", bus: "#64748b", ferry: "#0284c7", other: "#a16207", unknown: "#b8bcc4" },
  dark:  { metro: "#818cf8", rail: "#22d3ee", tram: "#f59e0b", bus: "#94a3b8", ferry: "#38bdf8", other: "#fbbf24", unknown: "#5b616b" },
};

const MAP_STYLES = {
  light: "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
  dark: "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
};
// veh is INK, deliberately outside the delay ramp: a vehicle is an object,
// not a measurement, and must never share a hue with "early" blue
const SEMANTIC = {
  light: { good: "#059669", warn: "#d97700", bad: "#cc0000", early: "#2563eb", veh: "26,26,26", ring: "255,255,255", routeAlpha: 95, heroAlpha: 175 },
  dark:  { good: "#a3be8c", warn: "#ebc88d", bad: "#bf616a", early: "#85c1fc", veh: "216,222,233", ring: "26,26,26", routeAlpha: 80, heroAlpha: 150 },
};

// ---------- theme ----------
// The head inline script stamps data-theme from cookie/localStorage pre-paint;
// this module only reads, toggles, and persists (sky §7a-c contract).
function currentTheme() {
  const t = document.documentElement.getAttribute("data-theme");
  if (t) return t;
  return matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}
function cityColor(key) { return CITIES[key][currentTheme()]; }

const SIDEBAR_FILL = { light: "#f5f5f5", dark: "#1f1f1f" };

function persistTheme(theme) {
  try { localStorage.setItem("theme", theme); } catch { /* private mode */ }
  const { hostname, protocol } = location;
  const onSite = hostname === "gillu.me" || hostname.endsWith(".gillu.me");
  const attrs = [`theme=${theme}`, "path=/", `max-age=${60 * 60 * 24 * 365}`, "samesite=lax"];
  // a domain attribute the browser can't match is rejected outright
  if (onSite) attrs.push("domain=.gillu.me");
  if (protocol === "https:") attrs.push("secure");
  document.cookie = attrs.join("; ");
}

// ---------- helpers ----------
const $ = (id) => document.getElementById(id);
const fmt = (n) => {
  n = Number(n);
  if (n >= 1e9) return (n / 1e9).toFixed(2) + "B";
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

// ---------- shared hover tooltip ----------
// Any element carrying data-tip (HTML, escaped at build time) gets the floating
// tooltip. One delegated listener serves the mode table, donuts and day bars;
// pointerdown makes a tap work the same way on touch screens.
const hoverTip = document.createElement("div");
hoverTip.className = "tooltip float-tip";
hoverTip.hidden = true;
document.body.appendChild(hoverTip);
let tipEl = null;

function donutCenter(seg, on) {
  const svg = seg?.closest(".donut");
  if (!svg) return;
  const src = on ? seg : svg;
  svg.querySelector(".donut-val").textContent = src.dataset.pct;
  svg.querySelector(".donut-lbl").textContent = src.dataset.mode;
}
function setTipTarget(el) {
  if (el === tipEl) return;
  if (tipEl) { tipEl.classList.remove("is-hot"); donutCenter(tipEl, false); }
  tipEl = el;
  if (!el) { hoverTip.hidden = true; return; }
  el.classList.add("is-hot");
  donutCenter(el, true);
  hoverTip.innerHTML = el.dataset.tip;
  hoverTip.hidden = false;
}
function onTipPointer(ev) {
  setTipTarget(ev.target.closest?.("[data-tip]") ?? null);
  if (!tipEl) return;
  const pad = 14, w = hoverTip.offsetWidth, h = hoverTip.offsetHeight;
  let x = ev.clientX + pad, y = ev.clientY - h - pad;
  if (x + w > innerWidth - 8) x = ev.clientX - w - pad;
  if (y < 8) y = ev.clientY + pad;
  hoverTip.style.left = Math.max(8, x) + "px";
  hoverTip.style.top = y + "px";
}
document.addEventListener("pointermove", onTipPointer);
document.addEventListener("pointerdown", onTipPointer);
addEventListener("scroll", () => setTipTarget(null), { passive: true });
document.documentElement.addEventListener("mouseleave", () => setTipTarget(null));

// ---------- header stamp + hero/pipeline tiles ----------
async function renderTiles() {
  const s = await loadJSON("data/summary.json");
  const c = s.census;
  const day = s.as_of.slice(0, 10);
  $("as-of").textContent = day;
  $("as-of-hero").textContent = s.as_of;
  $("as-of-banner").textContent = s.as_of;
  $("as-of-footer").textContent = day;
  $("hero-tiles").innerHTML = [
    tile(fmt(c.stop_events), "stop events scored"),
    tile(fmt(s.trips.observed), "trips observed"),
    tile("8 · 5", "cities · countries"),
    tile(fmt(c.silver_rows), "rows in silver"),
    tile(fmt(c.gold_rows), "rows in gold"),
    tile(c.service_days, "service days"),
  ].join("");
  $("spec-silver").textContent = fmt(c.silver_rows);
  $("spec-gold").textContent = fmt(c.gold_rows);
  $("pipeline-tiles").innerHTML = [
    tile(fmt(c.silver_rows), "silver rows (iceberg)"),
    tile(fmt(c.gold_rows), "gold rows (dbt marts)"),
    tile("2h · 12h", "drain · dbt cadence"),
    tile("30s", "poll floor per feed"),
  ].join("");
}

// ---------- standings ----------
// A metric the feed cannot express is a quiet dash, never a printed zero.
const NA = `<span class="na-dash">—</span>`;

// Cancellations carry three states and one number cannot show them: a feed with
// no CANCELED vocabulary (New York, Tokyo) reads a dash, a feed that says it and
// almost never means it reads "<0.01%" rather than a bare 0.00% beside a real
// count (Toronto: 4 trips in 661,306), and everyone else reads the rate.
function cancelCell(r) {
  if (r.cancel_pct == null || r.emits_cancels === false) return NA;
  const v = Number(r.cancel_pct);
  // compare on the COUNT, not the rate: the rate arrives pre-rounded to two
  // places, so a real handful of cancellations is already 0.0 by the time it
  // gets here and would otherwise print as a flat 0%.
  return Number(r.cancelled) > 0 && v < 0.01 ? "&lt;0.01%" : v + "%";
}

function sparkline(points, color) {
  // a judged day with no scoreable delay is absent, not zero — plotting it at 0
  // drew Toronto's timetable-refresh days (09-03..05) as a service collapse
  points = points.filter((p) => p.otp_pct != null);
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
    `<span class="optional">daily on-time</span><span class="num">score</span>` +
    `<span class="num">on-time</span>` +
    `<span class="num">excess wait</span><span class="num optional">bunching</span>` +
    `<span class="num optional">cancelled</span><span class="num">judged</span></div>`;
  // Rank by the composite, which is what the section claims to rank by. Cities
  // still short of 20 judged days have no score and sort to the bottom on
  // on-time rather than being hidden -- they are evidence, not a verdict.
  const ranked = [...s.standings].sort((a, b) => {
    const as = a.score_0_100, bs = b.score_0_100;
    if (as != null && bs != null) return bs - as;
    if (as != null) return -1;
    if (bs != null) return 1;
    return (b.otp_pct ?? 0) - (a.otp_pct ?? 0);
  });
  const rows = ranked.map((r, i) => {
    const color = cityColor(r.city_key);
    const name = CITIES[r.city_key]?.name ?? r.city_key;
    return `<div class="standing-row">
      <span class="rank mono">${i + 1}</span>
      <span class="city"><span class="dot" style="background:${color}"></span>${name}</span>
      <span class="optional">${sparkline(byCity[r.city_key] ?? [], color)}</span>
      <span class="num mono otp">${r.score_0_100 == null
        ? `<span class="unscored" title="needs 20 judged days">—</span>`
        : Number(r.score_0_100).toFixed(1)}</span>
      <span class="num mono">${r.otp_pct == null ? "—" : Number(r.otp_pct).toFixed(1) + "%"}</span>
      <span class="num mono">${r.ewt_sec == null ? NA : r.ewt_sec + "s"}</span>
      <span class="num mono optional">${r.bunching_pct == null ? NA : r.bunching_pct + "%"}</span>
      <span class="num mono optional">${cancelCell(r)}</span>
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

/* series: {cityKey: [{x, y} ...]} on an integer x grid; xLabel maps x → tick text.
   angled: tick labels at 45° so a dense axis never overlaps on a narrow card. */
function drawLineChart({ svgId, tipId, series, xMax, xTickStep, xLabel, tipTitle, angled = false }) {
  const svg = $(svgId);
  if (!svg) return;
  const W = svg.clientWidth || 640, H = svg.clientHeight || 280;
  const M = { top: 12, right: 14, bottom: angled ? 34 : 28, left: 40 };
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
    if (angled) {
      const bx = x(i).toFixed(1), by = H - M.bottom;
      g += `<line x1="${bx}" x2="${bx}" y1="${by}" y2="${by + 4}" stroke="${gridCol}" stroke-width="1"/>` +
        `<text transform="translate(${bx},${by + 9}) rotate(-45)" text-anchor="end" dominant-baseline="central" font-size="10" fill="${mutedCol}" font-family="Geist Mono,monospace">${xLabel(i)}</text>`;
    } else {
      g += `<text x="${x(i)}" y="${H - 8}" text-anchor="middle" font-size="10.5" fill="${mutedCol}" font-family="Geist Mono,monospace">${xLabel(i)}</text>`;
    }
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
  for (const r of hourly.rows) {
    if (r.local_hour == null) continue; // events with no arrival time have no hour to plot
    (hSeries[r.city_key] ??= [])[r.local_hour] = { y: +r.otp_pct };
  }
  drawLineChart({
    svgId: "hourly-chart", tipId: "hourly-tooltip", series: hSeries,
    xMax: 23, xTickStep: 3,
    xLabel: (h) => String(h).padStart(2, "0"),
    tipTitle: (h) => `${String(h).padStart(2, "0")}:00 local`,
  });

  // x is the day of recording, not the calendar: day 1 is each city's first
  // judged day, so cities that started a week apart still line up
  const byCity = {};
  for (const r of daily.rows) (byCity[r.city_key] ??= []).push(r);
  const dSeries = {};
  let days = 0;
  for (const [c, rows] of Object.entries(byCity)) {
    rows.sort((a, b) => a.service_date.localeCompare(b.service_date));
    days = Math.max(days, rows.length);
    // a judged day with no scoreable delay is a gap in the line, never a plunge to 0
    dSeries[c] = rows.map((r) => (r.otp_pct == null ? undefined : { y: +r.otp_pct }));
  }
  drawLineChart({
    svgId: "daily-chart", tipId: "daily-tooltip", series: dSeries,
    xMax: days - 1, xTickStep: 1, angled: true,
    xLabel: (i) => i + 1,
    tipTitle: (i) => `day ${i + 1} of recording`,
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
      const tip = `<div class="t-title mono">${sec >= 0 ? "+" : ""}${sec}s to ${sec + STEP >= 0 ? "+" : ""}${sec + STEP}s</div>` +
        `<div class="t-row">${esc(CITIES[c].name)}<b class="mono">${(s * 100).toFixed(1)}%</b></div>`;
      return `<g class="bin" data-tip="${esc(tip)}"><rect class="hit" x="${(i * bw).toFixed(1)}" y="0" width="${bw.toFixed(1)}" height="${H}"/>` +
        `<rect x="${(i * bw).toFixed(1)}" y="${(H - h).toFixed(1)}" width="${(bw - 0.6).toFixed(1)}" height="${h.toFixed(1)}" fill="${col}"/></g>`;
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
  // one hue, light → dark with on-time share; the shade is a reading aid, the
  // printed number stays the value
  const LO = 40, HI = 100;
  const shade = (otp) => (6 + 42 * Math.max(0, Math.min(1, (otp - LO) / (HI - LO)))).toFixed(0);
  const head = `<tr><th>city</th>${cols.map((m) => `<th class="num">${m}</th>`).join("")}</tr>`;
  const rows = order.map((c) => {
    const cells = cols.map((m) => {
      const v = byCity[c][m];
      if (!v) return `<td class="num"><span class="heat empty" aria-label="no ${m} service">·</span></td>`;
      const tip = `<div class="t-title">${esc(CITIES[c].name)} · ${esc(m)}</div>` +
        `<div class="t-row">on time<b class="mono">${v.otp.toFixed(1)}%</b></div>` +
        `<div class="t-row">scored events<b class="mono">${v.events.toLocaleString("en-US")}</b></div>`;
      return `<td class="num"><span class="heat mono" style="--shade:${shade(v.otp)}%" data-tip="${esc(tip)}">${v.otp.toFixed(1)}%</span></td>`;
    }).join("");
    return `<tr><td><span class="city-cell"><span class="dot" style="background:${cityColor(c)}"></span>${CITIES[c].name}</span></td>${cells}</tr>`;
  }).join("");
  $("modes-table").innerHTML = `<div class="table-scroll"><table class="data-table heat-table">${head}${rows}</table></div>` +
    `<div class="heat-key"><span class="mono">${LO}%</span><span class="heat-ramp"></span><span class="mono">${HI}%</span>` +
    `<span class="heat-hint">on time · hover a cell for its scored events</span></div>`;
}

// ---------- storage: what each city put in the warehouse ----------
// Ring segments are filled annular sectors, not dashed circle strokes: a dash
// seam anti-aliases into a visible notch where the ring closes. A 2px surface
// stroke separates the segments evenly instead.
function donut(parts, colors, size = 92, stroke = 14) {
  const c = size / 2, R = c - 1, r = R - stroke;
  const total = parts.reduce((a, p) => a + p.value, 0) || 1;
  const pt = (rad, a) => `${(c + rad * Math.sin(a)).toFixed(2)},${(c - rad * Math.cos(a)).toFixed(2)}`;
  const ring = (rad, sweep) => `M${c},${c - rad}A${rad},${rad} 0 1 ${sweep} ${c},${c + rad}A${rad},${rad} 0 1 ${sweep} ${c},${c - rad}Z`;
  const pct = (v) => (100 * v / total).toFixed(1) + "%";
  // a sliver thinner than the 2px separator reads as a chip in the ring, so
  // every segment gets at least ~5px of arc, borrowed from the largest one
  const minSweep = 5 / R;
  const sweeps = parts.map((p) => Math.max(minSweep, (p.value / total) * 2 * Math.PI));
  if (parts.length > 1) sweeps[0] -= sweeps.reduce((a, b) => a + b, 0) - 2 * Math.PI;
  let a0 = 0;
  const segs = parts.map((p, i) => {
    const share = p.value / total;
    const a1 = a0 + (parts.length > 1 ? sweeps[i] : 2 * Math.PI);
    const big = a1 - a0 > Math.PI ? 1 : 0;
    const d = share > 0.9999
      ? ring(R, 1) + ring(r, 0)
      : `M${pt(R, a0)}A${R},${R} 0 ${big} 1 ${pt(R, a1)}L${pt(r, a1)}A${r},${r} 0 ${big} 0 ${pt(r, a0)}Z`;
    a0 = a1;
    const tip = `<div class="t-title">${esc(p.key)}</div>` +
      `<div class="t-row">share of stop events<b class="mono">${pct(p.value)}</b></div>` +
      `<div class="t-row">stop events<b class="mono">${p.value.toLocaleString("en-US")}</b></div>` +
      (p.routes != null ? `<div class="t-row">routes<b class="mono">${Number(p.routes).toLocaleString("en-US")}</b></div>` : "");
    return `<path class="seg" d="${d}" fill="${colors[p.key] ?? colors.unknown}" fill-rule="evenodd"
      data-tip="${esc(tip)}" data-pct="${pct(p.value)}" data-mode="${esc(p.key)}"/>`;
  }).join("");
  const top = parts[0];
  return `<svg class="donut" viewBox="0 0 ${size} ${size}" width="${size}" height="${size}" role="img"
    aria-label="share of stop events by mode" data-pct="${top ? pct(top.value) : ""}" data-mode="${top ? esc(top.key) : ""}">
    <g class="segs">${segs}</g>
    <text class="donut-val mono" x="${c}" y="${c + 1}" text-anchor="middle">${top ? pct(top.value) : ""}</text>
    <text class="donut-lbl" x="${c}" y="${c + 13}" text-anchor="middle">${top ? esc(top.key) : ""}</text></svg>`;
}

function daybars(days, color) {
  if (!days.length) return "";
  const W = 300, H = 44, n = days.length, bw = W / n;
  const peak = Math.max(...days.map((d) => +d.events)) || 1;
  const bars = days.map((d, i) => {
    const h = Math.max(1, (+d.events / peak) * (H - 2));
    const x = i * bw, w = Math.max(0.8, bw - 1.5);
    const tip = `<div class="t-title mono">${esc(d.service_date)}</div>` +
      `<div class="t-row">stop events<b class="mono">${Number(d.events).toLocaleString("en-US")}</b></div>` +
      `<div class="t-row">trips<b class="mono">${Number(d.trips).toLocaleString("en-US")}</b></div>` +
      `<div class="t-row"><span class="sw ${d.judged ? "solid" : "hollow"}" style="--c:${color}"></span>${d.judged ? "judged" : "not judged"}</div>`;
    const bar = d.judged
      ? `<rect class="bar" x="${x.toFixed(1)}" y="${(H - h).toFixed(1)}" width="${w.toFixed(1)}" height="${h.toFixed(1)}" fill="${color}"/>`
      : `<rect class="bar hollow" x="${(x + 0.5).toFixed(1)}" y="${(H - h + 0.5).toFixed(1)}" width="${Math.max(0.5, w - 1).toFixed(1)}" height="${Math.max(0.5, h - 1).toFixed(1)}" fill="none" stroke="${color}" stroke-width="1" vector-effect="non-scaling-stroke"/>`;
    // the hit area is the full column, so a short day is as easy to point at as a tall one
    return `<g class="day" data-tip="${esc(tip)}"><rect class="hit" x="${x.toFixed(1)}" y="0" width="${bw.toFixed(1)}" height="${H}"/>${bar}</g>`;
  }).join("");
  return `<svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="none" class="daybars" role="img" aria-label="stop events per service day">${bars}</svg>` +
    `<div class="daybars-label"><span>${esc(days[0].service_date)}</span>` +
    `<span class="db-key"><span class="sw solid" style="--c:${color}"></span>judged<span class="sw hollow" style="--c:${color}"></span>not judged</span>` +
    `<span>${esc(days[days.length - 1].service_date)}</span></div>`;
}

async function renderStorage() {
  const [st, fleet, summary] = await Promise.all([
    loadJSON("data/storage.json"),
    loadJSON("data/fleet.json").catch(() => null), // a heavier, on-demand export; the cards stand without it
    loadJSON("data/summary.json"),
  ]);
  const order = summary.standings.map((r) => r.city_key).filter((c) => st.cities[c]);
  const mc = MODE_COLORS[currentTheme()];
  $("storage-grid").innerHTML = order.map((c) => {
    const d = st.cities[c];
    // under 0.05% of events prints as 0.0% and is a speck on the ring: left out
    const parts = d.modes.filter((m) => m.events >= 0.0005 * d.events).sort((a, b) => b.events - a.events)
      .map((m) => ({ key: m.mode, value: +m.events, routes: m.routes }));
    const legend = parts.map((p) =>
      `<li><span class="dot" style="background:${mc[p.key] ?? mc.unknown}"></span>${esc(p.key)} <span class="mono">${(100 * p.value / d.events).toFixed(1)}%</span></li>`).join("");
    const silverTotal = Object.values(d.silver).reduce((a, b) => a + b, 0);
    const silverBits = ["predictions", "positions", "trains", "alerts"].filter((k) => d.silver[k])
      .map((k) => `${fmt(d.silver[k])} ${k}`).join(" · ");
    // fleet at the peak minute: trips in motion per mode, plus any mode the city
    // publishes positions for and no trip updates (ToeiBus)
    const f = fleet?.cities?.[c];
    const motion = {};
    if (f) {
      for (const [m, v] of Object.entries(f.in_motion)) if (m !== "unknown") motion[m] = v.peak;
      for (const [m, v] of Object.entries(f.positions)) if (!(m in motion) && m !== "unknown" && m !== "no_route") motion[m] = v.on_trip_fresh;
    }
    const motionTotal = Object.values(motion).reduce((a, b) => a + b, 0);
    const motionBits = Object.entries(motion).sort((a, b) => b[1] - a[1])
      .map(([m, v]) => `${fmt(v)} ${m === "metro" && c === "nyc" ? "subway" : m}`).join(" · ");
    const noTrip = f ? (f.positions.no_route?.no_trip ?? 0) : 0;
    const vehicles = d.vehicles != null
      ? `${fmt(d.vehicles)} distinct vehicle ids`
      : c === "nyc" ? "no vehicle ids — a trip stands in for a train" : "no positions feed";
    return `<div class="card storage-card">
      <div class="storage-head"><span class="city-cell"><span class="dot" style="background:${cityColor(c)}"></span><b>${CITIES[c].name}</b></span><span class="sub">${esc(CITIES[c].src)} · ${d.days} service days</span></div>
      <div class="storage-body">
        ${donut(parts, mc)}
        <ul class="mode-legend">${legend}</ul>
        <div class="tile-grid storage-tiles">
          <div class="tile"><b>${fmt(d.events)}</b><span>stop events</span></div>
          <div class="tile"><b>${fmt(silverTotal)}</b><span>silver rows</span></div>
          <div class="tile"><b>${fmt(d.headways)}</b><span>headways</span></div>
          <div class="tile"><b>${fmt(d.trips)}</b><span>trips</span></div>
          <div class="tile"><b>${d.routes.toLocaleString("en-US")}</b><span>routes</span></div>
          <div class="tile"><b>${fmt(d.stops)}</b><span>stops</span></div>
        </div>
      </div>
      <p class="storage-fleet">${silverBits} · ${vehicles}${f ? ` · <b>${fmt(motionTotal)} in motion at the peak minute</b> (${motionBits})${noTrip ? ` · ${fmt(noTrip)} more reporting with no trip` : ""}` : ""}</p>
      ${daybars(d.days_series, cityColor(c))}
    </div>`;
  }).join("");
}

// ---------- measured answers ----------
function cityCell(key) {
  return `<span class="city-cell"><span class="dot" style="background:${cityColor(key)}"></span>${CITIES[key].name}</span>`;
}
async function renderAnswers() {
  const [raw, summary] = await Promise.all([loadJSON("data/answers.json"), loadJSON("data/summary.json")]);
  // a city can reach gold before this file's CITIES map learns it (tokyo did);
  // render what we know rather than throwing on the whole section
  const known = (rows) => rows.filter((r) => CITIES[r.city_key]);
  const a = {
    weekday_weekend: known(raw.weekday_weekend),
    early_departures: known(raw.early_departures),
    peak_offpeak: known(raw.peak_offpeak),
    cancellations: known(raw.cancellations),
  };

  const wk = {};
  for (const r of a.weekday_weekend) (wk[r.city_key] ??= {})[r.day_type] = +r.otp_pct;
  const wkRows = Object.entries(wk)
    .filter(([, v]) => v.weekday != null && v.weekend != null)
    .map(([c, v]) => ({ c, ...v, edge: v.weekend - v.weekday }))
    .sort((x, y) => y.edge - x.edge)
    .map((r) => `<tr><td>${cityCell(r.c)}</td>
      <td class="num mono">${r.weekday.toFixed(1)}%</td>
      <td class="num mono">${r.weekend.toFixed(1)}%</td>
      <td class="num mono">${r.edge > 0 ? "+" : ""}${r.edge.toFixed(1)}</td></tr>`).join("");
  $("answers-wkend").innerHTML =
    `<table class="data-table"><tr><th>city</th><th class="num">weekday</th><th class="num">weekend</th><th class="num">edge</th></tr>${wkRows}</table>`;

  const PARTS = ["am_peak", "midday", "pm_peak", "off_hours"];
  const LABEL = { am_peak: "am peak", midday: "midday", pm_peak: "pm peak", off_hours: "off hours" };
  const pk = {};
  for (const r of a.peak_offpeak) (pk[r.city_key] ??= {})[r.day_part] = +r.otp_pct;
  const pkRows = Object.entries(pk).map(([c, v]) =>
    `<tr><td>${cityCell(c)}</td>${PARTS.map((p) =>
      `<td class="num mono">${v[p] != null ? v[p].toFixed(1) + "%" : ""}</td>`).join("")}</tr>`).join("");
  $("answers-peak").innerHTML =
    `<table class="data-table"><tr><th>city</th>${PARTS.map((p) => `<th class="num">${LABEL[p]}</th>`).join("")}</tr>${pkRows}</table>`;

  // rows arrive per city × mode; the city figure is the measured-weighted total
  // and the modes sit under it, so a rail-heavy early habit is visible as such
  const early = {};
  for (const r of a.early_departures) {
    const e = (early[r.city_key] ??= { early: 0, measured: 0, modes: [] });
    e.early += (Number(r.early_dep_pct) / 100) * Number(r.measured);
    e.measured += Number(r.measured);
    e.modes.push(r);
  }
  const earlyRows = Object.entries(early)
    .map(([c, e]) => ({ c, pct: (100 * e.early) / e.measured, measured: e.measured, modes: e.modes.sort((x, y) => Number(y.measured) - Number(x.measured)) }))
    .sort((x, y) => y.pct - x.pct)
    .map((r) => `<tr><td>${cityCell(r.c)}</td>
     <td class="num mono"><span title="${fmt(r.measured)} measured timepoint departures">${r.pct.toFixed(1)}%</span>
     <span class="mode-split">${r.modes.map((m) => `${esc(m.mode)} ${Number(m.early_dep_pct).toFixed(1)}%`).join(" · ")}</span></td></tr>`).join("");
  // Three feeds cannot answer this one. The reason is a property of the feed,
  // not a hole in the data, so it sits on the same rows the measured cities use.
  const EARLY_NA = {
    nyc: "MTA's schedule marks no timepoints",
    zurich: "schedule marks no timepoints; the feed states delays, never a departure",
    tokyo: "no timepoints — Toei states a delay, never a departure",
  };
  const earlyNaRows = Object.entries(EARLY_NA)
    .filter(([c]) => CITIES[c] && !early[c])
    .map(([c, why]) => `<tr><td>${cityCell(c)}</td><td class="num"><span class="na">${why}</span></td></tr>`).join("");
  $("answers-early").innerHTML = a.early_departures.length
    ? `<table class="data-table"><tr><th>city</th><th class="num">left early</th></tr>${earlyRows}${earlyNaRows}</table>` +
      `<p class="footnote">Every mode whose static schedule marks timepoints; the rest say why. Hover for evidence counts.</p>`
    : `<p class="footnote">No timepoint-bearing statics in gold yet.</p>`;

  // Toronto cancels 4 trips in 661,306. Rounded to two places that prints
  // "0.00%" beside a count of 4, which reads as a contradiction rather than as
  // the real finding: this feed CAN say CANCELED and almost never does. Any
  // nonzero count under a hundredth of a percent shows as "<0.01%" instead.
  const cxPct = (r) => {
    const v = Number(r.cancel_pct);
    return Number(r.cancelled) > 0 && v < 0.005 ? "&lt;0.01%" : v.toFixed(2) + "%";
  };
  const cxRows = a.cancellations.map((r) =>
    `<tr><td>${cityCell(r.city_key)}</td>
     <td class="num mono">${r.emits_cancels ? cxPct(r) : "—"}</td>
     <td class="num mono">${r.emits_cancels ? fmt(r.cancelled) : "never emits"}</td></tr>`).join("");
  // the Toronto sentence reads its count and day span from the data, so a
  // regenerated snapshot cannot leave it quoting last week's numbers
  const tor = a.cancellations.find((r) => r.city_key === "toronto");
  const torDays = (summary.standings.find((r) => r.city_key === "toronto") || {}).judged_days;
  const torNote = tor && torDays
    ? ` Toronto can say it, and did — <b>${fmt(tor.cancelled)} times in ${torDays} judged days</b>.`
    : "";
  $("answers-cancel").innerHTML =
    `<table class="data-table"><tr><th>city</th><th class="num">cancelled</th><th class="num">trips</th></tr>${cxRows}</table>` +
    `<p class="footnote">A dash means the feed has no way to say "cancelled".${torNote}</p>`;
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

function delayColorFn(theme) {
  const sem = SEMANTIC[theme];
  const OK = hex2rgb(sem.good), WARN = hex2rgb(sem.warn), BAD = hex2rgb(sem.bad), EARLY = hex2rgb(sem.early);
  return (d) => d == null ? [128, 128, 128] : d < -60 ? EARLY : d < 60 ? OK : d < 300 ? WARN : BAD;
}

function mapLayers(id, data, theme, hero) {
  const sem = SEMANTIC[theme];
  const delayColor = delayColorFn(theme);
  const routeColor = (r) => {
    const info = data.routes[r] || {};
    return hex2rgb(info.route_color) || MODE_COLOR[+info.route_type] || [96, 140, 190];
  };
  const veh = sem.veh.split(",").map(Number), ring = sem.ring.split(",").map(Number);
  // parse once per city: route colors are theme-independent (agency colors),
  // theme only touches alpha, applied in the accessor below
  data._shapes ??= data.shapes.map((s) => ({ path: parsePath(s.path), color: routeColor(s.route_id) }));
  const shapes = data._shapes;
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
      id: id + "-veh", data: data.vehicles, getPosition: (d) => [+d.lon, +d.lat],
      getFillColor: [...veh, 235], getRadius: 2.6, radiusUnits: "pixels",
      stroked: true, getLineColor: [...ring, 200], lineWidthMinPixels: 1,
    }),
  ];
}

function legendHTML(theme, vehicles) {
  const sem = SEMANTIC[theme];
  return `<span><span class="dot" style="background:${sem.early}"></span>early</span>
    <span><span class="dot" style="background:${sem.good}"></span>on time</span>
    <span><span class="dot" style="background:${sem.warn}"></span>1–5 min late</span>
    <span><span class="dot" style="background:${sem.bad}"></span>&gt;5 min late</span>` +
    (vehicles ? `<span><span class="dot" style="background:rgb(${sem.veh});box-shadow:0 0 0 1px rgb(${sem.ring})"></span>vehicle · latest fix</span>` : "");
}

let heroDeck = null, cityDeck = null, activeCity = "nyc", mapMode = "network";

const NETWORK_NOTE = document.getElementById("network-note")?.textContent;
const HEX_NOTE = "Mean arrival delay aggregated to H3 hexagons over the last 7 days, " +
  "scored events only — hexes under 20 events dropped. One shared scale for every city: " +
  "that Zurich reads pale while others glow is the comparison.";

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
  const [data, summary, hexes] = await Promise.all([
    loadJSON(`data/maps/${key}.json`), loadJSON("data/summary.json"),
    // hex data (738KB) only when that layer is actually shown
    mapMode === "hexes" ? loadJSON("data/hexes.json") : null,
  ]);
  frame.querySelector(".map-loading")?.remove();
  if (activeCity !== key) return; // a later tab click won the race
  const [lat, lon, baseZoom] = meta.view;
  const zoom = mapMode === "hexes" ? baseZoom - 1 : baseZoom; // hex view reads city-wide
  const delayColor = delayColorFn(theme);
  const layers = mapMode === "hexes"
    ? [new deck.H3HexagonLayer({
        id: "city-hex",
        data: hexes.rows.filter((r) => r.city_key === key),
        getHexagon: (d) => d.h3_r8,
        getFillColor: (d) => [...delayColor(+d.mean_delay_sec), 145],
        extruded: false,
        pickable: true,
      })]
    : mapLayers("city", data, theme, false);
  const props = {
    mapStyle: MAP_STYLES[theme],
    layers,
    getTooltip: ({ layer, object }) => {
      if (!object) return null;
      if (layer?.id === "city-stops") return { text: `${object.delay}s mean delay` };
      if (layer?.id === "city-hex") return { text: `${object.mean_delay_sec}s mean · ${Number(object.events).toLocaleString()} events` };
      return null;
    },
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
  $("city-legend").innerHTML = legendHTML(theme, mapMode === "network" && data.vehicles.length > 0);
  $("city-note").textContent = mapMode === "hexes" ? HEX_NOTE : (VP_ABSENT[key] ?? "");
  $("network-note").textContent = mapMode === "hexes" ? HEX_NOTE : NETWORK_NOTE;
  document.querySelectorAll("#city-tabs button").forEach((b) =>
    b.setAttribute("aria-selected", String(b.dataset.city === key)));
  document.querySelectorAll("#map-mode button").forEach((b) =>
    b.setAttribute("aria-selected", String(b.dataset.mode === mapMode)));
}

function renderTabs() {
  $("city-tabs").innerHTML = Object.entries(CITIES).map(([key, c]) =>
    `<button role="tab" data-city="${key}" aria-selected="${key === activeCity}">${c.name.toLowerCase()}</button>`).join("");
  $("city-tabs").querySelectorAll("button").forEach((b) =>
    b.addEventListener("click", () => renderCityMap(b.dataset.city, true)));
  $("map-mode").querySelectorAll("button").forEach((b) =>
    b.addEventListener("click", () => {
      mapMode = b.dataset.mode;
      renderCityMap(activeCity, true);
    }));
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
  renderAnswers();
  renderStorage();
  renderRoutes();
  renderHero();
  renderCityMap(activeCity, false);
}

function updateToggle() {
  const target = currentTheme() === "dark" ? "light" : "dark";
  const btn = $("theme-toggle");
  btn.setAttribute("aria-label", `Switch to ${target} theme`);
  btn.title = `Switch to ${target} theme`;
  btn.querySelector(".corner-fill").style.background = SIDEBAR_FILL[target];
}

$("theme-toggle").addEventListener("click", () => {
  const target = currentTheme() === "dark" ? "light" : "dark";
  const apply = () => {
    document.documentElement.setAttribute("data-theme", target);
    persistTheme(target);
    updateToggle();
    rethemeAll();
  };
  const reduced = matchMedia("(prefers-reduced-motion: reduce)").matches;
  if (!document.startViewTransition || reduced) { apply(); return; }
  const transition = document.startViewTransition(apply);
  transition.ready.then(() => {
    const radius = Math.hypot(innerWidth, innerHeight);
    document.documentElement.animate(
      { clipPath: ["circle(0px at 0px 0px)", `circle(${radius}px at 0px 0px)`] },
      { duration: 700, easing: "cubic-bezier(0.4, 0, 0.2, 1)", pseudoElement: "::view-transition-new(root)" },
    );
  });
});
matchMedia("(prefers-color-scheme: dark)").addEventListener("change", () => {
  if (!document.documentElement.getAttribute("data-theme")) { updateToggle(); rethemeAll(); }
});
updateToggle();
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
renderAnswers();
renderStorage();
renderRoutes();
renderHero();
renderTabs();
renderCityMap("nyc", true);
setupReveal();
