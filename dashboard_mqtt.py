# =============================================================================
#  dashboard_mqtt.py — DC Motor Digital Twin Dashboard (MQTT Version)
#  Broker  : broker.hivemq.com (public, port 1883)
#  Topic   : motordignostic
#
#  Install dependencies:
#    pip install fastapi uvicorn paho-mqtt
#
#  Run:   python dashboard_mqtt.py
#  Open:  http://localhost:8000
#
#  Flow:
#    ESP32 → MQTT broker → paho-mqtt (Python) → FastAPI WebSocket → Browser
# =============================================================================

import asyncio
import json
import threading
import time
import webbrowser

import paho.mqtt.client as mqtt
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

# =============================================================================
# MQTT CONFIG
# =============================================================================
MQTT_BROKER  = "broker.hivemq.com"
MQTT_PORT    = 1883
MQTT_TOPIC   = "motordignostic"
MQTT_CLIENT  = "Dashboard_MotorTwin_" + str(int(time.time()))  # Unique ID

# =============================================================================
# FASTAPI APP
# =============================================================================
app = FastAPI(title="DC Motor Digital Twin — MQTT Dashboard")

# Shared queue: MQTT thread → asyncio event loop → browser WebSockets
mqtt_queue: asyncio.Queue = None
browser_clients: list[WebSocket] = []
loop: asyncio.AbstractEventLoop = None

# =============================================================================
# MQTT CALLBACKS
# =============================================================================
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"[MQTT] Connected to {MQTT_BROKER}")
        client.subscribe(MQTT_TOPIC, qos=0)
        print(f"[MQTT] Subscribed to topic: {MQTT_TOPIC}")
    else:
        print(f"[MQTT] Connection failed. Code: {rc}")


def on_message(client, userdata, msg):
    """Called in MQTT thread — put data into asyncio queue."""
    payload = msg.payload.decode("utf-8")
    if mqtt_queue and loop:
        # Thread-safe: schedule put in the event loop
        asyncio.run_coroutine_threadsafe(mqtt_queue.put(payload), loop)


def on_disconnect(client, userdata, rc):
    print(f"[MQTT] Disconnected. Code: {rc}. Auto-reconnect active.")


# =============================================================================
# MQTT CLIENT SETUP (runs in background thread)
# =============================================================================
def start_mqtt():
    client = mqtt.Client(client_id=MQTT_CLIENT, clean_session=True)
    client.on_connect    = on_connect
    client.on_message    = on_message
    client.on_disconnect = on_disconnect

    client.reconnect_delay_set(min_delay=2, max_delay=10)

    try:
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    except Exception as e:
        print(f"[MQTT] Initial connect error: {e}")

    # Blocking loop — handles reconnects automatically
    client.loop_forever()


# =============================================================================
# BACKGROUND TASK: broadcast MQTT messages to all browser WebSocket clients
# =============================================================================
async def mqtt_broadcaster():
    """Reads from queue and pushes to all connected browser tabs."""
    global mqtt_queue
    mqtt_queue = asyncio.Queue()

    while True:
        payload = await mqtt_queue.get()
        dead = []
        for ws in browser_clients:
            try:
                await ws.send_text(payload)
            except Exception:
                dead.append(ws)
        for d in dead:
            browser_clients.remove(d)


# =============================================================================
# STARTUP EVENT — init event loop ref + background tasks
# =============================================================================
@app.on_event("startup")
async def startup():
    global loop
    loop = asyncio.get_event_loop()

    # Start MQTT in background thread
    threading.Thread(target=start_mqtt, daemon=True).start()
    print("[APP] MQTT thread started.")

    # Start broadcaster coroutine
    asyncio.create_task(mqtt_broadcaster())
    print("[APP] MQTT broadcaster task started.")


# =============================================================================
# WEBSOCKET ENDPOINT — browser clients connect here
# =============================================================================
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    browser_clients.append(websocket)
    print(f"[WS] Browser connected. Total: {len(browser_clients)}")
    try:
        while True:
            await websocket.receive_text()   # Keep alive
    except WebSocketDisconnect:
        browser_clients.remove(websocket)
        print(f"[WS] Browser disconnected. Total: {len(browser_clients)}")


# =============================================================================
# HTTP ROOT — Serves the dashboard
# =============================================================================
@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    return HTMLResponse(content=HTML_CONTENT)


# =============================================================================
# EMBEDDED DASHBOARD HTML / CSS / JS
# =============================================================================
HTML_CONTENT = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>DC Motor Digital Twin — MQTT</title>

<link href="https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Exo+2:wght@300;400;600;700&display=swap" rel="stylesheet"/>
<script src="https://cdn.plot.ly/plotly-latest.min.js"></script>

<script type="importmap">
{
  "imports": {
    "three": "https://unpkg.com/three@0.158.0/build/three.module.js",
    "three/addons/": "https://unpkg.com/three@0.158.0/examples/jsm/"
  }
}
</script>

<style>
:root {
  --bg:       #0a0c10;
  --panel:    #0f1318;
  --border:   #1e2530;
  --accent:   #00e5ff;
  --accent2:  #ff6b35;
  --green:    #00ff88;
  --orange:   #ffaa00;
  --red:      #ff3355;
  --text:     #c8d8e8;
  --text-dim: #4a5a6a;
  --mono:     'Share Tech Mono', monospace;
  --main:     'Exo 2', sans-serif;
  --radius:   8px;
}
* { box-sizing: border-box; margin: 0; padding: 0; }

body {
  background: var(--bg);
  color: var(--text);
  font-family: var(--main);
  min-height: 100vh;
  overflow-x: hidden;
}
body::before {
  content: '';
  position: fixed; inset: 0;
  background: repeating-linear-gradient(0deg, transparent, transparent 2px, rgba(0,0,0,0.04) 2px, rgba(0,0,0,0.04) 4px);
  pointer-events: none; z-index: 999;
}

/* HEADER */
header {
  display: flex; align-items: center; justify-content: space-between;
  padding: 14px 28px;
  background: var(--panel);
  border-bottom: 1px solid var(--border);
  position: sticky; top: 0; z-index: 100;
}
.header-left h1 {
  font-size: 1.05rem; font-weight: 700; letter-spacing: 3px;
  text-transform: uppercase; color: var(--accent);
  text-shadow: 0 0 12px rgba(0,229,255,0.5);
}
.header-left p { font-family: var(--mono); font-size: 0.62rem; color: var(--text-dim); letter-spacing: 2px; margin-top: 2px; }
.header-right  { display: flex; align-items: center; gap: 14px; }

/* Badges */
#conn-badge {
  font-family: var(--mono); font-size: 0.7rem; padding: 5px 14px;
  border-radius: 20px; letter-spacing: 1px; transition: all 0.3s;
}
#conn-badge.connected    { background: rgba(0,255,136,0.1); color: var(--green);  border: 1px solid var(--green); }
#conn-badge.disconnected { background: rgba(255,51,85,0.1);  color: var(--red);    border: 1px solid var(--red); }

#mqtt-badge {
  font-family: var(--mono); font-size: 0.65rem; padding: 4px 12px;
  border-radius: 20px; letter-spacing: 1px;
  background: rgba(0,229,255,0.07); color: var(--accent); border: 1px solid var(--border);
}

#health-badge {
  font-family: var(--mono); font-size: 0.75rem; padding: 5px 18px;
  border-radius: 20px; letter-spacing: 2px; font-weight: 700; text-transform: uppercase; transition: all 0.4s;
}
#health-badge.ok   { background: rgba(0,255,136,0.1); color: var(--green);  border: 1px solid var(--green); }
#health-badge.warn { background: rgba(255,170,0,0.1);  color: var(--orange); border: 1px solid var(--orange); }
#health-badge.crit { background: rgba(255,51,85,0.12); color: var(--red);    border: 1px solid var(--red); box-shadow: 0 0 14px rgba(255,51,85,0.35); }

#export-btn {
  font-family: var(--mono); font-size: 0.68rem; letter-spacing: 1px;
  padding: 6px 16px; background: transparent; border: 1px solid var(--accent);
  color: var(--accent); border-radius: var(--radius); cursor: pointer; transition: all 0.2s;
}
#export-btn:hover { background: rgba(0,229,255,0.08); box-shadow: 0 0 14px rgba(0,229,255,0.2); }

/* MAIN */
main { max-width: 1400px; margin: 0 auto; padding: 20px 24px 40px; }

/* FAULT ROW */
.fault-row { display: grid; grid-template-columns: repeat(4,1fr); gap: 12px; margin-bottom: 20px; }
.fault-card {
  background: var(--panel); border: 1px solid var(--border);
  border-radius: var(--radius); padding: 12px 16px;
  display: flex; align-items: center; gap: 10px; transition: all 0.3s;
}
.fault-card .icon  { font-size: 1.3rem; }
.fault-card .label { font-size: 0.63rem; letter-spacing: 2px; text-transform: uppercase; color: var(--text-dim); font-family: var(--mono); }
.fault-card .state { font-size: 0.78rem; font-weight: 700; font-family: var(--mono); margin-top: 2px; }
.fault-card.active   { border-color: var(--red);   background: rgba(255,51,85,0.06);  box-shadow: 0 0 14px rgba(255,51,85,0.18); }
.fault-card.inactive { border-color: var(--border);}
.fault-card.active   .state { color: var(--red);   }
.fault-card.inactive .state { color: var(--green); }

/* GRAPHS */
.graphs-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; margin-bottom: 20px; }
.graph-card {
  background: var(--panel); border: 1px solid var(--border);
  border-radius: var(--radius); padding: 14px 14px 8px;
  position: relative; transition: border-color 0.3s, box-shadow 0.3s;
}
.graph-card.alert { border-color: var(--red); box-shadow: 0 0 18px rgba(255,51,85,0.18); }
.graph-title { font-size: 0.63rem; letter-spacing: 3px; text-transform: uppercase; color: var(--text-dim); font-family: var(--mono); margin-bottom: 6px; }
.live-val    { position: absolute; top: 14px; right: 14px; font-family: var(--mono); font-size: 1.1rem; color: var(--accent); font-weight: 700; }
.warn-tag    { display: none; position: absolute; top: 14px; right: 110px; font-family: var(--mono); font-size: 0.6rem; color: var(--red); border: 1px solid var(--red); padding: 2px 8px; border-radius: 10px; }
.graph-card.alert .warn-tag { display: block; }
.plotly-graph { width: 100%; height: 200px; }

/* BOTTOM */
.bottom-section { display: grid; grid-template-columns: 320px 1fr; gap: 14px; margin-bottom: 20px; }

/* SLIDERS */
.sliders-panel { background: var(--panel); border: 1px solid var(--border); border-radius: var(--radius); padding: 18px; }
.sliders-panel h3 { font-size: 0.6rem; letter-spacing: 3px; text-transform: uppercase; color: var(--text-dim); font-family: var(--mono); margin-bottom: 18px; border-bottom: 1px solid var(--border); padding-bottom: 8px; }
.slider-group { margin-bottom: 18px; }
.slider-group label { display: flex; justify-content: space-between; font-size: 0.72rem; font-family: var(--mono); color: var(--text); margin-bottom: 8px; }
.slider-group label span { color: var(--accent); font-weight: 700; }
input[type=range] { width: 100%; -webkit-appearance: none; height: 4px; background: var(--border); border-radius: 2px; outline: none; }
input[type=range]::-webkit-slider-thumb { -webkit-appearance: none; width: 14px; height: 14px; border-radius: 50%; background: var(--accent); cursor: pointer; box-shadow: 0 0 8px rgba(0,229,255,0.5); }

/* RUL */
.rul-section { margin-top: 20px; padding-top: 16px; border-top: 1px solid var(--border); }
.rul-section h3 { font-size: 0.6rem; letter-spacing: 3px; text-transform: uppercase; color: var(--text-dim); font-family: var(--mono); margin-bottom: 14px; }
.rul-item { display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px; padding: 8px 10px; background: rgba(0,0,0,0.3); border-radius: 6px; border: 1px solid var(--border); }
.rul-label { font-family: var(--mono); font-size: 0.68rem; color: var(--text-dim); }
.rul-value { font-family: var(--mono); font-size: 0.9rem; font-weight: 700; color: var(--green); }
.rul-value.warn { color: var(--orange); }
.rul-value.crit { color: var(--red); }

/* 3D MOTOR */
.motor-panel { background: var(--panel); border: 1px solid var(--border); border-radius: var(--radius); padding: 14px; display: flex; flex-direction: column; }
.motor-panel h3 { font-size: 0.6rem; letter-spacing: 3px; text-transform: uppercase; color: var(--text-dim); font-family: var(--mono); margin-bottom: 10px; }
#motor-canvas-wrap { flex: 1; min-height: 320px; border-radius: 6px; overflow: hidden; background: #05070a; }
#motor-canvas-wrap canvas { display: block; }
.motor-hint { font-family: var(--mono); font-size: 0.6rem; color: var(--text-dim); text-align: center; margin-top: 6px; }

@media (max-width: 900px) {
  .graphs-grid    { grid-template-columns: 1fr; }
  .fault-row      { grid-template-columns: 1fr 1fr; }
  .bottom-section { grid-template-columns: 1fr; }
}
</style>
</head>
<body>

<!-- HEADER -->
<header>
  <div class="header-left">
    <h1>⚡ DC Motor Digital Twin</h1>
    <p>MQTT Dashboard — broker.hivemq.com → topic: motordignostic</p>
  </div>
  <div class="header-right">
    <span id="mqtt-badge">📡 MQTT: broker.hivemq.com</span>
    <span id="conn-badge" class="disconnected">◉ DISCONNECTED</span>
    <span id="health-badge" class="ok">HEALTH: OK</span>
    <button id="export-btn" onclick="exportCSV()">⬇ EXPORT CSV</button>
  </div>
</header>

<main>

  <!-- FAULT FLAGS -->
  <div class="fault-row">
    <div class="fault-card inactive" id="fc-overheat">
      <div class="icon">🔥</div>
      <div><div class="label">Overheat</div><div class="state" id="fs-overheat">NORMAL</div></div>
    </div>
    <div class="fault-card inactive" id="fc-overcurrent">
      <div class="icon">⚡</div>
      <div><div class="label">Overcurrent</div><div class="state" id="fs-overcurrent">NORMAL</div></div>
    </div>
    <div class="fault-card inactive" id="fc-vibration">
      <div class="icon">📳</div>
      <div><div class="label">Vib Persist</div><div class="state" id="fs-vibration">NORMAL</div></div>
    </div>
    <div class="fault-card inactive" id="fc-rpmdrop">
      <div class="icon">🔄</div>
      <div><div class="label">RPM Drop</div><div class="state" id="fs-rpmdrop">NORMAL</div></div>
    </div>
  </div>

  <!-- GRAPHS -->
  <div class="graphs-grid">
    <div class="graph-card" id="gc-temp">
      <div class="graph-title">Temperature</div>
      <div class="live-val" id="lv-temp">— °C</div>
      <div class="warn-tag">⚠ ALERT</div>
      <div id="plot-temp" class="plotly-graph"></div>
    </div>
    <div class="graph-card" id="gc-current">
      <div class="graph-title">Current</div>
      <div class="live-val" id="lv-current">— A</div>
      <div class="warn-tag">⚠ ALERT</div>
      <div id="plot-current" class="plotly-graph"></div>
    </div>
    <div class="graph-card" id="gc-rpm">
      <div class="graph-title">RPM</div>
      <div class="live-val" id="lv-rpm">— RPM</div>
      <div class="warn-tag">⚠ ALERT</div>
      <div id="plot-rpm" class="plotly-graph"></div>
    </div>
    <div class="graph-card" id="gc-vib">
      <div class="graph-title">Vibration</div>
      <div class="live-val" id="lv-vib">—</div>
      <div class="warn-tag">⚠ ALERT</div>
      <div id="plot-vib" class="plotly-graph"></div>
    </div>
  </div>

  <!-- BOTTOM -->
  <div class="bottom-section">
    <div class="sliders-panel">
      <h3>Alert Thresholds</h3>
      <div class="slider-group">
        <label>Vibration (g) <span id="sv-vib">5.0</span></label>
        <input type="range" id="sl-vib" min="1.0" max="10.0" step="0.1" value="5.0"
               oninput="document.getElementById('sv-vib').textContent=parseFloat(this.value).toFixed(1)"/>
      </div>
      <div class="slider-group">
        <label>Temperature (°C) <span id="sv-temp">85</span></label>
        <input type="range" id="sl-temp" min="40" max="100" step="1" value="85"
               oninput="document.getElementById('sv-temp').textContent=this.value"/>
      </div>
      <div class="slider-group">
        <label>Current (A) <span id="sv-cur">4.5</span></label>
        <input type="range" id="sl-cur" min="1.0" max="15.0" step="0.1" value="4.5"
               oninput="document.getElementById('sv-cur').textContent=parseFloat(this.value).toFixed(1)"/>
      </div>
      <div class="rul-section">
        <h3>Remaining Useful Life</h3>
        <div class="rul-item">
          <span class="rul-label">🌡 TEMPERATURE</span>
          <span class="rul-value" id="rul-temp">— days</span>
        </div>
        <div class="rul-item">
          <span class="rul-label">⚡ CURRENT</span>
          <span class="rul-value" id="rul-cur">— days</span>
        </div>
        <div class="rul-item">
          <span class="rul-label">📳 VIBRATION</span>
          <span class="rul-value" id="rul-vib">— days</span>
        </div>
      </div>
    </div>

    <div class="motor-panel">
      <h3>3D Motor Model — Interactive View</h3>
      <div id="motor-canvas-wrap"></div>
      <p class="motor-hint">🖱 Drag to rotate · Scroll to zoom</p>
    </div>
  </div>

</main>

<!-- JAVASCRIPT -->
<script>
const MAX_PTS    = 200;
const RUL_WINDOW = 120;

let times = [], tempData = [], currentData = [], rpmData = [], vibData = [];

// Plotly dark layout
const darkLayout = (yTitle, yRange) => ({
  paper_bgcolor: 'transparent', plot_bgcolor: 'rgba(0,0,0,0)',
  font: { color: '#4a5a6a', family: 'Share Tech Mono, monospace', size: 10 },
  xaxis: { gridcolor: '#1a2030', showgrid: true, zeroline: false, showticklabels: false },
  yaxis: { gridcolor: '#1a2030', showgrid: true, zeroline: false,
           title: { text: yTitle, font: { size: 9 } }, range: yRange || null },
  margin: { l: 42, r: 8, t: 4, b: 8 },
  showlegend: false, hovermode: 'x'
});

const traceStyle = (color) => ({
  x: [], y: [], type: 'scatter', mode: 'lines',
  line: { color, width: 1.8, shape: 'spline' },
  fill: 'tozeroy',
  fillcolor: color.replace(')', ',0.07)').replace('rgb', 'rgba')
});

Plotly.newPlot('plot-temp',    [traceStyle('rgb(0,229,255)')],  darkLayout('°C'));
Plotly.newPlot('plot-current', [traceStyle('rgb(255,107,53)')], darkLayout('A'));
Plotly.newPlot('plot-rpm',     [traceStyle('rgb(0,255,136)')],  darkLayout('RPM'));
Plotly.newPlot('plot-vib',     [traceStyle('rgb(255,170,0)')],  darkLayout('State', [-0.1,1.1]));

// WebSocket to FastAPI (which relays MQTT)
let ws, reconnectTimer;

function connectWS() {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  ws = new WebSocket(`${proto}://${location.host}/ws`);

  ws.onopen = () => {
    document.getElementById('conn-badge').className = 'connected';
    document.getElementById('conn-badge').textContent = '◉ CONNECTED';
    clearTimeout(reconnectTimer);
  };
  ws.onclose = () => {
    document.getElementById('conn-badge').className = 'disconnected';
    document.getElementById('conn-badge').textContent = '◉ DISCONNECTED';
    reconnectTimer = setTimeout(connectWS, 3000);
  };
  ws.onerror = () => ws.close();
  ws.onmessage = (event) => {
    let parsed;
    try { parsed = JSON.parse(event.data); } catch { return; }
    const items = Array.isArray(parsed) ? parsed : [parsed];
    items.forEach(processData);
  };
}

function processData(d) {
  const t = new Date().toISOString();
  times.push(t); tempData.push(d.temperature);
  currentData.push(d.current); rpmData.push(d.rpm); vibData.push(d.vibration);

  if (times.length > MAX_PTS) {
    times.shift(); tempData.shift(); currentData.shift(); rpmData.shift(); vibData.shift();
  }

  Plotly.extendTraces('plot-temp',    { x: [[t]], y: [[d.temperature]] }, [0], MAX_PTS);
  Plotly.extendTraces('plot-current', { x: [[t]], y: [[d.current]] },     [0], MAX_PTS);
  Plotly.extendTraces('plot-rpm',     { x: [[t]], y: [[d.rpm]] },         [0], MAX_PTS);
  Plotly.extendTraces('plot-vib',     { x: [[t]], y: [[d.vibration]] },   [0], MAX_PTS);

  document.getElementById('lv-temp').textContent    = d.temperature.toFixed(1) + ' °C';
  document.getElementById('lv-current').textContent = d.current.toFixed(2) + ' A';
  document.getElementById('lv-rpm').textContent     = Math.round(d.rpm) + ' RPM';
  document.getElementById('lv-vib').textContent     = d.vibration ? 'HIGH' : 'OK';

  const thr = {
    temp: parseFloat(document.getElementById('sl-temp').value),
    cur:  parseFloat(document.getElementById('sl-cur').value),
    vib:  parseFloat(document.getElementById('sl-vib').value),
  };
  setAlert('gc-temp',    d.temperature >= thr.temp);
  setAlert('gc-current', d.current     >= thr.cur);
  setAlert('gc-vib',     d.vibration   >= thr.vib);
  setAlert('gc-rpm',     false);

  updateHealth(d.health);

  if (d.fault_flags) {
    setFault('fc-overheat',    'fs-overheat',   d.fault_flags.overheat);
    setFault('fc-overcurrent', 'fs-overcurrent', d.fault_flags.overcurrent);
    setFault('fc-vibration',   'fs-vibration',  d.fault_flags.vibration_persist);
    setFault('fc-rpmdrop',     'fs-rpmdrop',    d.fault_flags.rpm_drop);
  }
  updateRUL();
}

function setAlert(id, active) {
  document.getElementById(id).classList.toggle('alert', active);
}
function setFault(cardId, stateId, active) {
  document.getElementById(cardId).className = 'fault-card ' + (active ? 'active' : 'inactive');
  document.getElementById(stateId).textContent = active ? 'FAULT' : 'NORMAL';
}
function updateHealth(h) {
  const el = document.getElementById('health-badge');
  if      (h === 'Critical') { el.className = 'crit'; el.textContent = 'HEALTH: CRITICAL'; }
  else if (h === 'Warning')  { el.className = 'warn'; el.textContent = 'HEALTH: WARNING'; }
  else                       { el.className = 'ok';   el.textContent = 'HEALTH: OK'; }
}

// RUL
function calcRUL(arr, critVal) {
  const n = Math.min(arr.length, RUL_WINDOW);
  if (n < 2) return null;
  const sl = arr.slice(-n);
  const xm = (n-1)/2, ym = sl.reduce((a,b)=>a+b,0)/n;
  let num=0, den=0;
  for (let i=0;i<n;i++) { num+=(i-xm)*(sl[i]-ym); den+=(i-xm)**2; }
  if (den===0) return null;
  const slope = num/den;
  if (slope <= 0) return Infinity;
  const rem = critVal - sl[n-1];
  if (rem <= 0) return 0;
  return (rem / slope * 0.5) / 86400;
}
function fmtRUL(d) {
  if (d===null) return '— days';
  if (d===Infinity) return '∞ days';
  if (d<=0) return '0.0 days';
  return d.toFixed(1) + ' days';
}
function rulClass(d) {
  if (d===null||d===Infinity) return '';
  if (d<1) return 'crit'; if (d<7) return 'warn'; return '';
}
function updateRUL() {
  const rt=calcRUL(tempData,95), rc=calcRUL(currentData,6), rv=calcRUL(vibData,1);
  const et=document.getElementById('rul-temp'), ec=document.getElementById('rul-cur'), ev=document.getElementById('rul-vib');
  et.textContent=fmtRUL(rt); ec.textContent=fmtRUL(rc); ev.textContent=fmtRUL(rv);
  et.className='rul-value '+rulClass(rt); ec.className='rul-value '+rulClass(rc); ev.className='rul-value '+rulClass(rv);
}

// CSV Export
function exportCSV() {
  if (!times.length) { alert('No data yet.'); return; }
  let csv = 'Timestamp,Temperature_C,Current_A,RPM,Vibration\n';
  for (let i=0;i<times.length;i++) csv+=`${times[i]},${tempData[i]},${currentData[i]},${rpmData[i]},${vibData[i]}\n`;
  const a = document.createElement('a');
  a.href = URL.createObjectURL(new Blob([csv],{type:'text/csv'}));
  a.download = 'motor_mqtt_' + new Date().toISOString().slice(0,19).replace(/:/g,'-') + '.csv';
  a.click();
}

connectWS();
</script>

<!-- THREE.JS 3D MOTOR -->
<script type="module">
import * as THREE from 'three';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';

const wrap = document.getElementById('motor-canvas-wrap');
const W = wrap.clientWidth || 600, H = wrap.clientHeight || 320;

const scene    = new THREE.Scene();
scene.background = new THREE.Color(0x05070a);
scene.fog        = new THREE.FogExp2(0x05070a, 0.08);

const renderer = new THREE.WebGLRenderer({ antialias: true });
renderer.setSize(W, H);
renderer.setPixelRatio(window.devicePixelRatio);
wrap.appendChild(renderer.domElement);

const camera = new THREE.PerspectiveCamera(45, W/H, 0.1, 100);
camera.position.set(0, 1.5, 5);

const controls = new OrbitControls(camera, renderer.domElement);
controls.enableDamping = true; controls.dampingFactor = 0.06;
controls.minDistance = 2; controls.maxDistance = 12;

// Lights
scene.add(new THREE.AmbientLight(0x112233, 1.2));
const kl = new THREE.PointLight(0x00e5ff, 3, 15);
kl.position.set(3, 4, 3); scene.add(kl);
const rl = new THREE.PointLight(0xff6b35, 2, 10);
rl.position.set(-4, -2, -3); scene.add(rl);

// Materials
const bodyMat  = new THREE.MeshStandardMaterial({ color: 0x1a2535, metalness: 0.9, roughness: 0.25 });
const capMat   = new THREE.MeshStandardMaterial({ color: 0x0f1820, metalness: 0.95, roughness: 0.15 });
const ringMat  = new THREE.MeshStandardMaterial({ color: 0x00e5ff, metalness: 0.8, roughness: 0.1, emissive: 0x003344, emissiveIntensity: 0.5 });
const shaftMat = new THREE.MeshStandardMaterial({ color: 0xaabbcc, metalness: 1.0, roughness: 0.1 });
const wireMat  = new THREE.MeshStandardMaterial({ color: 0xff6b35, metalness: 0.3, roughness: 0.6, emissive: 0x441100, emissiveIntensity: 0.4 });

// Motor body
const body = new THREE.Mesh(new THREE.CylinderGeometry(0.9,0.9,2.6,64), bodyMat);
body.rotation.z = Math.PI/2; scene.add(body);

// End caps
const capL = new THREE.Mesh(new THREE.CylinderGeometry(0.92,0.92,0.18,64), capMat);
const capR = new THREE.Mesh(new THREE.CylinderGeometry(0.92,0.92,0.18,64), capMat);
capL.rotation.z = capR.rotation.z = Math.PI/2;
capL.position.x = -1.39; capR.position.x = 1.39;
scene.add(capL, capR);

// Rotating rings
const ring  = new THREE.Mesh(new THREE.TorusGeometry(0.94,0.06,16,80), ringMat);
const ring2 = new THREE.Mesh(new THREE.TorusGeometry(0.88,0.03,12,80), ringMat);
ring.rotation.y = ring2.rotation.y = Math.PI/2;
scene.add(ring, ring2);

// Shaft
const shaft = new THREE.Mesh(new THREE.CylinderGeometry(0.12,0.12,1.0,32), shaftMat);
shaft.rotation.z = Math.PI/2; shaft.position.x = 1.9; scene.add(shaft);

// Wires
for (let i=0;i<3;i++) {
  const w = new THREE.Mesh(new THREE.CylinderGeometry(0.04,0.04,0.5,12), wireMat);
  w.rotation.z = Math.PI/2; w.position.set(-1.6, 0.5-i*0.25, 0.7); scene.add(w);
}

// Grid
const grid = new THREE.GridHelper(12,20,0x1a2530,0x0f1820);
grid.position.y = -1.4; scene.add(grid);

window.addEventListener('resize', () => {
  const w=wrap.clientWidth, h=wrap.clientHeight||320;
  renderer.setSize(w,h); camera.aspect=w/h; camera.updateProjectionMatrix();
});

let angle = 0;
function animate() {
  requestAnimationFrame(animate); angle += 0.012;
  ring.rotation.z  =  angle * 2;
  ring2.rotation.z = -angle * 1.5;
  kl.intensity = 2.8 + Math.sin(angle*1.5)*0.4;
  controls.update(); renderer.render(scene, camera);
}
animate();
</script>

</body>
</html>"""


# =============================================================================
# AUTO-OPEN BROWSER
# =============================================================================
def open_browser():
    time.sleep(2.0)
    webbrowser.open("http://localhost:8000")


# =============================================================================
# ENTRY POINT
# =============================================================================
if __name__ == "__main__":
    print("=" * 60)
    print("  DC Motor Digital Twin — MQTT Dashboard")
    print(f"  Broker : {MQTT_BROKER}:{MQTT_PORT}")
    print(f"  Topic  : {MQTT_TOPIC}")
    print("  URL    : http://localhost:8000")
    print("=" * 60)

    threading.Thread(target=open_browser, daemon=True).start()
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
