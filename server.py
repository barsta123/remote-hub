"""
Remote Hub - serwer podgladu ekranu telefonu na zywo (v5).

Sciezki:
  GET  /        -> strona panelu (3 zakladki: Podglad / Telefony / Ustawienia)
  WS   /        -> relay: ramki z telefonu -> przegladarki, komendy -> telefon
  GET  /health  -> status serwera (diagnostyka)
  GET  /phones  -> lista wszystkich telefonow z apka (JSON, takze offline)
  GET  /api/state -> pelny stan serwera (JSON, do debugowania)

Zabezpieczenie (opcjonalne): ustaw zmienna srodowiskowa ACCESS_PIN na Render.
Wtedy przegladarki musza podac kod dostepu (strona pyta), a telefony wysylaja
go w hello ( pole "pin" ). Bez zmiennej - panel otwarty (jak w v1-v4).

Uruchomienie lokalne:   python server.py
Procfile (Render):      uvicorn server:app --host 0.0.0.0 --port $PORT
"""

import asyncio
import hmac
import json
import os
import time

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse

app = FastAPI(title="Remote Hub")

DEBUG = os.environ.get("DEBUG", "") == "1"

# Kod dostepu do panelu; pusty = panel otwarty dla wszystkich.
ACCESS_PIN = os.environ.get("ACCESS_PIN", "").strip()

# ---------------------------------------------------------------------------
# Stan polaczen
# ---------------------------------------------------------------------------
# clients[ws] = {
#   "kind":  "phone" | "viewer",
#   "id":    "staly id urzadzenia",
#   "model": "np. samsung SM-A166B",
#   "sub":   id ogladanego telefonu albo None (auto = najaktywniejszy),
#   "authed": czy przegladarka podala poprawny PIN,
#   "tries": ile razy probowala zlel kodu,
#   "frames": ile klatek wyslal w tym polaczeniu,
#   "last_frame": timestamp ostatniej klatki,
#   "connected": timestamp polaczenia,
# }
clients: dict = {}
relay_lock = asyncio.Lock()

PHONE_IDLE_ONLINE = 8.0      # po ilu sekundach bez klatki telefon = offline
PHONES_PUSH_INTERVAL = 5.0   # co ile sekund odswiezamy liste telefonu w przegladarkach
AUTH_TIMEOUT = 20.0          # ile sekund przegladarka ma na podanie PINu

# ---------------------------------------------------------------------------
# Log zdarzen widoczny w zakladce "Ustawienia" panelu
# ---------------------------------------------------------------------------
events: list = []
EVENTS_MAX = 40


def now() -> float:
    return time.time()


def log_event(text: str):
    events.append({"ts": int(now()), "text": text})
    if len(events) > EVENTS_MAX:
        del events[: len(events) - EVENTS_MAX]
    print(f"[event] {text}", flush=True)


# ---------------------------------------------------------------------------
# Spis wszystkich telefonow, ktore kiedykolwiek polaczyly sie z apka.
# Dzieki niu menu wyboru pokazuje tez urzadzenia offline.
# ---------------------------------------------------------------------------
KNOWN_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "phones.json")
known: dict = {}   # id -> {"model": str, "name": str|None, "last_seen": float}


def load_known():
    try:
        with open(KNOWN_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                known.update(data)
        print(f"[known] wczytano {len(known)} telefonow", flush=True)
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"[known] blad wczytywania: {e}", flush=True)


def save_known():
    try:
        with open(KNOWN_FILE, "w", encoding="utf-8") as f:
            json.dump(known, f, ensure_ascii=False, indent=1)
    except Exception as e:
        print(f"[known] zapis nieudany: {e}", flush=True)


def remember_phone(dev_id, model):
    k = known.get(dev_id) or {}
    if model:
        k["model"] = model
    k["last_seen"] = now()
    known[dev_id] = k
    save_known()


def phone_list():
    """Lista telefonow dla przegladarek: polaczone + pamietane offline."""
    out = []
    connected_ids = set()
    for ws, c in list(clients.items()):
        if c["kind"] != "phone":
            continue
        connected_ids.add(c["id"])
        idle = now() - (c["last_frame"] or c["connected"])
        k = known.get(c["id"]) or {}
        out.append(
            {
                "id": c["id"],
                "model": c["model"],
                "name": k.get("name"),
                "frames": c["frames"],
                "online": idle <= PHONE_IDLE_ONLINE,
                "since": round(idle, 1),
            }
        )
    for dev_id, k in known.items():
        if dev_id in connected_ids:
            continue
        idle = max(0.0, now() - float(k.get("last_seen") or 0.0))
        out.append(
            {
                "id": dev_id,
                "model": k.get("model") or "nieznany",
                "name": k.get("name"),
                "frames": 0,
                "online": False,
                "since": round(idle, 1),
            }
        )
    out.sort(key=lambda p: (not p["online"], (p["name"] or p["model"]).lower()))
    return out


def all_phones():
    return [ws for ws, c in clients.items() if c["kind"] == "phone"]


def most_active_phone():
    phones = all_phones()
    if not phones:
        return None
    return max(phones, key=lambda ws: clients[ws]["last_frame"])


def resolve_phone(sub_id):
    if sub_id:
        for ws, c in clients.items():
            if c["kind"] == "phone" and c["id"] == sub_id:
                return ws
    return most_active_phone()


async def send_to(ws, data) -> bool:
    try:
        if isinstance(data, (bytes, bytearray)):
            await ws.send_bytes(data)
        else:
            await ws.send_text(data)
        return True
    except (WebSocketDisconnect, RuntimeError, ConnectionError, OSError):
        discard_client(ws)
        return False
    except Exception as exc:
        print(f"[relay] unexpected send error ({type(exc).__name__}: {exc})", flush=True)
        return False


def discard_client(ws):
    clients.pop(ws, None)


def phones_payload():
    auto = most_active_phone()
    return json.dumps(
        {
            "t": "phones",
            "list": phone_list(),
            "auto": clients[auto]["id"] if auto else None,
            "events": events[-15:],
            "pin": bool(ACCESS_PIN),
        }
    )


async def push_phones(ws_list=None):
    payload = phones_payload()
    targets = [
        w
        for w, c in list(clients.items())
        if c["kind"] == "viewer" and c.get("authed", True)
        and (ws_list is None or w in ws_list)
    ]
    async with relay_lock:
        for t in targets:
            await send_to(t, payload)


async def relay_frame(sender, data):
    """Klatka z telefonu -> przegladarki, ktore ja ogladaja (tylko zalogowane)."""
    phone = clients[sender]
    auto = most_active_phone()
    auto_id = clients[auto]["id"] if auto else None
    dead = []
    async with relay_lock:
        for ws, c in list(clients.items()):
            if c["kind"] != "viewer" or not c.get("authed", True):
                continue
            sub = c.get("sub")
            if sub:
                if sub != phone["id"]:
                    continue
            else:
                if auto_id != phone["id"]:
                    continue
            try:
                if isinstance(data, (bytes, bytearray)):
                    await ws.send_bytes(data)
                else:
                    await ws.send_text(data)
            except (WebSocketDisconnect, RuntimeError, ConnectionError, OSError):
                dead.append(ws)
            except Exception as exc:
                print(f"[relay] unexpected ({type(exc).__name__}: {exc})", flush=True)
        for ws in dead:
            discard_client(ws)


async def route_command(sender, obj: dict):
    kind = obj.get("t")

    if kind == "view":
        clients[sender]["sub"] = obj.get("id") or None
        await push_phones()
        return

    if kind == "rename":
        pid = obj.get("id")
        name = str(obj.get("name") or "").strip()[:40]
        if pid in known:
            known[pid]["name"] = name or None
            save_known()
            await push_phones()
        return

    if kind == "phones":
        await push_phones()
        return

    phone = resolve_phone(clients[sender].get("sub"))
    if phone is None:
        return
    await send_to(phone, json.dumps(obj))


# ---------------------------------------------------------------------------
# Strona panelu (v5 - trzy zakladki)
# ---------------------------------------------------------------------------
html_content = """
<!DOCTYPE html>
<html lang="pl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Remote Hub - Panel</title>
    <style>
        * { box-sizing: border-box; }
        body {
            background-color: #0d1117; color: #c9d1d9; margin: 0; padding: 14px;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            display: flex; flex-direction: column; align-items: center; min-height: 100vh;
        }
        h1 { color: #58a6ff; margin: 6px 0 4px; font-size: 21px; }
        .badges { display: flex; gap: 6px; flex-wrap: wrap; justify-content: center; margin-bottom: 12px; }
        .badge { background: #161b22; border: 1px solid #30363d; border-radius: 999px; padding: 4px 11px; font-size: 12px; color: #8b949e; }
        .badge b { color: #c9d1d9; font-weight: 600; }
        .dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; background: #484f58; margin-right: 6px; vertical-align: 0px; }
        .dot.on { background: #238636; box-shadow: 0 0 6px #238636; }
        .dot.off { background: #da3633; }

        /* Zakladki */
        .tabs { display: flex; gap: 4px; margin-bottom: 12px; flex-wrap: wrap; justify-content: center; }
        .tabbtn {
            background: #161b22; color: #8b949e; border: 1px solid #30363d;
            border-radius: 8px 8px 0 0; padding: 9px 20px; font-size: 14px; cursor: pointer;
        }
        .tabbtn:hover { color: #c9d1d9; }
        .tabbtn.active { background: #1f6feb; border-color: #1f6feb; color: #fff; font-weight: 600; }
        .tab { display: none; width: min(860px, 97vw); }
        .tab.active { display: block; }

        .panel {
            background: #161b22; border: 1px solid #30363d; border-radius: 10px;
            padding: 12px; margin-bottom: 12px; box-shadow: 0 4px 12px rgba(0,0,0,0.5);
        }
        .panel h2 { margin: 0 0 10px; font-size: 15px; color: #58a6ff; }
        .row { display: flex; gap: 8px; align-items: center; }
        .row label { font-size: 12px; color: #8b949e; min-width: 74px; }
        select, input[type=password], input[type=text] {
            background: #0d1117; color: #c9d1d9; border: 1px solid #30363d;
            border-radius: 6px; padding: 7px 8px; font-size: 13px; outline: none; flex: 1;
        }
        select:focus, input:focus { border-color: #58a6ff; }

        .screen-container {
            width: 100%; background: #000; border-radius: 8px; overflow: hidden;
            display: flex; align-items: center; justify-content: center;
            min-height: 360px; max-height: 70vh; position: relative;
        }
        #screenView { width: 100%; height: 100%; object-fit: contain; display: none; min-height: 360px; cursor: crosshair; -webkit-user-drag: none; user-select: none; }
        #placeholder { position: absolute; inset: 0; display: flex; align-items: center; justify-content: center; color: #484f58; font-size: 14px; text-align: center; padding: 20px; pointer-events: none; }
        .controls { display: grid; grid-template-columns: repeat(4, 1fr); gap: 6px; margin-top: 10px; }
        button {
            background: #21262d; color: #c9d1d9; border: 1px solid #30363d; border-radius: 6px;
            padding: 8px 4px; font-size: 12px; cursor: pointer; transition: background .15s;
        }
        button:hover { background: #30363d; border-color: #8b949e; }
        button:active { transform: translateY(1px); }
        button.active { background: #1f6feb; border-color: #1f6feb; color: #fff; }

        table { width: 100%; border-collapse: collapse; font-size: 13px; }
        th { text-align: left; color: #8b949e; font-weight: 500; padding: 6px 8px; border-bottom: 1px solid #30363d; font-size: 12px; }
        td { padding: 7px 8px; border-bottom: 1px solid #21262d; }
        tr:hover td { background: #1c2129; }
        .st { font-size: 12px; }
        .st.on { color: #3fb950; } .st.off { color: #8b949e; }
        .mini { padding: 4px 8px; font-size: 11px; }

        .log { max-height: 220px; overflow-y: auto; font-size: 12px; }
        .log div { padding: 3px 0; border-bottom: 1px solid #21262d; color: #8b949e; }
        .log b { color: #c9d1d9; font-weight: 500; }

        #spark { background: #0d1117; border: 1px solid #30363d; border-radius: 6px; margin-top: 8px; }
        .hint { color: #6e7681; font-size: 11.5px; margin-top: 6px; text-align: center; line-height: 1.55; max-width: 860px; }
        .toast {
            position: fixed; bottom: 18px; left: 50%; transform: translateX(-50%);
            background: #1f6feb; color: #fff; padding: 8px 16px; border-radius: 999px;
            font-size: 13px; opacity: 0; transition: opacity .25s; pointer-events: none; z-index: 30;
        }
        .toast.show { opacity: 1; }

        /* Okno PIN */
        #pinModal { position: fixed; inset: 0; background: rgba(1,4,9,.85); display: none; align-items: center; justify-content: center; z-index: 40; }
        #pinModal.show { display: flex; }
        #pinModal .box { background: #161b22; border: 1px solid #30363d; border-radius: 12px; padding: 24px; width: min(340px, 92vw); text-align: center; }
        #pinModal h3 { margin: 0 0 8px; color: #58a6ff; }
        #pinModal p { font-size: 13px; color: #8b949e; margin: 0 0 14px; }
        #pinErr { color: #f85149; font-size: 12px; min-height: 16px; margin-top: 8px; }
    </style>
</head>
<body>

    <h1>Remote Hub - Panel</h1>

    <div class="badges">
        <span class="badge"><span id="srvDot" class="dot"></span>Serwer: <b id="srvState">laczenie...</b></span>
        <span class="badge"><span id="phDot" class="dot"></span>Telefon: <b id="phState">brak</b></span>
        <span class="badge">FPS: <b id="fps">0</b></span>
        <span class="badge">Transfer: <b id="kbps">0</b> KB/s</span>
        <span class="badge">Rozdzielczosc: <b id="resolution">-</b></span>
        <span class="badge">Kadry: <b id="frames">0</b></span>
        <span class="badge" id="pinBadge" style="display:none;">&#128274; PIN</span>
    </div>

    <div class="tabs">
        <button class="tabbtn active" data-tab="tabPodglad">&#128249; Podglad</button>
        <button class="tabbtn" data-tab="tabTelefony">&#128241; Telefony</button>
        <button class="tabbtn" data-tab="tabUstawienia">&#9881; Ustawienia</button>
    </div>

    <!-- ================================================================ -->
    <div class="tab active" id="tabPodglad">
        <div class="panel">
            <div class="screen-container" id="screenBox">
                <div id="placeholder">Oczekiwanie na polaczenie z telefonem...</div>
                <img id="screenView" alt="Brak obrazu z telefonu" />
            </div>
            <div class="controls">
                <button id="btnBack" title="Przycisk Wstecz (skrot: B)">&#8592; Wstecz</button>
                <button id="btnHome" title="Przycisk Home (skrot: H)">&#8962; Home</button>
                <button id="btnRecents" title="Lista otwartych aplikacji">&#9776; Appy</button>
                <button id="btnLock" title="Zablokuj ekran (skrot: L)">&#128274; Blokada</button>
                <button id="btnPower" title="Menu zasilania">&#9211; Zasilanie</button>
                <button id="btnScreenshotPhone" title="Zrzut ekranu wykonywany NA telefonie">&#128190; Zrzut na tel.</button>
                <button id="btnPause" title="Wstrzymaj wyswietlanie obrazu (skrot: spacja)">&#10074;&#10074; Pauza</button>
                <button id="btnSnap" title="Zapisz aktualny obraz jako JPG">&#128247; Pobierz obraz</button>
                <button id="btnFs" title="Pelny ekran (skrot: F)">&#9974; Pelny ekran</button>
            </div>
            <div class="row" style="margin-top:10px;">
                <label>Jakosc:</label>
                <select id="qualitySel" title="Jakosc i czestotliwosc wysylania klatek">
                    <option value="low">Niska - 5 FPS / mala rozdzielczosc</option>
                    <option value="medium" selected>Srednia - 10 FPS / 960 px</option>
                    <option value="high">Wysoka - 15 FPS / 1280 px</option>
                    <option value="ultra">Bardzo wysoka - 20 FPS / pelna</option>
                </select>
            </div>
            <canvas id="spark" width="820" height="54"></canvas>
            <div class="hint" style="margin-top:4px;">
                <b>Klik</b> = dotkniecie. <b>Przeciagniecie</b> = swipe. <b>Przytrzymaj</b> = przytrzymanie palca.
                Skroty: <b>B</b> wstecz, <b>H</b> home, <b>L</b> blokada, <b>F</b> pelny ekran, <b>spacja</b> pauza.
            </div>
        </div>
    </div>

    <!-- ================================================================ -->
    <div class="tab" id="tabTelefony">
        <div class="panel">
            <h2>Wybierz telefon do podgladu</h2>
            <div class="row" style="margin-bottom:8px;">
                <label>Telefon:</label>
                <select id="phoneSel" title="Wybierz urzadzenie do podgladu">
                    <option value="auto">Automatycznie (najaktywniejszy)</option>
                </select>
            </div>
            <div class="row">
                <label>Nazwa:</label>
                <button id="btnRename" title="Nadaj wlasna nazwe wybranemu telefonowi">Zmien nazwe wybranego</button>
            </div>
        </div>
        <div class="panel">
            <h2>Wszystkie telefony z zainstalowana apka</h2>
            <table>
                <thead>
                    <tr><th>Model</th><th>Nazwa</th><th>Status</th><th>Ostatnio widziany</th><th></th></tr>
                </thead>
                <tbody id="phoneTable">
                    <tr><td colspan="5" style="color:#484f58;">Ladowanie...</td></tr>
                </tbody>
            </table>
        </div>
        <div class="panel">
            <h2>Co tu dziala?</h2>
            <div style="font-size:13px; color:#8b949e; line-height:1.6;">
                Kazdy telefon z zainstalowana apka <b>Remote Control</b> pojawia sie na tej liscie -
                takze wylaczony (z czasem ostatniego widzenia). Nazwy (np. "Telefon taty")
                widza wszystkie przegladarki laczące sie do panelu.<br>
                <b>Obserwuj</b> - podglada wybrany telefon. <b>Automatycznie</b> - zawsze najaktywniejszy.
            </div>
        </div>
    </div>

    <!-- ================================================================ -->
    <div class="tab" id="tabUstawienia">
        <div class="panel">
            <h2>Kod dostepu do panelu (PIN)</h2>
            <div style="font-size:12.5px; color:#8b949e; margin-bottom:10px; line-height:1.55;">
                Chroni podglad kodem - bez niego nikt nie zobaczy ekranu ani nie polaczy sie z telefonem.
                Dziala tylko gdy na serwerze (Render) ustawiono zmienna <b>ACCESS_PIN</b>.
                Aplikacja na telefonie tez wysyla ten kod (pole PIN w ustawieniach apki).
            </div>
            <div class="row">
                <label>PIN:</label>
                <input type="password" id="pinSetting" placeholder="kod dostepu do panelu" maxlength="32">
                <button id="btnPinSave" style="flex:0 0 auto;">Zapisz i wyslij</button>
            </div>
            <div id="pinStatus" style="font-size:12px; color:#8b949e; margin-top:8px;">PIN nieustawiony / brak zabezpieczenia.</div>
        </div>

        <div class="panel">
            <h2>Sesja</h2>
            <div style="font-size:13px; color:#8b949e; line-height:1.7;">
                Czas podlaczenia: <b id="sessTime" style="color:#c9d1d9;">0 s</b><br>
                odebranych klatek: <b id="sessFrames" style="color:#c9d1d9;">0</b> &nbsp;|&nbsp;
                transfer: <b id="sessMB" style="color:#c9d1d9;">0.0</b> MB &nbsp;|&nbsp;
                srednie FPS: <b id="sessAvg" style="color:#c9d1d9;">0</b>
            </div>
        </div>

        <div class="panel">
            <h2>Log zdarzen</h2>
            <div class="log" id="eventLog"><div style="color:#484f58;">Brak zdarzen.</div></div>
        </div>

        <div class="panel">
            <h2>Informacje</h2>
            <div style="font-size:12.5px; color:#8b949e; line-height:1.7;">
                Serwer: <b id="srvUrl" style="color:#c9d1d9;"></b><br>
                Panel: Remote Hub v5 &nbsp;|&nbsp; apka: Remote Control v1.4+<br>
                Sterowanie: tap / swipe / przytrzymanie dzialaja po wlaczeniu uslugi
                <b>Remote Control</b> w telefonie (Ustawienia &rarr; Dostepnosc).
            </div>
        </div>
    </div>

    <div id="toast" class="toast"></div>

    <!-- Okno PIN -->
    <div id="pinModal">
        <div class="box">
            <h3>&#128274; Panel zabezpieczony</h3>
            <p>Podaj kod dostepu, aby zobaczyc podglad ekranu telefonu.</p>
            <div class="row">
                <input type="password" id="pinInput" placeholder="PIN" maxlength="32">
                <button id="pinSend" style="flex:0 0 auto;">OK</button>
            </div>
            <div id="pinErr"></div>
        </div>
    </div>

    <script>
        var screenView = document.getElementById("screenView");
        var placeholder = document.getElementById("placeholder");
        var srvState = document.getElementById("srvState"), srvDot = document.getElementById("srvDot");
        var phState = document.getElementById("phState"), phDot = document.getElementById("phDot");
        var fpsEl = document.getElementById("fps"), framesEl = document.getElementById("frames");
        var resEl = document.getElementById("resolution"), kbpsEl = document.getElementById("kbps");
        var phoneSel = document.getElementById("phoneSel"), qualitySel = document.getElementById("qualitySel");
        var toastEl = document.getElementById("toast");
        document.getElementById("srvUrl").textContent = location.host;

        var ws = null, backoff = 1000, retryTimer = null;
        var frameCount = 0, totalFrames = 0, lastFrameAt = 0;
        var byteWindow = 0, kbps = 0, totalBytes = 0;
        var sessStart = Date.now(), sessFrames = 0;
        var lastObjectUrl = null, paused = false;
        var phones = {}, autoPhoneId = null, pinRequired = false, authed = true;
        var selectedPhone = localStorage.getItem("rh_phone") || "auto";
        var quality = localStorage.getItem("rh_quality") || "medium";
        var toastTimer = null;
        var histFps = [], histKbps = [];

        var QUALITIES = {
            low:    { fps: 5,  q: 25, max: 640  },
            medium: { fps: 10, q: 35, max: 960  },
            high:   { fps: 15, q: 45, max: 1280 },
            ultra:  { fps: 20, q: 55, max: 1920 }
        };

        qualitySel.value = QUALITIES[quality] ? quality : "medium";

        function setServer(state, on) { srvState.textContent = state; srvDot.className = "dot " + (on ? "on" : "off"); }
        function setPhone(state, on) { phState.textContent = state; phDot.className = "dot " + (on ? "on" : "off"); }
        function toast(msg) {
            toastEl.textContent = msg; toastEl.className = "toast show";
            clearTimeout(toastTimer); toastTimer = setTimeout(function(){ toastEl.className = "toast"; }, 1600);
        }
        function send(obj) {
            if (ws && ws.readyState === 1) { ws.send(JSON.stringify(obj)); return true; }
            return false;
        }

        // ------------------------------------------------------------------
        // Zakladki
        // ------------------------------------------------------------------
        document.querySelectorAll(".tabbtn").forEach(function (b) {
            b.addEventListener("click", function () {
                document.querySelectorAll(".tabbtn").forEach(function (x) { x.classList.remove("active"); });
                document.querySelectorAll(".tab").forEach(function (x) { x.classList.remove("active"); });
                b.classList.add("active");
                document.getElementById(b.dataset.tab).classList.add("active");
            });
        });
        function gotoTab(id) {
            var btn = document.querySelector('.tabbtn[data-tab="' + id + '"]');
            if (btn) btn.click();
        }

        // ------------------------------------------------------------------
        // Polaczenie
        // ------------------------------------------------------------------
        function connect() {
            setServer("laczenie...", false);
            var protocol = location.protocol === "https:" ? "wss:" : "ws:";
            try { ws = new WebSocket(protocol + "//" + location.host); }
            catch (e) { scheduleRetry(); return; }
            ws.binaryType = "blob";

            ws.onopen = function () {
                backoff = 1000;
                authed = !pinRequired;
                setServer("polaczony", true);
                if (!Object.keys(phones).length) setPhone("czekam...", false);
                // Gdy znamy PIN z poprzedniego razu - probuj od razu.
                var saved = localStorage.getItem("rh_pin");
                if (pinRequired && saved) send({ t: "auth", pin: saved });
                sendView();
                sendQuality();
            };
            ws.onmessage = function (event) {
                var data = event.data;
                if (typeof data === "string") {
                    if (data.charAt(0) === "{") { handleStatus(data); return; }
                    showFrame("data:image/jpeg;base64," + data, data.length * 0.75);
                } else {
                    showFrame(URL.createObjectURL(data), data.size || 0);
                }
            };
            ws.onclose = function () { setServer("rozlaczony - ponawiam...", false); scheduleRetry(); };
            ws.onerror = function () { setServer("blad polaczenia", false); };
        }
        function scheduleRetry() {
            clearTimeout(retryTimer);
            retryTimer = setTimeout(connect, backoff);
            backoff = Math.min(Math.round(backoff * 1.5), 15000);
        }

        // ------------------------------------------------------------------
        // Wiadomosci statusowe z serwera
        // ------------------------------------------------------------------
        function handleStatus(text) {
            var msg; try { msg = JSON.parse(text); } catch (e) { return; }
            if (msg.t === "phones") {
                phones = {};
                (msg.list || []).forEach(function (p) { phones[p.id] = p; });
                autoPhoneId = msg.auto || null;
                pinRequired = !!msg.pin;
                document.getElementById("pinBadge").style.display = pinRequired ? "" : "none";
                renderPhones();
                renderEvents(msg.events || []);
                updatePinStatus();
            } else if (msg.t === "auth_req") {
                pinRequired = true; authed = false;
                document.getElementById("pinBadge").style.display = "";
                updatePinStatus();
                showPinModal(false);
            } else if (msg.t === "auth") {
                if (msg.ok) {
                    authed = true;
                    hidePinModal();
                    toast("Zalogowano do panelu");
                    sendView();
                } else {
                    authed = false;
                    localStorage.removeItem("rh_pin");
                    showPinModal(true);
                }
            } else if (msg.t === "hello") {
                send({ t: "phones" });
            } else if (msg.t === "error") {
                toast("Serwer: " + (msg.m || "blad"));
            }
        }

        // ------------------------------------------------------------------
        // PIN
        // ------------------------------------------------------------------
        var pinModal = document.getElementById("pinModal");
        function showPinModal(wrong) {
            pinModal.classList.add("show");
            document.getElementById("pinErr").textContent =
                wrong ? "Bledny kod - sprobuj jeszcze raz." : "";
            var inp = document.getElementById("pinInput");
            if (!wrong) { inp.value = ""; inp.focus(); }
        }
        function hidePinModal() { pinModal.classList.remove("show"); }
        function tryPin() {
            var v = document.getElementById("pinInput").value;
            if (!v) return;
            localStorage.setItem("rh_pin", v);
            send({ t: "auth", pin: v });
        }
        document.getElementById("pinSend").addEventListener("click", tryPin);
        document.getElementById("pinInput").addEventListener("keydown", function (e) {
            if (e.key === "Enter") tryPin();
        });
        document.getElementById("btnPinSave").addEventListener("click", function () {
            var v = document.getElementById("pinSetting").value;
            if (!v) { toast("Wpisz kod"); return; }
            localStorage.setItem("rh_pin", v);
            send({ t: "auth", pin: v });
            toast("Kod zapisany - wyslany do serwera");
        });
        function updatePinStatus() {
            var el = document.getElementById("pinStatus");
            if (!pinRequired) el.textContent = "Serwer bez zabezpieczenia - podglad otwarty dla kazdego, kto zna adres.";
            else if (authed) el.textContent = "Zalogowano - podglad chroniony PINEM.";
            else el.textContent = "Serwer wymaga PIN - podaj go powyzej.";
        }

        // ------------------------------------------------------------------
        // Lista telefonow + tabela + log
        // ------------------------------------------------------------------
        function fmtSince(s) {
            if (s < 90) return Math.round(s) + " s temu";
            if (s < 5400) return Math.round(s / 60) + " min temu";
            if (s < 172800) return Math.round(s / 3600) + " godz. temu";
            return Math.round(s / 86400) + " dni temu";
        }

        function renderPhones() {
            var current = phoneSel.value || "auto";
            var html = '<option value="auto">Automatycznie (najaktywniejszy)</option>';
            var ids = Object.keys(phones);
            ids.forEach(function (id) {
                var p = phones[id];
                var label = (p.name || p.model) +
                    (p.online ? " - online" : " - offline, widziany " + fmtSince(p.since));
                html += '<option value="' + id + '">' + label + '</option>';
            });
            phoneSel.innerHTML = html;

            var valid = current === "auto" || phones[current];
            phoneSel.value = valid ? current : "auto";
            selectedPhone = phoneSel.value;

            if (ids.length === 0) {
                setPhone("brak", false);
            } else if (paused) {
                setPhone("pauza", true);
            } else {
                var shown = selectedPhone === "auto" ? autoPhoneId : selectedPhone;
                var p2 = phones[shown];
                setPhone(p2 ? (p2.online ? (p2.name || p2.model) : (p2.name || p2.model) + " (offline)") : "brak", !!(p2 && p2.online));
            }
            renderPhoneTable();
        }

        function renderPhoneTable() {
            var tb = document.getElementById("phoneTable");
            var ids = Object.keys(phones);
            if (!ids.length) {
                tb.innerHTML = '<tr><td colspan="5" style="color:#484f58;">' +
                    'Jeszcze zaden telefon nie polaczyl sie z tym serwerem.</td></tr>';
                return;
            }
            var rows = "";
            ids.forEach(function (id) {
                var p = phones[id];
                rows += "<tr>" +
                    "<td>" + p.model + "</td>" +
                    "<td>" + (p.name || "<span style='color:#484f58;'>-</span>") + "</td>" +
                    '<td><span class="st ' + (p.online ? "on" : "off") + '">' +
                    (p.online ? "&#9679; online" : "&#9675; offline") + "</span></td>" +
                    "<td>" + (p.online ? "nadaje teraz" : fmtSince(p.since)) + "</td>" +
                    '<td style="text-align:right; white-space:nowrap;">' +
                    '<button class="mini" data-watch="' + id + '">Obserwuj</button> ' +
                    '<button class="mini" data-rename="' + id + '">Nazwa</button>' +
                    "</td></tr>";
            });
            tb.innerHTML = rows;
            tb.querySelectorAll("[data-watch]").forEach(function (b) {
                b.addEventListener("click", function () {
                    selectedPhone = b.dataset.watch;
                    localStorage.setItem("rh_phone", selectedPhone);
                    phoneSel.value = selectedPhone;
                    sendView(); renderPhones();
                    toast("Obserwuje: " + (phones[selectedPhone] ? (phones[selectedPhone].name || phones[selectedPhone].model) : ""));
                    gotoTab("tabPodglad");
                });
            });
            tb.querySelectorAll("[data-rename]").forEach(function (b) {
                b.addEventListener("click", function () {
                    var p = phones[b.dataset.rename];
                    var name = prompt("Nazwa telefonu (pusta = pokazuj model):", p.name || p.model || "");
                    if (name === null) return;
                    send({ t: "rename", id: b.dataset.rename, name: name.trim() });
                    toast("Zapisano nazwe");
                });
            });
        }

        function renderEvents(evts) {
            var el = document.getElementById("eventLog");
            if (!evts.length) {
                el.innerHTML = "<div style='color:#484f58;'>Brak zdarzen.</div>";
                return;
            }
            var html = "";
            evts.slice().reverse().forEach(function (e) {
                var t = new Date(e.ts * 1000).toLocaleTimeString("pl-PL");
                html += "<div><b>" + t + "</b> - " + e.text + "</div>";
            });
            el.innerHTML = html;
        }

        phoneSel.addEventListener("change", function () {
            selectedPhone = phoneSel.value;
            localStorage.setItem("rh_phone", selectedPhone);
            sendView();
            renderPhones();
            toast(selectedPhone === "auto" ? "Tryb automatyczny" : "Wybrano: " + (phones[selectedPhone] ? (phones[selectedPhone].name || phones[selectedPhone].model) : selectedPhone));
        });
        function sendView() { send({ t: "view", id: selectedPhone === "auto" ? null : selectedPhone }); }

        document.getElementById("btnRename").addEventListener("click", function () {
            if (selectedPhone === "auto" || !phones[selectedPhone]) {
                toast("Najpierw wybierz telefon z listy");
                return;
            }
            var p = phones[selectedPhone];
            var name = prompt("Nazwa tego telefonu (pusta = pokazuj model):", p.name || p.model || "");
            if (name === null) return;
            send({ t: "rename", id: selectedPhone, name: name.trim() });
            toast("Zapisano nazwe");
        });

        // ------------------------------------------------------------------
        // Jakosc transmisji
        // ------------------------------------------------------------------
        function sendQuality() {
            var q = QUALITIES[qualitySel.value] || QUALITIES.medium;
            send({ t: "cfg", fps: q.fps, q: q.q, max: q.max });
        }
        qualitySel.addEventListener("change", function () {
            quality = qualitySel.value;
            localStorage.setItem("rh_quality", quality);
            sendQuality();
            toast("Ustawiono jakosc: " + qualitySel.options[qualitySel.selectedIndex].text);
        });

        // ------------------------------------------------------------------
        // Obraz + wykres + statystyki sesji
        // ------------------------------------------------------------------
        function showFrame(src, bytes) {
            frameCount++; totalFrames++; sessFrames++;
            lastFrameAt = Date.now();
            byteWindow += bytes || 0; totalBytes += bytes || 0;
            framesEl.textContent = totalFrames;
            if (paused) return;
            if (lastObjectUrl) { URL.revokeObjectURL(lastObjectUrl); lastObjectUrl = null; }
            if (src.indexOf("blob:") === 0) lastObjectUrl = src;
            screenView.src = src;
            screenView.style.display = "block";
            placeholder.style.display = "none";
            setPhone(textOfSelected(), true);
        }
        function textOfSelected() {
            var id = selectedPhone === "auto" ? autoPhoneId : selectedPhone;
            var p = phones[id];
            return p ? (p.name || p.model) : "na zywo";
        }

        var spark = document.getElementById("spark"), sctx = spark.getContext("2d");
        function drawSpark() {
            sctx.clearRect(0, 0, spark.width, spark.height);
            sctx.font = "10px sans-serif";
            function line(arr, color, maxV, label) {
                sctx.strokeStyle = color; sctx.beginPath();
                var n = arr.length;
                for (var i = 0; i < n; i++) {
                    var x = spark.width - (n - i) * (spark.width / 90);
                    var y = spark.height - 4 - Math.min(1, arr[i] / maxV) * (spark.height - 12);
                    if (i === 0) sctx.moveTo(x, y); else sctx.lineTo(x, y);
                }
                sctx.stroke();
                sctx.fillStyle = color;
                sctx.fillText(label + " " + (n ? arr[n - 1] : 0), 4, 10 + (color === "#58a6ff" ? 0 : 12));
            }
            line(histFps, "#58a6ff", 30, "FPS");
            line(histKbps, "#3fb950", 2048, "KB/s");
        }

        setInterval(function () {
            fpsEl.textContent = frameCount;
            histFps.push(frameCount); if (histFps.length > 90) histFps.shift();
            frameCount = 0;
            kbps = Math.round(byteWindow / 1024); byteWindow = 0;
            histKbps.push(kbps); if (histKbps.length > 90) histKbps.shift();
            kbpsEl.textContent = kbps;
            drawSpark();

            var secs = Math.round((Date.now() - sessStart) / 1000);
            document.getElementById("sessTime").textContent =
                secs < 120 ? secs + " s" : Math.round(secs / 60) + " min";
            document.getElementById("sessFrames").textContent = sessFrames;
            document.getElementById("sessMB").textContent = (totalBytes / 1048576).toFixed(1);
            document.getElementById("sessAvg").textContent = sessFrames && secs ? Math.round(sessFrames / secs) : 0;

            if (lastFrameAt && Date.now() - lastFrameAt > 3000 && !paused) {
                setPhone(Object.keys(phones).length ? "brak obrazu" : "brak", false);
                if (Object.keys(phones).length) placeholder.textContent = "Telefon nie wysyla obrazu...";
            }
        }, 1000);

        screenView.onload = function () {
            if (screenView.naturalWidth) resEl.textContent = screenView.naturalWidth + " x " + screenView.naturalHeight;
        };

        // ------------------------------------------------------------------
        // Sterowanie: klik = tap, przeciągnięcie = swipe, przytrzymanie = hold
        // ------------------------------------------------------------------
        var dragStart = null, dragging = false, holdTimer = null, holdDone = false;
        function norm(e) {
            var r = screenView.getBoundingClientRect();
            return {
                x: Math.max(0, Math.min(1, (e.clientX - r.left) / r.width)),
                y: Math.max(0, Math.min(1, (e.clientY - r.top) / r.height))
            };
        }
        screenView.addEventListener("mousedown", function (e) {
            dragStart = norm(e); dragging = false; holdDone = false;
            var p = dragStart;
            holdTimer = setTimeout(function () {
                if (dragStart && !dragging) {
                    send({ t: "longpress", x: r4(p.x), y: r4(p.y) });
                    holdDone = true;
                    toast("Przytrzymanie");
                }
            }, 550);
        });
        screenView.addEventListener("mousemove", function (e) {
            if (!dragStart) return;
            var p = norm(e);
            var dx = (p.x - dragStart.x) * screenView.getBoundingClientRect().width;
            var dy = (p.y - dragStart.y) * screenView.getBoundingClientRect().height;
            if (Math.sqrt(dx * dx + dy * dy) > 14) { dragging = true; clearTimeout(holdTimer); }
        });
        window.addEventListener("mouseup", function (e) {
            clearTimeout(holdTimer);
            if (!dragStart) return;
            var end = norm(e), start = dragStart;
            dragStart = null;
            if (holdDone) { holdDone = false; return; }
            if (dragging) {
                send({ t: "swipe", x1: r4(start.x), y1: r4(start.y), x2: r4(end.x), y2: r4(end.y) });
                toast("Swipe");
            } else {
                send({ t: "tap", x: r4(end.x), y: r4(end.y) });
            }
            dragging = false;
        });
        var touchStart = null, touchHold = null;
        screenView.addEventListener("touchstart", function (e) {
            touchStart = norm(e.touches[0]);
            var p = touchStart;
            touchHold = setTimeout(function () {
                if (touchStart) {
                    send({ t: "longpress", x: r4(p.x), y: r4(p.y) });
                    touchStart = null;
                    toast("Przytrzymanie");
                }
            }, 550);
        }, { passive: true });
        screenView.addEventListener("touchend", function (e) {
            clearTimeout(touchHold);
            if (!touchStart) return;
            var end = norm(e.changedTouches[0]);
            var dx = (end.x - touchStart.x) * screenView.getBoundingClientRect().width;
            var dy = (end.y - touchStart.y) * screenView.getBoundingClientRect().height;
            if (Math.sqrt(dx * dx + dy * dy) > 14) send({ t: "swipe", x1: r4(touchStart.x), y1: r4(touchStart.y), x2: r4(end.x), y2: r4(end.y) });
            else send({ t: "tap", x: r4(end.x), y: r4(end.y) });
            touchStart = null;
        }, { passive: true });
        function r4(v) { return Math.round(v * 10000) / 10000; }

        document.getElementById("btnBack").addEventListener("click", function () { send({ t: "back" }); toast("Wstecz"); });
        document.getElementById("btnHome").addEventListener("click", function () { send({ t: "home" }); toast("Home"); });
        document.getElementById("btnRecents").addEventListener("click", function () { send({ t: "recents" }); toast("Ostatnie aplikacje"); });
        document.getElementById("btnLock").addEventListener("click", function () { send({ t: "lock" }); toast("Blokada ekranu"); });
        document.getElementById("btnPower").addEventListener("click", function () { send({ t: "power" }); toast("Menu zasilania"); });
        document.getElementById("btnScreenshotPhone").addEventListener("click", function () { send({ t: "screenshot" }); toast("Zrzut na telefonie (galeria telefonu)"); });

        document.getElementById("btnPause").addEventListener("click", function () {
            paused = !paused;
            this.classList.toggle("active", paused);
            this.innerHTML = paused ? "&#9654; Wznow" : "&#10074;&#10074; Pauza";
            if (!paused) { lastFrameAt = Date.now(); renderPhones(); }
            else setPhone("pauza", true);
        });

        document.getElementById("btnSnap").addEventListener("click", function () {
            if (!screenView.src) { toast("Brak obrazu"); return; }
            var a = document.createElement("a");
            a.href = screenView.src;
            a.download = "remote-hub-" + Date.now() + ".jpg";
            a.click();
            toast("Zapisano obraz");
        });

        document.getElementById("btnFs").addEventListener("click", function () {
            var box = document.getElementById("screenBox");
            if (document.fullscreenElement) document.exitFullscreen();
            else if (box.requestFullscreen) box.requestFullscreen();
        });

        window.addEventListener("keydown", function (e) {
            if (e.target.tagName === "SELECT" || e.target.tagName === "INPUT") return;
            var k = e.key.toLowerCase();
            if (k === "b") send({ t: "back" });
            else if (k === "h") send({ t: "home" });
            else if (k === "l") send({ t: "lock" });
            else if (k === "f") document.getElementById("btnFs").click();
            else if (k === " ") { e.preventDefault(); document.getElementById("btnPause").click(); }
        });

        connect();
    </script>

</body>
</html>
"""


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    return HTMLResponse(content=html_content, status_code=200)


@app.get("/health")
async def health():
    phones_n = sum(1 for c in clients.values() if c["kind"] == "phone")
    return JSONResponse(
        {
            "ok": True,
            "clients": len(clients),
            "phones": phones_n,
            "viewers": len(clients) - phones_n,
            "auth": "on" if ACCESS_PIN else "off",
            "known": len(known),
        }
    )


@app.get("/phones")
async def phones_endpoint():
    return JSONResponse({"phones": phone_list()})


@app.get("/api/state")
async def api_state():
    return JSONResponse(
        {
            "phones": phone_list(),
            "known": known,
            "events": events[-20:],
            "auth": bool(ACCESS_PIN),
            "clients": len(clients),
        }
    )


# ---------------------------------------------------------------------------
# WebSocket
# ---------------------------------------------------------------------------
@app.websocket("/")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    is_viewer = not ACCESS_PIN   # domyslnie przegladarka; telefon pozniej sie "ujawni"
    clients[websocket] = {
        "kind": "viewer",
        "id": None,
        "model": None,
        "sub": None,
        "authed": not ACCESS_PIN,
        "tries": 0,
        "frames": 0,
        "last_frame": 0.0,
        "connected": now(),
    }
    if ACCESS_PIN:
        await send_to(websocket, json.dumps({"t": "auth_req"}))
    if DEBUG:
        print(f"[ws] open, clients={len(clients)}", flush=True)
    try:
        while True:
            message = await websocket.receive()
            if message.get("type") == "websocket.disconnect":
                break

            data = message.get("text")
            is_binary = False
            if data is None:
                data = message.get("bytes")
                is_binary = data is not None
            if data is None:
                continue

            c = clients.get(websocket)
            if c is None:
                continue

            if is_binary:
                if c["kind"] != "phone":
                    c["kind"] = "phone"
                    c["id"] = c["id"] or f"phone-{int(c['connected'])}"
                    c["model"] = c["model"] or "nieznany"
                    log_event(f"Telefon {c['model']} zaczal nadawac")
                    await push_phones()
                c["frames"] += 1
                await relay_frame(websocket, data)  # routing przed timestampem
                c["last_frame"] = now()
                k = known.get(c["id"])
                if k is not None:
                    k["last_seen"] = c["last_frame"]
                continue

            if data.startswith("{"):
                try:
                    obj = json.loads(data)
                except Exception:
                    continue
                t = obj.get("t")

                # ---- logowanie przegladarki (PIN) ----
                if t == "auth":
                    if not ACCESS_PIN:
                        await send_to(websocket, json.dumps({"t": "auth", "ok": True}))
                        continue
                    c["tries"] += 1
                    if hmac.compare_digest(str(obj.get("pin") or ""), ACCESS_PIN):
                        c["authed"] = True
                        log_event("Podglad: zalogowano do panelu")
                        await send_to(websocket, json.dumps({"t": "auth", "ok": True}))
                        await push_phones([websocket])
                    else:
                        await send_to(websocket, json.dumps({"t": "auth", "ok": False}))
                        log_event("Podglad: proba logowania z blednym kodem")
                        if c["tries"] >= 4:
                            log_event("Podglad: zbyt duze proby - rozlaczenie")
                            await websocket.close()
                            break
                    continue

                # ---- hello telefonu ----
                if t == "hello":
                    if ACCESS_PIN and not hmac.compare_digest(
                        str(obj.get("pin") or ""), ACCESS_PIN
                    ):
                        log_event("Telefon: odrzucony (bledny kod dostepu)")
                        await send_to(
                            websocket,
                            json.dumps({"t": "error", "m": "bad_pin"}),
                        )
                        await websocket.close()
                        break
                    dev_id = obj.get("id") or f"phone-{int(c['connected'])}"
                    c["kind"] = "phone"
                    c["id"] = dev_id
                    c["model"] = obj.get("device") or "nieznany"
                    remember_phone(dev_id, c["model"])
                    log_event(f"Telefon polaczyl: {c['model']}")
                    if DEBUG:
                        print(f"[ws] phone {c['model']} ({c['id']}) connected", flush=True)
                    await push_phones()
                    continue

                # ---- przegladarka bez PINu nie steruje niczym ----
                if c["kind"] == "viewer" and not c.get("authed", True):
                    continue

                if c["kind"] == "viewer":
                    await route_command(websocket, obj)
                elif t == "stat":
                    for ws2, c2 in list(clients.items()):
                        if c2["kind"] == "viewer" and c2.get("authed", True):
                            await send_to(ws2, data)
                continue

            # Klatka base64 (apki starsze wysylaja tekst).
            if len(data) > 256:
                if c["kind"] != "phone":
                    c["kind"] = "phone"
                    c["id"] = c["id"] or f"phone-{int(c['connected'])}"
                    c["model"] = c["model"] or "nieznany"
                    log_event(f"Telefon {c['model']} zaczal nadawac (base64)")
                    await push_phones()
                c["frames"] += 1
                await relay_frame(websocket, data)  # routing przed timestampem
                c["last_frame"] = now()
                k = known.get(c["id"])
                if k is not None:
                    k["last_seen"] = c["last_frame"]
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        import traceback

        print(f"[ws] error: {type(exc).__name__}: {exc}", flush=True)
        traceback.print_exc()
    finally:
        c = clients.get(websocket)
        was_phone = c is not None and c.get("kind") == "phone"
        phone_model = c.get("model") if was_phone else None
        discard_client(websocket)
        if was_phone:
            log_event(f"Telefon rozlaczyl: {phone_model}")
            await push_phones()
        if DEBUG:
            print(f"[ws] closed, clients={len(clients)}", flush=True)


async def phones_refresher():
    while True:
        await asyncio.sleep(PHONES_PUSH_INTERVAL)
        # Zamykanie przegladarek, ktore nie podaly PINu na czas.
        for ws, c in list(clients.items()):
            if (
                c["kind"] == "viewer"
                and not c.get("authed", True)
                and now() - c["connected"] > AUTH_TIMEOUT
            ):
                log_event("Podglad: rozlaczony (brak kodu dostepu)")
                await send_to(ws, json.dumps({"t": "error", "m": "auth_timeout"}))
                discard_client(ws)
                try:
                    await ws.close()
                except Exception:
                    pass
        if any(c["kind"] == "phone" for c in clients.values()) or clients:
            await push_phones()


@app.on_event("startup")
async def startup():
    load_known()
    asyncio.create_task(phones_refresher())


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
