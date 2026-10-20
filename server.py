"""
Remote Hub - serwer podgladu ekranu telefonu na zywo (v3).

Sciezki:
  GET  /        -> strona panelu podgladu (HTML + JS)
  WS   /        -> relay: ramki z telefonu -> przegladarki, komendy -> telefon
  GET  /health  -> status serwera (diagnostyka)
  GET  /phones  -> lista podlaczonych telefonow (JSON)

Uruchomienie lokalne:   python server.py
Procfile (Render):      uvicorn server:app --host 0.0.0.0 --port $PORT
"""

import asyncio
import json
import os
import time

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse

app = FastAPI(title="Remote Hub")

DEBUG = os.environ.get("DEBUG", "") == "1"

# ---------------------------------------------------------------------------
# Stan polaczen
# ---------------------------------------------------------------------------
# clients[ws] = {
#   "kind":  "phone" | "viewer",
#   "id":    "stable id urzadzenia",
#   "model": "np. SM-G991B",
#   "sub":   id ogladaneGO telefonu albo None (auto = najaktywniejszy),
#   "frames": ile klatek wyslal w tym polaczeniu,
#   "last_frame": timestamp ostatniej klatki,
#   "connected": timestamp polaczenia,
# }
clients: dict = {}
relay_lock = asyncio.Lock()

PHONE_IDLE_ONLINE = 8.0      # po ilu sekundach bez klatki telefon = offline
PHONES_PUSH_INTERVAL = 5.0   # co ile sekund odswiezamy liste telefonu w przegladarkach


def now() -> float:
    return time.time()


def phone_list():
    """Lista telefonow dla przegladarek."""
    out = []
    for ws, c in list(clients.items()):
        if c["kind"] != "phone":
            continue
        # Telefon bez klatek (dopiero co sie polaczyl) traktujemy jak online
        # na podstawie czasu polaczenia - nie epoch.
        idle = now() - (c["last_frame"] or c["connected"])
        out.append(
            {
                "id": c["id"],
                "model": c["model"],
                "frames": c["frames"],
                "online": idle <= PHONE_IDLE_ONLINE,
                "since": round(idle, 1),
            }
        )
    out.sort(key=lambda p: (not p["online"], p["model"]))
    return out


def all_phones():
    return [ws for ws, c in clients.items() if c["kind"] == "phone"]


def most_active_phone():
    phones = all_phones()
    if not phones:
        return None
    return max(phones, key=lambda ws: clients[ws]["last_frame"])


def resolve_phone(sub_id):
    """Telefon do ktorego maja trafic komendy (wybrany albo najaktywniejszy)."""
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


async def push_phones(ws_list=None):
    """Wysyla liste telefonow do przegladarek."""
    payload = json.dumps(
        {
            "t": "phones",
            "list": phone_list(),
            "auto": (clients[m]["id"] if (m := most_active_phone()) else None),
        }
    )
    targets = [
        w
        for w, c in list(clients.items())
        if c["kind"] == "viewer" and (ws_list is None or w in ws_list)
    ]
    async with relay_lock:
        for t in targets:
            await send_to(t, payload)


async def relay_frame(sender, data):
    """Klatka z telefonu -> wszystkie przegladarki, ktore ja ogladaja."""
    phone = clients[sender]
    auto = most_active_phone()
    auto_id = clients[auto]["id"] if auto else None
    dead = []
    async with relay_lock:
        for ws, c in list(clients.items()):
            if c["kind"] != "viewer":
                continue
            sub = c.get("sub")
            if sub:
                if sub != phone["id"]:
                    continue
            else:
                # Tryb "auto": ogladasz najaktywniejszy telefon.
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
    if DEBUG:
        print(f"[relay] frame from {phone['id']} -> viewers ok", flush=True)


async def route_command(sender, obj: dict):
    """Komenda z przegladarki -> odpowiedni telefon."""
    kind = obj.get("t")

    if kind == "view":
        clients[sender]["sub"] = obj.get("id") or None
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
# Strona podgladu
# ---------------------------------------------------------------------------
html_content = """
<!DOCTYPE html>
<html lang="pl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Remote Hub - Podglad na zywo</title>
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
        .panel {
            background: #161b22; border: 1px solid #30363d; border-radius: 10px;
            padding: 12px; width: min(430px, 97vw); box-shadow: 0 4px 12px rgba(0,0,0,0.5);
            margin-bottom: 12px;
        }
        .row { display: flex; gap: 8px; align-items: center; }
        .row label { font-size: 12px; color: #8b949e; min-width: 74px; }
        select {
            flex: 1; background: #0d1117; color: #c9d1d9; border: 1px solid #30363d;
            border-radius: 6px; padding: 7px 8px; font-size: 13px; outline: none;
        }
        select:focus { border-color: #58a6ff; }
        .screen-container {
            width: 100%; background: #000; border-radius: 8px; overflow: hidden;
            display: flex; align-items: center; justify-content: center;
            min-height: 300px; max-height: 74vh; position: relative;
        }
        #screenView { width: 100%; height: 100%; object-fit: contain; display: none; min-height: 300px; cursor: crosshair; -webkit-user-drag: none; user-select: none; }
        #placeholder { position: absolute; inset: 0; display: flex; align-items: center; justify-content: center; color: #484f58; font-size: 14px; text-align: center; padding: 20px; pointer-events: none; }
        .controls { display: grid; grid-template-columns: repeat(4, 1fr); gap: 6px; margin-top: 10px; }
        button {
            background: #21262d; color: #c9d1d9; border: 1px solid #30363d; border-radius: 6px;
            padding: 8px 4px; font-size: 12px; cursor: pointer; transition: background .15s;
        }
        button:hover { background: #30363d; border-color: #8b949e; }
        button:active { transform: translateY(1px); }
        button.active { background: #1f6feb; border-color: #1f6feb; color: #fff; }
        .hint { color: #6e7681; font-size: 11.5px; margin-top: 10px; text-align: center; line-height: 1.55; max-width: 430px; }
        .toast {
            position: fixed; bottom: 18px; left: 50%; transform: translateX(-50%);
            background: #1f6feb; color: #fff; padding: 8px 16px; border-radius: 999px;
            font-size: 13px; opacity: 0; transition: opacity .25s; pointer-events: none; z-index: 10;
        }
        .toast.show { opacity: 1; }
    </style>
</head>
<body>

    <h1>Remote Hub - Podglad na zywo</h1>

    <div class="badges">
        <span class="badge"><span id="srvDot" class="dot"></span>Serwer: <b id="srvState">laczenie...</b></span>
        <span class="badge"><span id="phDot" class="dot"></span>Telefon: <b id="phState">brak</b></span>
        <span class="badge">FPS: <b id="fps">0</b></span>
        <span class="badge">Transfer: <b id="kbps">0</b> KB/s</span>
        <span class="badge">Rozdzielczosc: <b id="resolution">-</b></span>
        <span class="badge">Kadry: <b id="frames">0</b></span>
    </div>

    <div class="panel">
        <div class="row" style="margin-bottom:8px;">
            <label>Telefon:</label>
            <select id="phoneSel" title="Wybierz urzadzenie do podgladu">
                <option value="auto">Automatycznie (najaktywniejszy)</option>
            </select>
        </div>
        <div class="row" style="margin-bottom:10px;">
            <label>Jakosc:</label>
            <select id="qualitySel" title="Jakosc i czestotliwosc wysylania klatek">
                <option value="low">Niska - 5 FPS / mala rozdzielczosc</option>
                <option value="medium" selected>Srednia - 10 FPS / 960 px</option>
                <option value="high">Wysoka - 15 FPS / 1280 px</option>
                <option value="ultra">Bardzo wysoka - 20 FPS / pelna</option>
            </select>
        </div>

        <div class="screen-container" id="screenBox">
            <div id="placeholder">Oczekiwanie na polaczenie z telefonem...</div>
            <img id="screenView" alt="Brak obrazu z telefonu" />
        </div>

        <div class="controls">
            <button id="btnBack" title="Przycisk Wstecz">&#8592; Wstecz</button>
            <button id="btnHome" title="Przycisk Home">&#8962; Home</button>
            <button id="btnRecents" title="Lista otwartych aplikacji">&#9776; Appy</button>
            <button id="btnLock" title="Zablokuj ekran">&#128274; Blokada</button>
            <button id="btnPower" title="Menu zasilania">&#9211; Zasilanie</button>
            <button id="btnPause" title="Wstrzymaj wyswietlanie obrazu">&#10074;&#10074; Pauza</button>
            <button id="btnSnap" title="Zapisz aktualny obraz jako JPG">&#128247; Zrzut</button>
            <button id="btnFs" title="Pelny ekran">&#9974; Pelny ekran</button>
        </div>
    </div>

    <div class="hint">
        <b>Klik</b> w obraz = dotkniecie ekranu. <b>Przeciagniecie</b> = swipe na telefonie.<br>
        Aby dzialalo sterowanie, wlacz usluge <b>Remote Control</b> w Ustawieniach &rarr; Dostepnosc telefonu.<br>
        Pamietaj: ekran telefonu musi byc wlaczony, a optymalizacja baterii wylaczona dla apki.
    </div>

    <div id="toast" class="toast"></div>

    <script>
        var screenView = document.getElementById("screenView");
        var placeholder = document.getElementById("placeholder");
        var srvState = document.getElementById("srvState"), srvDot = document.getElementById("srvDot");
        var phState = document.getElementById("phState"), phDot = document.getElementById("phDot");
        var fpsEl = document.getElementById("fps"), framesEl = document.getElementById("frames");
        var resEl = document.getElementById("resolution"), kbpsEl = document.getElementById("kbps");
        var phoneSel = document.getElementById("phoneSel"), qualitySel = document.getElementById("qualitySel");
        var toastEl = document.getElementById("toast");

        var ws = null, backoff = 1000, retryTimer = null;
        var frameCount = 0, totalFrames = 0, lastFrameAt = 0;
        var byteWindow = 0, kbps = 0;
        var lastObjectUrl = null, paused = false;
        var phones = {}, autoPhoneId = null;
        var selectedPhone = localStorage.getItem("rh_phone") || "auto";
        var quality = localStorage.getItem("rh_quality") || "medium";
        var toastTimer = null;

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
                setServer("polaczony", true);
                if (!Object.keys(phones).length) setPhone("czekam...", false);
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
        // Lista telefonow + komendy statusowe z serwera
        // ------------------------------------------------------------------
        function handleStatus(text) {
            var msg; try { msg = JSON.parse(text); } catch (e) { return; }
            if (msg.t === "phones") {
                phones = {};
                (msg.list || []).forEach(function (p) { phones[p.id] = p; });
                autoPhoneId = msg.auto || null;
                renderPhones();
            } else if (msg.t === "hello") {
                // Nowy telefon - popros liste (serwer i tak ja odswieza co kilka sekund).
                send({ t: "phones" });
            }
        }

        function renderPhones() {
            var current = phoneSel.value || "auto";
            var html = '<option value="auto">Automatycznie (najaktywniejszy)</option>';
            var ids = Object.keys(phones);
            ids.forEach(function (id) {
                var p = phones[id];
                var label = p.model + (p.online ? " - online" : " - offline (" + p.since + "s)");
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
                setPhone(p2 ? (p2.online ? p2.model : p2.model + " (offline)") : "brak", !!(p2 && p2.online));
            }
        }

        phoneSel.addEventListener("change", function () {
            selectedPhone = phoneSel.value;
            localStorage.setItem("rh_phone", selectedPhone);
            sendView();
            renderPhones();
            toast(selectedPhone === "auto" ? "Tryb automatyczny" : "Wybrano: " + (phones[selectedPhone] ? phones[selectedPhone].model : selectedPhone));
        });
        function sendView() { send({ t: "view", id: selectedPhone === "auto" ? null : selectedPhone }); }

        // ------------------------------------------------------------------
        // Jakosc transmisji (zdalnie sterowana przez apke)
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
        // Obraz
        // ------------------------------------------------------------------
        function showFrame(src, bytes) {
            frameCount++; totalFrames++;
            lastFrameAt = Date.now();
            byteWindow += bytes || 0;
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
            return p ? p.model : "na zywo";
        }

        setInterval(function () {
            fpsEl.textContent = frameCount; frameCount = 0;
            kbps = Math.round(byteWindow / 1024); byteWindow = 0;
            kbpsEl.textContent = kbps;
            if (lastFrameAt && Date.now() - lastFrameAt > 3000 && !paused) {
                setPhone(Object.keys(phones).length ? "brak obrazu" : "brak", false);
                if (Object.keys(phones).length) placeholder.textContent = "Telefon nie wysyla obrazu...";
            }
        }, 1000);

        screenView.onload = function () {
            if (screenView.naturalWidth) resEl.textContent = screenView.naturalWidth + " x " + screenView.naturalHeight;
        };

        // ------------------------------------------------------------------
        // Sterowanie: klik = tap, przeciągnięcie = swipe
        // ------------------------------------------------------------------
        var dragStart = null, dragging = false;
        function norm(e) {
            var r = screenView.getBoundingClientRect();
            return {
                x: Math.max(0, Math.min(1, (e.clientX - r.left) / r.width)),
                y: Math.max(0, Math.min(1, (e.clientY - r.top) / r.height))
            };
        }
        screenView.addEventListener("mousedown", function (e) { dragStart = norm(e); dragging = false; });
        screenView.addEventListener("mousemove", function (e) {
            if (!dragStart) return;
            var p = norm(e);
            var dx = (p.x - dragStart.x) * screenView.getBoundingClientRect().width;
            var dy = (p.y - dragStart.y) * screenView.getBoundingClientRect().height;
            if (Math.sqrt(dx * dx + dy * dy) > 14) dragging = true;
        });
        window.addEventListener("mouseup", function (e) {
            if (!dragStart) return;
            var end = norm(e), start = dragStart;
            dragStart = null;
            if (dragging) {
                send({ t: "swipe", x1: r4(start.x), y1: r4(start.y), x2: r4(end.x), y2: r4(end.y) });
                toast("Swipe");
            } else {
                send({ t: "tap", x: r4(end.x), y: r4(end.y) });
            }
            dragging = false;
        });
        // Obsluga dotyku na telefonach/tabletach przegladajacych panel.
        var touchStart = null;
        screenView.addEventListener("touchstart", function (e) { touchStart = norm(e.touches[0]); }, { passive: true });
        screenView.addEventListener("touchend", function (e) {
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
            toast("Zapisano zrzut ekranu");
        });

        document.getElementById("btnFs").addEventListener("click", function () {
            var box = document.getElementById("screenBox");
            if (document.fullscreenElement) document.exitFullscreen();
            else if (box.requestFullscreen) box.requestFullscreen();
        });

        // Skroty klawiaturowe: B - wstecz, H - home, L - blokada, spacja - pauza.
        window.addEventListener("keydown", function (e) {
            if (e.target.tagName === "SELECT" || e.target.tagName === "INPUT") return;
            var k = e.key.toLowerCase();
            if (k === "b") send({ t: "back" });
            else if (k === "h") send({ t: "home" });
            else if (k === "l") send({ t: "lock" });
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
        }
    )


@app.get("/phones")
async def phones_endpoint():
    return JSONResponse({"phones": phone_list()})


# ---------------------------------------------------------------------------
# WebSocket
# ---------------------------------------------------------------------------
@app.websocket("/")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    clients[websocket] = {
        "kind": "viewer",
        "id": None,
        "model": None,
        "sub": None,
        "frames": 0,
        "last_frame": 0.0,
        "connected": now(),
    }
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
                # Ramka obrazu w formacie binarnym.
                if c["kind"] != "phone":
                    c["kind"] = "phone"
                    c["id"] = c["id"] or f"phone-{int(c['connected'])}"
                    c["model"] = c["model"] or "nieznany"
                    await push_phones()
                c["frames"] += 1
                # Najpierw routing (auto = ktory telefon byl najaktywniejszy
                # WCZESNIEJ), potem aktualizacja timestampu - inaczej kazdy
                # nadawca chwilowo jest "najaktywniejszy" i auto podglada
                # klatki z obu telefonow naraz.
                await relay_frame(websocket, data)
                c["last_frame"] = now()
                continue

            # Wiadomosc tekstowa: komenda JSON albo ramka base64.
            if data.startswith("{"):
                try:
                    obj = json.loads(data)
                except Exception:
                    continue
                t = obj.get("t")

                if t == "hello":
                    dev_id = obj.get("id") or f"phone-{int(c['connected'])}"
                    c["kind"] = "phone"
                    c["id"] = dev_id
                    c["model"] = obj.get("device") or "nieznany"
                    if DEBUG:
                        print(f"[ws] phone {c['model']} ({c['id']}) connected", flush=True)
                    await push_phones()
                    continue

                if c["kind"] == "viewer":
                    await route_command(websocket, obj)
                elif t == "stat":
                    # Telefon raportuje rozdzielczosc / fps - przekazujemy widzom.
                    for ws2, c2 in list(clients.items()):
                        if c2["kind"] == "viewer":
                            await send_to(ws2, data)
                continue

            # Klatka base64 (apki starsze wysylaja tekst).
            if len(data) > 256:
                if c["kind"] != "phone":
                    c["kind"] = "phone"
                    c["id"] = c["id"] or f"phone-{int(c['connected'])}"
                    c["model"] = c["model"] or "nieznany"
                    await push_phones()
                c["frames"] += 1
                await relay_frame(websocket, data)  # routing przed timestampem
                c["last_frame"] = now()

    except WebSocketDisconnect:
        pass
    except Exception as exc:
        import traceback

        print(f"[ws] error: {type(exc).__name__}: {exc}", flush=True)
        traceback.print_exc()
    finally:
        was_phone = clients.get(websocket, {}).get("kind") == "phone"
        discard_client(websocket)
        if was_phone:
            await push_phones()
        if DEBUG:
            print(f"[ws] closed, clients={len(clients)}", flush=True)


async def phones_refresher():
    """Cyklicznie odswieza liste telefonu (online/offline) w przegladarkach."""
    while True:
        await asyncio.sleep(PHONES_PUSH_INTERVAL)
        if any(c["kind"] == "phone" for c in clients.values()):
            await push_phones()


@app.on_event("startup")
async def startup():
    asyncio.create_task(phones_refresher())


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
