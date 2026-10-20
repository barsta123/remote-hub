"""
Remote Hub - serwer podgladu ekranu telefonu na zywo.

Sciezki:
  GET  /        -> strona panelu podgladu (HTML + JS)
  WS   /        -> relay: ramki z telefonu -> przegladarki, komendy -> telefon
  GET  /health  -> status serwera (uzyteczne dla Render / diagnostyki)

Uruchomienie lokalne:   python server.py
Procfile (Render):      uvicorn server:app --host 0.0.0.0 --port $PORT
"""

import asyncio
import os
import time

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse

app = FastAPI(title="Remote Hub")

# Logowanie kazdej ramki przydatne przy diagnostyce: DEBUG=1 python server.py
DEBUG = os.environ.get("DEBUG", "") == "1"

# ---------------------------------------------------------------------------
# Stan polaczen
# ---------------------------------------------------------------------------
# Wszyscy klienci (przegladarki + telefony) podlaczeni pod WS /
clients: set = set()
# Ktorzy klienci wysylaja ramki ekranu (telefony)
phones: set = set()
# Kiedy dany klient ostatnio cos wyslal (do statystyk /health)
last_seen: dict = {}
# Serializacja broadcastu - dzieki temu ramki sie nie "sklejaja" przy
# wielu nadawcach jednoczesnie (telefon wysyla ramki, przegladarka komendy).
relay_lock = asyncio.Lock()


async def broadcast(sender, data):
    """Wysyla dane do wszystkich oprocz nadawcy.

    Martwe polaczenia sa usuwane zamiast przerywac petle nadawania -
    wczesniej powodowaly to, ze zdrowe polaczenie (telefon) bylo wyrzucane
    z serwera przy pierwszej nieudanej probie wysylki do zmarlej przegladarki.
    """
    if not clients:
        return
    dead = []
    ok = 0
    kind = "bytes" if isinstance(data, (bytes, bytearray)) else "text"
    async with relay_lock:
        for client in list(clients):
            if client is sender:
                continue
            try:
                if isinstance(data, (bytes, bytearray)):
                    await client.send_bytes(data)
                else:
                    await client.send_text(data)
                ok += 1
            except (WebSocketDisconnect, RuntimeError, ConnectionError, OSError):
                # Polaczenie juz nie zyje - wyrzucamy je z listy.
                dead.append(client)
            except Exception as exc:
                # Nie kasujemy zdrowych klientow z powodu bledu programu.
                print(f"[relay] unexpected send error ({type(exc).__name__}: {exc})", flush=True)
        for client in dead:
            discard_client(client)
    if DEBUG:
        print(f"[relay] {kind} -> ok={ok} dead={len(dead)} clients={len(clients)}", flush=True)


def discard_client(ws):
    clients.discard(ws)
    phones.discard(ws)
    last_seen.pop(ws, None)


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
            background-color: #0d1117;
            color: #c9d1d9;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            margin: 0;
            padding: 16px;
            display: flex;
            flex-direction: column;
            align-items: center;
            min-height: 100vh;
        }
        h1 { color: #58a6ff; margin: 8px 0 4px; font-size: 22px; }
        .badges { display: flex; gap: 8px; flex-wrap: wrap; justify-content: center; margin-bottom: 14px; }
        .badge {
            background: #161b22; border: 1px solid #30363d; border-radius: 999px;
            padding: 5px 12px; font-size: 12px; color: #8b949e;
        }
        .badge b { color: #c9d1d9; font-weight: 600; }
        .dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; background: #484f58; margin-right: 6px; }
        .dot.on { background: #238636; box-shadow: 0 0 6px #238636; }
        .dot.off { background: #da3633; }
        .device-card {
            background-color: #161b22;
            border: 1px solid #30363d;
            border-radius: 10px;
            padding: 12px;
            width: min(420px, 96vw);
            box-shadow: 0 4px 12px rgba(0,0,0,0.5);
        }
        .device-header {
            font-size: 13px; font-weight: bold; margin-bottom: 10px;
            color: #8b949e; display: flex; justify-content: space-between;
        }
        .screen-container {
            width: 100%; background-color: #000; border-radius: 6px;
            overflow: hidden; display: flex; align-items: center; justify-content: center;
            min-height: 320px; max-height: 78vh; position: relative;
        }
        #screenView {
            width: 100%; height: 100%; object-fit: contain; display: block;
            image-rendering: auto; cursor: crosshair; min-height: 320px;
        }
        #placeholder {
            position: absolute; inset: 0; display: flex; align-items: center;
            justify-content: center; color: #484f58; font-size: 14px;
            text-align: center; padding: 20px; pointer-events: none;
        }
        .controls { display: flex; gap: 8px; margin-top: 10px; }
        button {
            flex: 1; background: #21262d; color: #c9d1d9; border: 1px solid #30363d;
            border-radius: 6px; padding: 9px 10px; font-size: 13px; cursor: pointer;
            transition: background .15s, border-color .15s;
        }
        button:hover { background: #30363d; border-color: #8b949e; }
        button:active { transform: translateY(1px); }
        .hint { color: #6e7681; font-size: 12px; margin-top: 12px; text-align: center; line-height: 1.5; }
        .hint a { color: #58a6ff; }
    </style>
</head>
<body>

    <h1>Remote Hub - Podglad na zywo</h1>

    <div class="badges">
        <span class="badge"><span id="srvDot" class="dot"></span>Serwer: <b id="srvState">laczenie...</b></span>
        <span class="badge"><span id="phDot" class="dot"></span>Telefon: <b id="phState">brak</b></span>
        <span class="badge">FPS: <b id="fps">0</b></span>
        <span class="badge">Kadry: <b id="frames">0</b></span>
    </div>

    <div class="device-card">
        <div class="device-header">
            <span>Podglad ekranu urzadzenia</span>
            <span id="resolution">-</span>
        </div>
        <div class="screen-container" id="screenBox">
            <div id="placeholder">Oczekiwanie na polaczenie z telefonu...</div>
            <img id="screenView" alt="Brak obrazu z telefonu" />
        </div>
        <div class="controls">
            <button id="btnBack" title="Przycisk Wstecz na telefonie">&#8592; Wstecz</button>
            <button id="btnHome" title="Przycisk Home na telefonie">&#8962; Home</button>
            <button id="btnFs" title="Pelny ekran">&#9974; Pelny ekran</button>
        </div>
    </div>

    <div class="hint">
        Kliknij w obraz, aby dotknac ekranu telefonu w tym miejscu.<br>
        Telefon musi miec wlaczona usluge <b>Remote Control (Ulainosc)</b> w Ustawieniach &rarr; Dostepnosc.
    </div>

    <script>
        var screenView = document.getElementById("screenView");
        var placeholder = document.getElementById("placeholder");
        var srvState = document.getElementById("srvState");
        var srvDot = document.getElementById("srvDot");
        var phState = document.getElementById("phState");
        var phDot = document.getElementById("phDot");
        var fpsEl = document.getElementById("fps");
        var framesEl = document.getElementById("frames");
        var resEl = document.getElementById("resolution");

        var ws = null;
        var backoff = 1000;
        var retryTimer = null;
        var frameCount = 0;
        var totalFrames = 0;
        var lastFrameAt = 0;
        var lastObjectUrl = null;
        var connected = false;

        screenView.style.display = "none";

        function setServer(state, on) {
            srvState.textContent = state;
            srvDot.className = "dot " + (on ? "on" : "off");
        }
        function setPhone(state, on) {
            phState.textContent = state;
            phDot.className = "dot " + (on ? "on" : "off");
        }

        function connect() {
            setServer("laczenie...", false);
            var protocol = location.protocol === "https:" ? "wss:" : "ws:";
            try {
                ws = new WebSocket(protocol + "//" + location.host);
            } catch (e) {
                scheduleRetry();
                return;
            }
            ws.binaryType = "blob";

            ws.onopen = function () {
                connected = true;
                backoff = 1000;
                setServer("polaczony", true);
                setPhone("czekam...", false);
            };
            ws.onmessage = function (event) {
                var data = event.data;
                if (typeof data === "string") {
                    // Komunikaty statusowe (JSON) traktujemy osobno - to nie sa ramki.
                    if (data.charAt(0) === "{") { handleStatus(data); return; }
                    showFrame("data:image/jpeg;base64," + data);
                } else {
                    showFrame(URL.createObjectURL(data));
                }
            };
            ws.onclose = function () {
                connected = false;
                setServer("rozlaczony - ponawiam...", false);
                setPhone("brak", false);
                scheduleRetry();
            };
            ws.onerror = function () {
                setServer("blad polaczenia", false);
            };
        }

        function scheduleRetry() {
            clearTimeout(retryTimer);
            retryTimer = setTimeout(connect, backoff);
            backoff = Math.min(Math.round(backoff * 1.5), 15000);
        }

        function handleStatus(text) {
            var msg;
            try { msg = JSON.parse(text); } catch (e) { return; }
            if (msg.t === "hello") {
                setPhone("polaczony", true);
                placeholder.textContent = "Telefon polaczony - czekam na klatki...";
            }
        }

        function showFrame(src) {
            if (lastObjectUrl) { URL.revokeObjectURL(lastObjectUrl); lastObjectUrl = null; }
            if (src.indexOf("blob:") === 0) { lastObjectUrl = src; }
            screenView.src = src;
            screenView.style.display = "block";
            placeholder.style.display = "none";
            frameCount++;
            totalFrames++;
            lastFrameAt = Date.now();
            framesEl.textContent = totalFrames;
            if (!screenView.complete) return;
            if (screenView.naturalWidth) {
                resEl.textContent = screenView.naturalWidth + " x " + screenView.naturalHeight;
            }
            setPhone("na zywo", true);
        }

        // Statystyki FPS + wykrywanie, ze telefon przestal wysylac.
        setInterval(function () {
            fpsEl.textContent = frameCount;
            frameCount = 0;
            if (lastFrameAt && Date.now() - lastFrameAt > 3000) {
                setPhone(connected ? "brak obrazu" : "brak", false);
                if (connected) placeholder.textContent = "Telefon nie wysyla obrazu...";
            }
            if (!screenView.complete) return;
            if (screenView.naturalWidth) {
                resEl.textContent = screenView.naturalWidth + " x " + screenView.naturalHeight;
            }
        }, 1000);

        screenView.onload = function () {
            if (screenView.naturalWidth) {
                resEl.textContent = screenView.naturalWidth + " x " + screenView.naturalHeight;
            }
        };

        function send(obj) {
            if (ws && ws.readyState === 1) {
                ws.send(JSON.stringify(obj));
                return true;
            }
            return false;
        }

        // Klikniecie w obraz = dotkniecie ekranu telefonu (wspolrzedne 0..1).
        screenView.addEventListener("click", function (e) {
            var r = screenView.getBoundingClientRect();
            var x = (e.clientX - r.left) / r.width;
            var y = (e.clientY - r.top) / r.height;
            if (x < 0 || x > 1 || y < 0 || y > 1) return;
            send({ t: "tap", x: Math.round(x * 10000) / 10000, y: Math.round(y * 10000) / 10000 });
        });

        document.getElementById("btnBack").addEventListener("click", function () { send({ t: "back" }); });
        document.getElementById("btnHome").addEventListener("click", function () { send({ t: "home" }); });
        document.getElementById("btnFs").addEventListener("click", function () {
            var box = document.getElementById("screenBox");
            if (document.fullscreenElement) { document.exitFullscreen(); }
            else if (box.requestFullscreen) { box.requestFullscreen(); }
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
    return JSONResponse(
        {
            "ok": True,
            "clients": len(clients),
            "phones": len(phones),
            "uptime": time.time(),
        }
    )


# ---------------------------------------------------------------------------
# WebSocket - relay obrazu i komend
# ---------------------------------------------------------------------------
@app.websocket("/")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    clients.add(websocket)
    last_seen[websocket] = time.time()
    print(f"[ws] open, clients={len(clients)}", flush=True)
    try:
        while True:
            message = await websocket.receive()
            kind = message.get("type")
            if kind == "websocket.disconnect":
                break

            data = message.get("text")
            if data is None:
                data = message.get("bytes")
            if data is None:
                continue

            last_seen[websocket] = time.time()
            if DEBUG:
                print(
                    f"[ws] rx {'bytes:' + str(len(data)) if isinstance(data, (bytes, bytearray)) else 'text:' + str(len(data))}",
                    flush=True,
                )

            # Kto jest telefonem? Ten, kto wysyla ramki (binarnie / base64)
            # lub komunikat powitalny hello.
            if isinstance(data, (bytes, bytearray)):
                phones.add(websocket)
            else:
                if data.startswith('{"t":"hello"'):
                    phones.add(websocket)
                elif len(data) > 256:
                    phones.add(websocket)

            await broadcast(websocket, data)
    except WebSocketDisconnect:
        print("[ws] disconnect", flush=True)
    except Exception as exc:
        # Nigdy nie wyrzucamy bledu, ktory moze zerwac polaczenie zdrowego klienta.
        import traceback

        print(f"[ws] error: {type(exc).__name__}: {exc}", flush=True)
        traceback.print_exc()
    finally:
        discard_client(websocket)
        print(f"[ws] closed, clients={len(clients)}", flush=True)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
