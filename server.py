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

# Kod dostepu do panelu. Domyslnie 4949 - panel NIGDY nie startuje otwarty.
# Zeby wylaczyc zabezpieczenie, ustaw na Render zmienna ACCESS_PIN="" (pusta).
ACCESS_PIN = os.environ.get("ACCESS_PIN", "4949").strip()

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
                "bat": c.get("bat"),
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
                "bat": None,
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

    if kind in ("text", "clip", "launch", "apps", "dblclick"):
        # Komendy dzialajace tylko gdy apka ma wlaczona usluge dostepnosci -
        # serwer przekazuje je do wybranego telefonu jak kazda inna.
        phone = resolve_phone(clients[sender].get("sub"))
        if phone is not None:
            await send_to(phone, json.dumps(obj))
        return

    phone = resolve_phone(clients[sender].get("sub"))
    if phone is None:
        return
    await send_to(phone, json.dumps(obj))


# ---------------------------------------------------------------------------
# Strona panelu (v5 - trzy zakladki)
# ---------------------------------------------------------------------------
html_content = r"""
<!DOCTYPE html>
<html lang="pl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Remote Hub - Panel</title>
    <style>
        :root {
            --bg0: #05070d;
            --bg1: #0a0e1a;
            --glass: rgba(16, 21, 38, 0.72);
            --glass2: rgba(22, 28, 48, 0.55);
            --brd: rgba(140, 160, 230, 0.14);
            --brd-hi: rgba(150, 170, 255, 0.35);
            --txt: #e9edfb;
            --dim: #93a0c4;
            --dim2: #5d6a8f;
            --acc: #6c8cff;
            --acc2: #37e6c0;
            --acc3: #c07bff;
            --ok: #2ee6a8;
            --bad: #ff6b81;
            --warn: #ffc857;
            --r-lg: 20px;
            --r-md: 14px;
            --r-sm: 10px;
            --grad: linear-gradient(135deg, #6c8cff 0%, #37e6c0 100%);
            --grad3: linear-gradient(135deg, #6c8cff 0%, #c07bff 55%, #37e6c0 100%);
        }
        * { box-sizing: border-box; }
        html, body { height: 100%; }
        body {
            margin: 0; padding: 18px 14px 40px;
            color: var(--txt);
            font-family: ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", sans-serif;
            display: flex; flex-direction: column; align-items: center; min-height: 100vh;
            background:
                radial-gradient(1100px 650px at 12% -12%, #17223f 0%, transparent 62%),
                radial-gradient(900px 620px at 105% -5%, #241342 0%, transparent 55%),
                radial-gradient(800px 800px at 50% 120%, #07203a 0%, transparent 60%),
                var(--bg0);
            -webkit-font-smoothing: antialiased;
        }
        ::-webkit-scrollbar { width: 10px; height: 10px; }
        ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb { background: #27304e; border-radius: 99px; border: 2px solid var(--bg0); }
        ::-webkit-scrollbar-thumb:hover { background: #38446e; }

        /* ------------------------------ naglowek --------------------------- */
        .brand { display: flex; align-items: center; gap: 13px; margin: 6px 0 4px; }
        .logo {
            width: 42px; height: 42px; border-radius: 13px;
            background: var(--grad3);
            display: flex; align-items: center; justify-content: center;
            font-size: 20px; box-shadow: 0 8px 26px -8px rgba(108, 140, 255, 0.65);
            position: relative; overflow: hidden;
        }
        .logo::after {
            content: ""; position: absolute; inset: 0;
            background: linear-gradient(120deg, transparent 35%, rgba(255,255,255,.28) 50%, transparent 65%);
        }
        h1 {
            margin: 0; font-size: 23px; font-weight: 800; letter-spacing: -0.4px;
            background: linear-gradient(90deg, #ffffff 10%, #9db4ff 55%, #37e6c0 100%);
            -webkit-background-clip: text; background-clip: text; color: transparent;
        }
        .brand small { display: block; font-size: 11px; color: var(--dim2); font-weight: 500; letter-spacing: .16em; text-transform: uppercase; margin-top: 2px; }

        .badges { display: flex; gap: 7px; flex-wrap: wrap; justify-content: center; margin: 12px 0 16px; }
        .badge {
            display: inline-flex; align-items: center; gap: 7px;
            background: var(--glass); border: 1px solid var(--brd); border-radius: 999px;
            padding: 6px 13px; font-size: 12px; color: var(--dim);
            transition: border-color .3s;
        }
        .badge b { color: var(--txt); font-weight: 650; font-variant-numeric: tabular-nums; }
        .badge.hot { border-color: var(--brd-hi); }
        .dot { width: 8px; height: 8px; border-radius: 50%; background: #3a4568; flex: 0 0 auto; }
        .dot.on { background: var(--ok); box-shadow: 0 0 0 0 rgba(46, 230, 168, .55); animation: pulse 2s infinite; }
        .dot.off { background: var(--bad); }
        @keyframes pulse {
            0% { box-shadow: 0 0 0 0 rgba(46, 230, 168, .5); }
            70% { box-shadow: 0 0 0 7px rgba(46, 230, 168, 0); }
            100% { box-shadow: 0 0 0 0 rgba(46, 230, 168, 0); }
        }

        /* ------------------------------ zakladki --------------------------- */
        .tabs {
            display: flex; gap: 4px; padding: 5px; margin-bottom: 16px;
            background: rgba(14, 18, 33, 0.97); border: 1px solid var(--brd); border-radius: 999px;
            position: sticky; top: 10px; z-index: 20;
        }
        .tabbtn {
            background: transparent; color: var(--dim); border: 0;
            border-radius: 999px; padding: 9px 20px; font-size: 13.5px; font-weight: 600;
            cursor: pointer; transition: all .22s; display: flex; align-items: center; gap: 7px;
        }
        .tabbtn:hover { color: var(--txt); background: rgba(120, 140, 220, 0.10); }
        .tabbtn.active {
            background: var(--grad); color: #fff;
            box-shadow: 0 6px 20px -8px rgba(108, 140, 255, 0.8);
        }
        .tab { display: none; width: min(1040px, 97vw); }
        .tab.active { display: block; animation: fadeUp .28s cubic-bezier(.2,.7,.3,1); }
        @keyframes fadeUp { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: none; } }

        .panel {
            background: rgba(14, 18, 33, 0.94); border: 1px solid var(--brd); border-radius: var(--r-lg);
            padding: 16px; margin-bottom: 14px;
            box-shadow: 0 24px 60px -34px rgba(0, 0, 0, 0.9);
            transition: border-color .3s;
        }
        .panel:hover { border-color: rgba(140, 160, 230, 0.22); }
        .panel h2 {
            margin: 0 0 12px; font-size: 11.5px; font-weight: 700;
            letter-spacing: .14em; text-transform: uppercase; color: var(--acc);
            display: flex; align-items: center; gap: 8px;
        }
        .panel h2::before {
            content: ""; width: 7px; height: 7px; border-radius: 50%; flex: 0 0 auto;
            background: var(--grad3); box-shadow: 0 0 10px rgba(108, 140, 255, .9);
        }
        .panel h2 .cnt { color: var(--dim2); font-weight: 600; letter-spacing: 0; text-transform: none; }
        .row { display: flex; gap: 9px; align-items: center; }
        .row label { font-size: 12px; color: var(--dim); min-width: 74px; }
        select, input[type=password], input[type=text] {
            background: rgba(8, 11, 22, 0.8); color: var(--txt);
            border: 1px solid var(--brd); border-radius: var(--r-sm);
            padding: 9px 11px; font-size: 13.5px; outline: none; flex: 1;
            transition: border-color .2s, box-shadow .2s;
        }
        select:focus, input:focus { border-color: var(--acc); box-shadow: 0 0 0 3px rgba(108, 140, 255, 0.18); }
        select option { background: #0d1222; }

        button {
            background: rgba(120, 140, 220, 0.08); color: var(--txt);
            border: 1px solid var(--brd); border-radius: var(--r-sm);
            padding: 9px 8px; font-size: 12.5px; font-weight: 600; cursor: pointer;
            transition: transform .15s, border-color .2s, background .2s, box-shadow .2s;
            font-family: inherit;
        }
        button:hover { transform: translateY(-2px); border-color: var(--brd-hi); background: rgba(120, 140, 220, 0.16); }
        button:active { transform: translateY(0); }
        button.primary { background: var(--grad); border-color: transparent; color: #fff; box-shadow: 0 8px 22px -10px rgba(108, 140, 255, 0.9); }
        button.primary:hover { filter: brightness(1.08); }
        button.active { background: var(--grad); border-color: transparent; color: #fff; }
        button.ghost { background: transparent; }
        button.mini { padding: 5px 10px; font-size: 11.5px; border-radius: 8px; font-weight: 650; }

        /* ------------------------------ podglad ---------------------------- */
        .split { display: grid; grid-template-columns: minmax(0, 1.65fr) minmax(0, 1fr); gap: 14px; }
        @media (max-width: 940px) { .split { grid-template-columns: 1fr; } }

        .screen-container {
            width: 100%; background: linear-gradient(160deg, #04060c, #080b14);
            border-radius: var(--r-md); overflow: hidden; position: relative;
            display: flex; align-items: center; justify-content: center;
            height: max(240px, calc(100vh - 330px));
            border: 1px solid var(--brd);
            box-shadow: inset 0 0 60px rgba(0, 0, 0, 0.75);
            transition: box-shadow .4s, border-color .4s;
        }
        .screen-container.live {
            border-color: rgba(46, 230, 168, 0.35);
            box-shadow: inset 0 0 60px rgba(0,0,0,.75), 0 0 0 1px rgba(46,230,168,.18), 0 0 44px -14px rgba(46,230,168,.45);
        }
        .screen-container:fullscreen { width: 100%; height: 100%; }
        #screenView {
            position: absolute; inset: 0;
            width: 100%; height: 100%; object-fit: contain; display: none;
            cursor: crosshair; -webkit-user-drag: none; user-select: none; -webkit-user-select: none;
            transform-origin: center center; will-change: transform;
        }
        #placeholder {
            position: absolute; inset: 0; display: flex; flex-direction: column; gap: 8px;
            align-items: center; justify-content: center; color: var(--dim2);
            font-size: 14px; text-align: center; padding: 20px; pointer-events: none;
        }
        #placeholder .ph-ico { font-size: 34px; opacity: .7; animation: float 3.4s ease-in-out infinite; }
        @keyframes float { 0%,100% { transform: translateY(0); } 50% { transform: translateY(-7px); } }

        .livechip {
            position: absolute; top: 10px; left: 10px; z-index: 6;
            display: none; align-items: center; gap: 6px;
            background: rgba(6, 9, 16, 0.72); border: 1px solid rgba(255, 107, 129, 0.5);
            color: #ffd7dd; border-radius: 999px; padding: 4px 11px;
            font-size: 10.5px; font-weight: 800; letter-spacing: .14em;
        }
        .livechip.show { display: inline-flex; }
        .livechip i { width: 7px; height: 7px; border-radius: 50%; background: var(--bad); animation: blink 1.1s infinite; }
        @keyframes blink { 50% { opacity: 0.25; } }
        .reschip {
            position: absolute; top: 10px; right: 10px; z-index: 6;
            background: rgba(6, 9, 16, 0.72); border: 1px solid var(--brd);
            color: var(--dim); border-radius: 999px; padding: 4px 11px; font-size: 11px;
            backdrop-filter: none; display: none;
        }
        .reschip.show { display: inline-block; }
        .darkchip {
            position: absolute; top: 42px; right: 10px; z-index: 6;
            display: none; background: rgba(6, 9, 16, 0.78);
            border: 1px solid rgba(255, 200, 87, 0.55);
            color: #ffe9b8; border-radius: 999px; padding: 4px 11px;
            font-size: 11px; font-weight: 700;
        }
        .darkchip.show { display: inline-block; animation: pulse 2s infinite; }

        .zoombAR {
            position: absolute; left: 10px; bottom: 10px; z-index: 6;
            display: flex; gap: 4px; align-items: center;
            background: rgba(6, 9, 16, 0.7); border: 1px solid var(--brd);
            border-radius: 999px; padding: 4px 6px;
            opacity: 0; transition: opacity .25s;
        }
        .screen-container:hover .zoombAR { opacity: 1; }
        .zoombAR button { border: 0; background: transparent; padding: 3px 8px; font-size: 13px; border-radius: 999px; }
        .zoombAR button:hover { background: rgba(120,140,220,.2); transform: none; }
        #zoomLabel { font-size: 11px; color: var(--dim); min-width: 40px; text-align: center; font-variant-numeric: tabular-nums; }

        .ripple {
            position: fixed; width: 34px; height: 34px; margin: -17px 0 0 -17px;
            border: 2px solid var(--acc2); border-radius: 50%; pointer-events: none; z-index: 50;
            animation: rip .55s ease-out forwards;
        }
        @keyframes rip { from { transform: scale(.35); opacity: 1; } to { transform: scale(2.3); opacity: 0; } }

        .controls { display: grid; grid-template-columns: repeat(4, 1fr); gap: 7px; margin-top: 11px; }
        .controls button { padding: 11px 4px; font-size: 12px; display: flex; flex-direction: column; gap: 3px; align-items: center; border-radius: 12px; }
        .controls button .ic { font-size: 16px; transition: transform .18s ease; }
        .controls button:hover .ic { transform: scale(1.18) translateY(-1px); }

        /* prawy slupek */
        .kpi { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
        .kpi .cell {
            background: rgba(8, 11, 22, 0.6); border: 1px solid var(--brd);
            border-radius: var(--r-sm); padding: 10px 12px;
            position: relative; overflow: hidden;
        }
        .kpi .cell::before {
            content: ""; position: absolute; top: 0; left: 0; right: 0; height: 2px;
            background: var(--grad); opacity: .65;
        }
        .kpi .cell span { display: block; font-size: 10.5px; color: var(--dim2); letter-spacing: .1em; text-transform: uppercase; }
        .kpi .cell b { font-size: 19px; font-weight: 750; font-variant-numeric: tabular-nums; }

        #spark { width: 100%; background: rgba(8, 11, 22, 0.6); border: 1px solid var(--brd); border-radius: var(--r-sm); margin-top: 4px; }

        .hint { color: var(--dim2); font-size: 11.5px; margin-top: 10px; text-align: center; line-height: 1.75; }
        kbd {
            background: rgba(120,140,220,.12); border: 1px solid var(--brd); border-bottom-width: 2px;
            border-radius: 5px; padding: 0 5px; font-size: 10.5px; font-family: inherit; color: var(--dim);
        }

        /* ------------------------------ aplikacje -------------------------- */
        .appsgrid { display: grid; grid-template-columns: repeat(auto-fill, minmax(96px, 1fr)); gap: 10px; }
        .apptile {
            background: rgba(8, 11, 22, 0.55); border: 1px solid var(--brd); border-radius: 14px;
            padding: 12px 6px 9px; text-align: center; cursor: pointer;
            transition: transform .16s, border-color .2s, background .2s;
        }
        .apptile:hover { transform: translateY(-3px); border-color: var(--brd-hi); background: rgba(120,140,220,.1); }
        .apptile:active { transform: translateY(0) scale(.97); }
        .apptile .av {
            width: 40px; height: 40px; margin: 0 auto 7px; border-radius: 12px;
            display: flex; align-items: center; justify-content: center;
            font-weight: 800; font-size: 17px; color: #fff; text-transform: uppercase;
        }
        .apptile .an { font-size: 11px; color: var(--dim); line-height: 1.25; overflow: hidden; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; word-break: break-word; }

        /* ------------------------------ tabele ----------------------------- */
        table { width: 100%; border-collapse: collapse; font-size: 13px; }
        th {
            text-align: left; color: var(--dim2); font-weight: 700; padding: 8px;
            border-bottom: 1px solid var(--brd); font-size: 10.5px;
            letter-spacing: .1em; text-transform: uppercase;
        }
        td { padding: 9px 8px; border-bottom: 1px solid rgba(140,160,230,0.07); }
        tbody tr { transition: background .15s; }
        tbody tr:hover { background: rgba(120, 140, 220, 0.07); }
        .who { display: flex; align-items: center; gap: 9px; }
        .av-sm {
            width: 30px; height: 30px; border-radius: 9px; flex: 0 0 auto;
            display: flex; align-items: center; justify-content: center;
            font-size: 12px; font-weight: 800; color: #fff; text-transform: uppercase;
        }
        .st { font-size: 11.5px; font-weight: 700; display: inline-flex; align-items: center; gap: 6px; }
        .st.on { color: var(--ok); } .st.off { color: var(--dim2); }
        .st i { width: 7px; height: 7px; border-radius: 50%; background: currentColor; }

        /* ------------------------------ log / ustawienia ------------------- */
        .log { max-height: 240px; overflow-y: auto; font-size: 12.5px; }
        .log div {
            padding: 7px 4px 7px 14px; border-bottom: 1px solid rgba(140,160,230,.07);
            color: var(--dim); position: relative;
        }
        .log div::before {
            content: ""; position: absolute; left: 2px; top: 13px;
            width: 6px; height: 6px; border-radius: 50%; background: var(--acc);
            box-shadow: 0 0 8px rgba(108,140,255,.8);
        }
        .log b { color: var(--txt); font-weight: 650; font-variant-numeric: tabular-nums; margin-right: 4px; }
        .empty { text-align: center; color: var(--dim2); padding: 26px 10px; font-size: 13px; }
        .empty .ic { font-size: 34px; display: block; margin-bottom: 8px; opacity: .65; }

        .info-list { font-size: 13px; color: var(--dim); line-height: 1.85; }
        .info-list b { color: var(--txt); font-weight: 650; }

        /* ------------------------------ toast / modal ---------------------- */
        .toast {
            position: fixed; bottom: 22px; left: 50%;
            background: rgba(14, 19, 34, 0.92); border: 1px solid var(--brd-hi);
            color: var(--txt); padding: 10px 20px; border-radius: 999px;
            font-size: 13.5px; font-weight: 600; opacity: 0; pointer-events: none;
            transform: translate(-50%, 16px); transition: all .3s cubic-bezier(.2,.7,.3,1);
            box-shadow: 0 18px 44px -18px rgba(0,0,0,.9); z-index: 30; max-width: 92vw; text-align: center;
        }
        .toast.show { opacity: 1; transform: translate(-50%, 0); }

        #pinModal { position: fixed; inset: 0; background: rgba(3, 5, 10, 0.93); display: none; align-items: center; justify-content: center; z-index: 40; }
        #pinModal.show { display: flex; animation: fadeUp .25s; }
        #pinModal .box {
            background: linear-gradient(rgba(16,21,38,.96), rgba(16,21,38,.96)) padding-box, var(--grad3) border-box;
            border: 1.5px solid transparent; border-radius: 22px; padding: 30px 26px;
            width: min(360px, 92vw); text-align: center;
            box-shadow: 0 34px 90px -30px rgba(108, 140, 255, 0.55);
        }
        #pinModal .box.shake { animation: shake .4s; }
        @keyframes shake { 0%,100% { transform: translateX(0); } 20% { transform: translateX(-9px); } 40% { transform: translateX(8px); } 60% { transform: translateX(-5px); } 80% { transform: translateX(4px); } }
        #pinModal h3 { margin: 10px 0 6px; font-size: 18px; letter-spacing: -.02em; }
        #pinModal .lockico { font-size: 32px; }
        #pinModal p { font-size: 13px; color: var(--dim); margin: 0 0 16px; line-height: 1.5; }
        #pinModal .row { gap: 7px; }
        #pinErr { color: var(--bad); font-size: 12px; min-height: 17px; margin-top: 9px; font-weight: 600; }

        @media (max-width: 620px) {
            .controls { grid-template-columns: repeat(3, 1fr); }
            .tabbtn { padding: 8px 13px; font-size: 12.5px; }
            h1 { font-size: 19px; }
            .kpi { grid-template-columns: 1fr 1fr; }
        }
    </style>
</head>
<body>

    <div class="brand">
        <div class="logo">&#128225;</div>
        <div style="text-align:left;">
            <h1>Remote Hub</h1>
            <small>panel sterowania &middot; na zywo</small>
        </div>
    </div>

    <div class="badges">
        <span class="badge" id="badgeSrv"><span id="srvDot" class="dot"></span>Serwer <b id="srvState">laczenie...</b></span>
        <span class="badge" id="badgePh"><span id="phDot" class="dot"></span><b id="phState">brak</b></span>
        <span class="badge">FPS <b id="fps">0</b></span>
        <span class="badge">Transfer <b id="kbps">0</b> KB/s</span>
        <span class="badge" id="badgeRes">Rozdz. <b id="resolution">-</b></span>
        <span class="badge">Kadry <b id="frames">0</b></span>
        <span class="badge" id="batBadge" style="display:none;">&#128267; <b id="batVal">-</b></span>
        <span class="badge" id="pinBadge" style="display:none;">&#128274; PIN</span>
    </div>

    <div class="tabs">
        <button class="tabbtn active" data-tab="tabPodglad">&#128249; Podglad</button>
        <button class="tabbtn" data-tab="tabAplikacje">&#128241; Aplikacje</button>
        <button class="tabbtn" data-tab="tabTelefony">&#128225; Telefony</button>
        <button class="tabbtn" data-tab="tabUstawienia">&#9881; Ustawienia</button>
    </div>

    <!-- ================================================================ -->
    <div class="tab active" id="tabPodglad">
        <div class="split">
            <div>
                <div class="panel" style="margin-bottom:0;">
                    <div class="screen-container" id="screenBox">
                        <div id="placeholder"><span class="ph-ico">&#128241;</span><span id="phTxt">Oczekiwanie na polaczenie z telefonem...</span></div>
                        <img id="screenView" alt="" decoding="async" />
                        <div class="livechip" id="liveChip"><i></i>LIVE</div>
                        <div class="reschip" id="resChip"></div>
                        <div class="darkchip" id="darkChip">&#128164; Ekran zgaszony</div>
                        <div class="zoombAR">
                            <button id="btnZoomOut" title="Pomniejsz">&#8722;</button>
                            <span id="zoomLabel">100%</span>
                            <button id="btnZoomIn" title="Powieksz">&#43;</button>
                            <button id="btnZoomFit" title="Dopasuj">Dopasuj</button>
                        </div>
                    </div>
                    <div class="controls">
                        <button id="btnBack" title="Wstecz (skrot: B)"><span class="ic">&#8592;</span>Wstecz</button>
                        <button id="btnHome" title="Home (skrot: H)"><span class="ic">&#8962;</span>Home</button>
                        <button id="btnRecents" title="Ostatnie aplikacje"><span class="ic">&#9776;</span>Appy</button>
                        <button id="btnLock" title="Blokada ekranu (skrot: L)"><span class="ic">&#128274;</span>Blokada</button>
                        <button id="btnPower" title="Menu zasilania"><span class="ic">&#9211;</span>Zasilanie</button>
                        <button id="btnScreenshotPhone" title="Zrzut wykonywany NA telefonie"><span class="ic">&#128190;</span>Zrzut tel.</button>
                        <button id="btnPause" title="Pauza obrazu (spacja)"><span class="ic">&#10074;&#10074;</span>Pauza</button>
                        <button id="btnSnap" title="Zapisz aktualny obraz"><span class="ic">&#128247;</span>Pobierz</button>
                        <button id="btnFs" title="Pelny ekran (skrot: F)"><span class="ic">&#9974;</span>Pelny ekran</button>
                        <button id="btnKbd" title="Pisz klawiatura komputera prosto na telefon (Esc = wylacz)"><span class="ic">&#9000;</span>Pisanie</button>
                        <button id="btnAppsShortcut" title="Lista aplikacji na telefonie"><span class="ic">&#128241;</span>Aplikacje</button>
                        <button id="btnLogShortcut" title="Log zdarzen"><span class="ic">&#128203;</span>Log</button>
                    </div>
                    <div class="row" style="margin-top:11px;">
                        <label>Jakosc:</label>
                        <select id="qualitySel" title="Jakosc i czestotliwosc wysylania klatek">
                            <option value="low">Lekka &middot; 8 FPS / 720 px</option>
                            <option value="medium" selected>Szybka &middot; 15 FPS / 960 px</option>
                            <option value="high">Bardzo szybka &middot; 24 FPS / 1280 px</option>
                            <option value="ultra">Maksymalna &middot; 30 FPS / 1600 px</option>
                        </select>
                    </div>
                </div>
            </div>

            <div>
                <div class="panel" style="margin-bottom:14px;">
                    <h2>Szybki tekst i schowek</h2>
                    <div class="row" style="margin-bottom:8px;">
                        <input type="text" id="quickTxt" placeholder="Napisz cos i wyslij prosto na ekran..." maxlength="300">
                        <button class="primary" id="btnSendTxt" style="flex:0 0 auto;">Wy&#347;slij</button>
                    </div>
                    <div class="row">
                        <button id="btnClip" style="flex:1;">Skopiuj do schowka telefonu</button>
                        <button id="btnPasteHint" style="flex:0 0 auto;" title="Jak wkleic?">Jak wkleic?</button>
                    </div>
                    <div class="hint" style="text-align:left; margin-top:8px;">
                        „Wy&#347;slij" wpisuje tekst do <b>otwartego pola</b> na telefonie.
                        „Schowek" kopiuje - wklejysz <b>dlugim dotknieciem</b> w dowolnym miejscu.
                    </div>
                </div>

                <div class="panel" style="margin-bottom:14px;">
                    <h2>Na zywo <span class="cnt">ostatnie 90 s</span></h2>
                    <div class="kpi">
                        <div class="cell"><span>FPS teraz</span><b id="kpiFps" style="color:var(--acc);">0</b></div>
                        <div class="cell"><span>Transfer</span><b id="kpiKbps" style="color:var(--acc2);">0<span style="font-size:11px;color:var(--dim2);"> KB/s</span></b></div>
                    </div>
                    <canvas id="spark" width="480" height="110"></canvas>
                </div>

                <div class="panel" style="margin-bottom:0;">
                    <h2>Sesja</h2>
                    <div class="info-list">
                        Czas podlaczenia: <b id="sessTime">0 s</b><br>
                        odebranych klatek: <b id="sessFrames">0</b><br>
                        transfer: <b id="sessMB">0.0 MB</b><br>
                        srednie FPS: <b id="sessAvg">0</b>
                    </div>
                    <button id="btnRec" class="primary" style="width:100%; margin-top:11px;"
                            title="Nagrywa obraz z telefonu do pliku wideo (webm)">&#9210; Nagrywaj wideo</button>
                </div>
            </div>
        </div>

        <div class="panel" style="margin-top:14px;">
            <div class="hint" style="margin-top:0;">
                <b>Klik</b> = dotkniecie &middot; <b>przeciagnij</b> = swipe &middot; <b>przytrzymaj</b> = long-press &middot;
                <b>podwojny klik</b> = double-tap &middot; <b>kolo myszy</b> = przewijanie &middot;
                <b>prawy przycisk</b> = wstecz &middot; <b>Ctrl + kolo</b> = zoom &middot; <b>Shift + ciagnij</b> = przesuwanie obrazu<br>
                Skroty: <kbd>B</kbd> wstecz &middot; <kbd>H</kbd> home &middot; <kbd>L</kbd> blokada &middot;
                <kbd>F</kbd> pelny ekran &middot; <kbd>spacja</kbd> pauza<br>
                Tryb <b>&#9000; Pisanie</b> - co napiszesz na klawiaturze komputera, wchodzi
                <b>prosto na otwarte pole telefonu</b> (spacja, <kbd>Enter</kbd>, <kbd>Backspace</kbd> tez dzialaja; <kbd>Esc</kbd> wylacza)
            </div>
        </div>
    </div>

    <!-- ================================================================ -->
    <div class="tab" id="tabAplikacje">
        <div class="panel">
            <h2>Aplikacje na telefonie <span class="cnt" id="appsMeta"></span></h2>
            <div class="row" style="margin-bottom:12px;">
                <button class="primary" id="btnAppsRefresh">&#8635; Odswiez liste aplikacji</button>
                <button id="btnAppsHome">&#8962; Wroc do ekranu glownego</button>
            </div>
            <div class="appsgrid" id="appsGrid">
                <div class="empty" style="grid-column:1/-1;"><span class="ic">&#128241;</span>
                    Kliknij „Odswiez liste" - telefon odesle liste zainstalowanych aplikacji.<br>
                    <span style="font-size:11.5px;">(wymaga apki Remote Control 1.5 + wlaczonego zdalnego sterowania)</span>
                </div>
            </div>
        </div>
        <div class="panel">
            <h2>Jak to dziala</h2>
            <div class="info-list">
                Klikniecie kafelka <b>uruchamia aplikacje na telefonie</b> i przelacza do podgladu.
                Lista jest pobierana na zyczenie (nie obciaza baterii ani sieci).
                Pierwsza litera w kolorowym kafelku to skrot nazwy aplikacji.
            </div>
        </div>
    </div>

    <!-- ================================================================ -->
    <div class="tab" id="tabTelefony">
        <div class="panel">
            <h2>Wybierz telefon do podgladu</h2>
            <div class="row" style="margin-bottom:9px;">
                <label>Telefon:</label>
                <select id="phoneSel" title="Wybierz urzadzenie do podgladu">
                    <option value="auto">Automatycznie (najaktywniejszy)</option>
                </select>
            </div>
            <div class="row">
                <button id="btnRename" title="Nadaj wlasna nazwe wybranemu telefonowi">Zmien nazwe wybranego</button>
            </div>
        </div>
        <div class="panel">
            <h2>Wszystkie telefony z apka <span class="cnt" id="phonesCnt"></span></h2>
            <table>
                <thead>
                    <tr><th>Telefon</th><th>Nazwa</th><th>Status</th><th>Bateria</th><th>Ostatnio</th><th></th></tr>
                </thead>
                <tbody id="phoneTable">
                    <tr><td colspan="6"><div class="empty"><span class="ic">&#128241;</span>Ladowanie...</div></td></tr>
                </tbody>
            </table>
        </div>
        <div class="panel">
            <h2>Co tu dziala</h2>
            <div class="info-list">
                Kazdy telefon z zainstalowana apka <b>Remote Control</b> pojawia sie na liscie - takze wylaczony
                (z czasem ostatniego widzenia). Nazwy (np. „Telefon taty") widza wszystkie przegladarki.
                <b>Obserwuj</b> - podglada wybrany telefon. <b>Automatycznie</b> - zawsze najaktywniejszy.
                Kolumna <b>Bateria</b> pokazuje stan na zywo (apka raportuje co 5 s).
            </div>
        </div>
    </div>

    <!-- ================================================================ -->
    <div class="tab" id="tabUstawienia">
        <div class="panel">
            <h2>&#128274; Kod dostepu do panelu (PIN)</h2>
            <div class="info-list" style="font-size:12.5px; line-height:1.6; margin-bottom:11px;">
                Panel chroni podglad kodem - bez niego nikt nie zobaczy ekranu ani nie polaczy sie z telefonem.
                Aplikacja na telefonie tez wysyla ten kod (sekcja PIN w ustawieniach apki).
            </div>
            <div class="row">
                <label>PIN:</label>
                <input type="password" id="pinSetting" placeholder="kod dostepu do panelu" maxlength="32">
                <button class="primary" id="btnPinSave" style="flex:0 0 auto;">Zapisz i wyslij</button>
            </div>
            <div id="pinStatus" style="font-size:12px; color:var(--dim); margin-top:9px;">Sprawdzanie...</div>
        </div>

        <div class="panel">
            <h2>Log zdarzen</h2>
            <div class="log" id="eventLog"><div>Brak zdarzen.</div></div>
        </div>

        <div class="panel">
            <h2>Informacje</h2>
            <div class="info-list">
                Serwer: <b id="srvUrl"></b><br>
                Panel: <b>Remote Hub v6</b> &middot; apka: <b>Remote Control v1.5+</b><br>
                Sterowanie dziala po wlaczeniu uslugi <b>Remote Control</b> w telefonie
                (Ustawienia &rarr; Dostepnosc).<br>
                Skroty klawiszy: <kbd>B</kbd> <kbd>H</kbd> <kbd>L</kbd> <kbd>F</kbd> <kbd>spacja</kbd>
            </div>
        </div>
    </div>

    <div id="toast" class="toast"></div>

    <!-- Okno PIN -->
    <div id="pinModal">
        <div class="box" id="pinBox">
            <div class="lockico">&#128274;</div>
            <h3>Panel zabezpieczony</h3>
            <p>Podaj kod dostepu, aby zobaczyc podglad ekranu telefonu i przejac sterowanie.</p>
            <div class="row">
                <input type="password" id="pinInput" placeholder="Kod dostepu" maxlength="32">
                <button class="primary" id="pinSend" style="flex:0 0 auto;">Wejdz</button>
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
        var appsList = null, appsRequestedAt = 0;
        var rec = null, recChunks = [], recStart = 0, recCanvas = null, recCtx = null;
        var darkCanvas = document.createElement("canvas");
        darkCanvas.width = 24; darkCanvas.height = 24;
        var darkCtx = darkCanvas.getContext("2d", { willReadFrequently: true });

        var QUALITIES = {
            low:    { fps: 8,  q: 30, max: 720  },
            medium: { fps: 15, q: 36, max: 960  },
            high:   { fps: 24, q: 42, max: 1280 },
            ultra:  { fps: 30, q: 50, max: 1600 }
        };

        qualitySel.value = QUALITIES[quality] ? quality : "medium";

        function setServer(state, on) { srvState.textContent = state; srvDot.className = "dot " + (on ? "on" : "off"); }
        function setPhone(state, on) { phState.textContent = state; phDot.className = "dot " + (on ? "on" : "off"); }
        function toast(msg) {
            toastEl.textContent = msg; toastEl.className = "toast show";
            clearTimeout(toastTimer); toastTimer = setTimeout(function () { toastEl.className = "toast"; }, 1900);
        }
        function send(obj) {
            if (ws && ws.readyState === 1) { ws.send(JSON.stringify(obj)); return true; }
            return false;
        }
        function hue(s) { var h = 0; for (var i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) % 360; return h; }

        // ------------------------------------------------------------------
        // Zakladki
        // ------------------------------------------------------------------
        document.querySelectorAll(".tabbtn").forEach(function (b) {
            b.addEventListener("click", function () { gotoTab(b.dataset.tab, true); });
        });
        function gotoTab(id, scroll) {
            var btn = document.querySelector('.tabbtn[data-tab="' + id + '"]');
            if (!btn) return;
            document.querySelectorAll(".tabbtn").forEach(function (x) { x.classList.remove("active"); });
            document.querySelectorAll(".tab").forEach(function (x) { x.classList.remove("active"); });
            btn.classList.add("active");
            document.getElementById(id).classList.add("active");
            if (id === "tabAplikacje" && appsList === null && !appsRequestedAt) requestApps();
            if (scroll !== false) window.scrollTo({ top: 0, behavior: "smooth" });
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
                    authed = true; hidePinModal();
                    toast("Zalogowano - panel odblokowany");
                    sendView(); sendQuality();
                } else {
                    authed = false;
                    localStorage.removeItem("rh_pin");
                    showPinModal(true);
                }
            } else if (msg.t === "hello") {
                send({ t: "phones" });
            } else if (msg.t === "stat") {
                if (msg.w && msg.h) setResolution(msg.w + " x " + msg.h);
                // aktywny telefon widac od razu - nawet zanim serwer odswiezy liste
                if (msg.id && !phones[msg.id]) {
                    phones[msg.id] = { id: msg.id, model: msg.device || ("Telefon " + msg.id), name: null,
                                       online: true, since: 0, frames: 0, bat: null };
                    renderPhones();
                } else if (msg.id && phones[msg.id] && !phones[msg.id].online) {
                    phones[msg.id].online = true;
                    phones[msg.id].since = 0;
                    renderPhones();
                }
                if (typeof msg.bat === "number") {
                    if (msg.id && phones[msg.id] && phones[msg.id].bat !== msg.bat) {
                        phones[msg.id].bat = msg.bat;
                        renderPhones();
                    }
                    showBattery(msg.bat);
                }
            } else if (msg.t === "apps") {
                appsList = Array.isArray(msg.list) ? msg.list : [];
                appsRequestedAt = 0;
                renderApps();
                toast("Odebrano liste: " + appsList.length + " aplikacji");
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
            if (wrong) {
                var box = document.getElementById("pinBox");
                box.classList.remove("shake"); void box.offsetWidth; box.classList.add("shake");
            } else {
                var inp = document.getElementById("pinInput");
                inp.value = ""; inp.focus();
            }
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
            if (!pinRequired) el.textContent = "Serwer dziala bez zabezpieczenia - podglad otwarty.";
            else if (authed) el.textContent = "Zalogowano - podglad chroniony PINEM.";
            else el.textContent = "Serwer wymaga PIN - podaj go w okienku logowania.";
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
        function initials(s) {
            var parts = String(s || "?").replace(/[^A-Za-z0-9 ]/g, " ").trim().split(/\s+/);
            return ((parts[0] || "?")[0] || "?") + ((parts[1] || "")[0] || "");
        }
        function showBattery(pct) {
            var b = document.getElementById("batBadge");
            if (typeof pct === "number" && pct >= 0) {
                document.getElementById("batVal").textContent = pct + "%";
                b.style.display = "";
                b.classList.toggle("hot", pct <= 20);
            } else b.style.display = "none";
        }

        function renderPhones() {
            var current = phoneSel.value || "auto";
            var ids = Object.keys(phones);
            // przebudowujemy liste wyboru TYLKO gdy sie cos zmienilo - dzieki temu
            // odswiezanie co 5 s nie zamyka listy, gdy wlasciwie wybierasz telefon.
            var selKey = ids.map(function (id) {
                var p = phones[id];
                return id + "|" + (p.online ? 1 : 0) + "|" + (p.name || "") + "|" + p.model + "|" + (p.bat == null ? "" : p.bat);
            }).join(";");
            if (selKey !== phoneSel._key) {
                phoneSel._key = selKey;
                var html = '<option value="auto">Automatycznie (najaktywniejszy)</option>';
                var act = [], off = [];
                ids.forEach(function (id) { (phones[id].online ? act : off).push(id); });
                function opts(list) {
                    var s = "";
                    list.forEach(function (id) {
                        var p = phones[id];
                        var label = (p.name || p.model) + (typeof p.bat === "number" ? " \u{1F50B} " + p.bat + "%" : "") +
                            (p.online ? "  \u{1F7E2} aktywny teraz" : "  \u{1F534} offline, widziany " + fmtSince(p.since));
                        s += '<option value="' + id + '">' + label + "</option>";
                    });
                    return s;
                }
                if (act.length) html += '<optgroup label="Aktywne teraz">' + opts(act) + "</optgroup>";
                if (off.length) html += '<optgroup label="Zapisane (offline)">' + opts(off) + "</optgroup>";
                phoneSel.innerHTML = html;
            }

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
                setPhone(p2 ? (p2.name || p2.model) + (p2.online ? "" : " (offline)") : "brak", !!(p2 && p2.online));
                showBattery(p2 ? p2.bat : null);
            }
            document.getElementById("phonesCnt").textContent = ids.length + " w spisie";
            renderPhoneTable();
        }

        function renderPhoneTable() {
            var tb = document.getElementById("phoneTable");
            var ids = Object.keys(phones);
            if (!ids.length) {
                tb.innerHTML = '<tr><td colspan="6"><div class="empty"><span class="ic">&#128241;</span>' +
                    "Jeszcze zaden telefon nie polaczyl sie z tym serwerem.</div></td></tr>";
                return;
            }
            var rows = "";
            ids.forEach(function (id) {
                var p = phones[id];
                var nm = p.name || p.model;
                rows += "<tr>" +
                    '<td><div class="who"><div class="av-sm" style="background:linear-gradient(135deg,hsl(' +
                    hue(nm) + ",70%,55%),hsl(" + (hue(nm) + 60) + ',70%,45%));">' + initials(nm) + "</div>" +
                    "<div>" + p.model + (p.name ? '<div style="font-size:11px;color:var(--dim2);">' + p.name + "</div>" : "") + "</div></div></td>" +
                    "<td>" + (p.name || "<span style='color:var(--dim2);'>-</span>") + "</td>" +
                    '<td><span class="st ' + (p.online ? "on" : "off") + '"><i></i>' +
                    (p.online ? "online" : "offline") + "</span></td>" +
                    "<td>" + (typeof p.bat === "number" ? "\u{1F50B} " + p.bat + "%" : "<span style='color:var(--dim2);'>-</span>") + "</td>" +
                    "<td>" + (p.online ? "nadaje teraz" : fmtSince(p.since)) + "</td>" +
                    '<td style="text-align:right; white-space:nowrap;">' +
                    '<button class="mini primary" data-watch="' + id + '">Obserwuj</button> ' +
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
                    var p = phones[selectedPhone];
                    toast("Obserwuje: " + (p ? (p.name || p.model) : ""));
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
            var key = evts.map(function (e) { return e.ts + "|" + e.text; }).join("~");
            if (key === el._key) return;   // nic sie nie zmienilo - nie kasujemy przewijenia
            el._key = key;
            if (!evts.length) { el.innerHTML = "<div>Brak zdarzen.</div>"; return; }
            var html = "";
            evts.slice().reverse().forEach(function (e) {
                var t = new Date(e.ts * 1000).toLocaleTimeString("pl-PL");
                html += "<div><b>" + t + "</b>" + e.text + "</div>";
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
            if (selectedPhone === "auto" || !phones[selectedPhone]) { toast("Najpierw wybierz telefon z listy"); return; }
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
            toast("Jakosc: " + qualitySel.options[qualitySel.selectedIndex].text);
        });

        // ------------------------------------------------------------------
        // Obraz: ramki, LIVE, wykres, sesja
        // ------------------------------------------------------------------
        function setResolution(r) {
            resEl.textContent = r;
            var c = document.getElementById("resChip");
            c.textContent = r; c.classList.add("show");
        }
        function showFrame(src, bytes) {
            frameCount++; totalFrames++; sessFrames++;
            lastFrameAt = Date.now();
            byteWindow += bytes || 0; totalBytes += bytes || 0;
            framesEl.textContent = totalFrames;
            if (paused || document.hidden) return;   // ukryta karta nie dekoduje klatek (mniej lagow)
            if (lastObjectUrl) { URL.revokeObjectURL(lastObjectUrl); lastObjectUrl = null; }
            if (src.indexOf("blob:") === 0) lastObjectUrl = src;
            screenView.src = src;
            screenView.style.display = "block";
            placeholder.style.display = "none";
            setPhone(textOfSelected(), true);
            if (rec && recCtx) {
                // rysujemy biezaca klatke na kanwie nagrywania (z opoznieniem 1 klatki)
                try { recCtx.drawImage(screenView, 0, 0, recCanvas.width, recCanvas.height); } catch (ignored) {}
            }
        }
        function textOfSelected() {
            var id = selectedPhone === "auto" ? autoPhoneId : selectedPhone;
            var p = phones[id];
            return p ? (p.name || p.model) : "na zywo";
        }

        var spark = document.getElementById("spark"), sctx = spark.getContext("2d");
        function drawSpark() {
            var W = spark.width, H = spark.height;
            sctx.clearRect(0, 0, W, H);
            sctx.font = "600 10px ui-sans-serif, system-ui, sans-serif";
            function series(arr, color, maxV, label, unit, top) {
                var n = arr.length;
                if (!n) return;
                var pts = [];
                for (var i = 0; i < n; i++) {
                    var x = (i / 89) * (W - 60) + 54;
                    var y = H - 8 - Math.min(1, arr[i] / maxV) * (H - 30);
                    pts.push([x, y]);
                }
                // wypelnienie gradientem
                var g = sctx.createLinearGradient(0, 0, 0, H);
                g.addColorStop(0, color.replace("rgb", "rgba").replace(")", ",0.30)"));
                g.addColorStop(1, color.replace("rgb", "rgba").replace(")", ",0.00)"));
                sctx.beginPath();
                sctx.moveTo(pts[0][0], H - 8);
                pts.forEach(function (p) { sctx.lineTo(p[0], p[1]); });
                sctx.lineTo(pts[pts.length - 1][0], H - 8);
                sctx.closePath();
                sctx.fillStyle = g; sctx.fill();
                // linia
                sctx.beginPath();
                pts.forEach(function (p, i) { i ? sctx.lineTo(p[0], p[1]) : sctx.moveTo(p[0], p[1]); });
                sctx.strokeStyle = color; sctx.lineWidth = 1.8; sctx.stroke();
                // kropka na koncu
                var lastp = pts[pts.length - 1];
                sctx.beginPath(); sctx.arc(lastp[0], lastp[1], 3, 0, 7); sctx.fillStyle = color; sctx.fill();
                // etykieta
                sctx.fillStyle = color;
                sctx.fillText(label + " " + (arr[n - 1]) + unit, 6, top);
            }
            series(histFps, "rgb(108,140,255)", 30, "FPS", "", 13);
            series(histKbps, "rgb(55,230,192)", 2048, "Transfer", " KB/s", 28);
        }

        setInterval(function () {
            fpsEl.textContent = frameCount;
            histFps.push(frameCount); if (histFps.length > 90) histFps.shift();
            frameCount = 0;
            kbps = Math.round(byteWindow / 1024); byteWindow = 0;
            histKbps.push(kbps); if (histKbps.length > 90) histKbps.shift();
            kbpsEl.textContent = kbps;
            document.getElementById("kpiFps").textContent = histFps.length ? histFps[histFps.length - 1] : 0;
            document.getElementById("kpiKbps").innerHTML = kbps + '<span style="font-size:11px;color:var(--dim2);"> KB/s</span>';
            drawSpark();

            var secs = Math.round((Date.now() - sessStart) / 1000);
            document.getElementById("sessTime").textContent =
                secs < 120 ? secs + " s" : Math.round(secs / 60) + " min";
            document.getElementById("sessFrames").textContent = sessFrames;
            document.getElementById("sessMB").textContent = (totalBytes / 1048576).toFixed(1);
            document.getElementById("sessAvg").textContent = sessFrames && secs ? Math.round(sessFrames / secs) : 0;

            var live = lastFrameAt && Date.now() - lastFrameAt < 2000;
            document.getElementById("liveChip").classList.toggle("show", !!live && !paused);
            document.getElementById("screenBox").classList.toggle("live", !!live && !paused);
            document.getElementById("darkChip").classList.toggle(
                "show", !!live && !paused && screenView.style.display === "block" && isScreenDark());

            if (lastFrameAt && Date.now() - lastFrameAt > 3000 && !paused) {
                setPhone(Object.keys(phones).length ? "brak obrazu" : "brak", false);
                if (Object.keys(phones).length) document.getElementById("phTxt").textContent = "Telefon nie wysyla obrazu - sprawdz apke na telefonie...";
            }
        }, 1000);

        screenView.onload = function () {
            if (screenView.naturalWidth) setResolution(screenView.naturalWidth + " x " + screenView.naturalHeight);
            fitContainer();
        };
        function fitContainer() {
            // kontener dopasowany do proporcji obrazu - telefon zawsze widac w calosci
            var box = document.getElementById("screenBox");
            if (document.fullscreenElement === box) { box.style.height = ""; return; }
            var maxH = Math.max(240, window.innerHeight - 330);
            var nw = screenView.naturalWidth, nh = screenView.naturalHeight, h = maxH;
            if (nw && nh && box.clientWidth) h = Math.min(maxH, Math.round(box.clientWidth * nh / nw));
            box.style.height = Math.max(240, h) + "px";
        }
        window.addEventListener("resize", fitContainer);
        document.addEventListener("fullscreenchange", fitContainer);

        // ------------------------------------------------------------------
        // Sterowanie: mysz (klik / swipe / long-press / dbl / kolo / prawy / zoom)
        // ------------------------------------------------------------------
        var dragStart = null, dragging = false, holdTimer = null, holdDone = false;
        var lastTap = { t: 0, x: 0, y: 0 }, dblDone = false;
        var zoom = 1, panX = 0, panY = 0, panning = false, panLast = null, panMoved = false;

        function contentRect() {
            // faktyczny obszar obrazu w kontenerze (object-fit: contain - moga byc pasy)
            var r = screenView.getBoundingClientRect();
            var nw = screenView.naturalWidth, nh = screenView.naturalHeight;
            if (!nw || !nh || !r.width || !r.height) return r;
            var s = Math.min(r.width / nw, r.height / nh);
            var cw = nw * s, ch = nh * s;
            return { left: r.left + (r.width - cw) / 2, top: r.top + (r.height - ch) / 2,
                     width: cw, height: ch };
        }
        function norm(e) {
            var r = contentRect();
            return {
                x: Math.max(0, Math.min(1, (e.clientX - r.left) / r.width)),
                y: Math.max(0, Math.min(1, (e.clientY - r.top) / r.height))
            };
        }
        function r4(v) { return Math.round(v * 10000) / 10000; }
        function ripple(cx, cy) {
            var el = document.createElement("div");
            el.className = "ripple";
            el.style.left = cx + "px"; el.style.top = cy + "px";
            document.body.appendChild(el);
            setTimeout(function () { el.remove(); }, 600);
        }
        function applyView() {
            screenView.style.transform =
                (zoom === 1 && panX === 0 && panY === 0) ? "none" :
                "translate(" + panX + "px," + panY + "px) scale(" + zoom + ")";
            document.getElementById("zoomLabel").textContent = Math.round(zoom * 100) + "%";
        }
        function setZoom(z) {
            zoom = Math.max(1, Math.min(4, z));
            if (zoom === 1) { panX = 0; panY = 0; }
            applyView();
        }

        screenView.addEventListener("mousedown", function (e) {
            // przesuwanie obrazu: srodkowy LUB shift + ciagniecie
            if (e.button === 1 || (e.button === 0 && e.shiftKey)) {
                e.preventDefault();
                panning = true; panMoved = false; panLast = { x: e.clientX, y: e.clientY };
                return;
            }
            if (e.button !== 0) return;
            dragStart = norm(e); dragging = false; holdDone = false; dblDone = false;
            var p = dragStart;
            holdTimer = setTimeout(function () {
                if (dragStart && !dragging) {
                    send({ t: "longpress", x: r4(p.x), y: r4(p.y) });
                    holdDone = true;
                    ripple(e.clientX, e.clientY);
                    toast("Przytrzymanie");
                }
            }, 520);
        });
        window.addEventListener("mousemove", function (e) {
            if (panning) {
                panMoved = true;
                panX += e.clientX - panLast.x;
                panY += e.clientY - panLast.y;
                panLast = { x: e.clientX, y: e.clientY };
                applyView();
                return;
            }
            if (!dragStart) return;
            var p = norm(e);
            var r = contentRect();
            var dx = (p.x - dragStart.x) * r.width;
            var dy = (p.y - dragStart.y) * r.height;
            if (Math.sqrt(dx * dx + dy * dy) > 12) { dragging = true; clearTimeout(holdTimer); }
        });
        window.addEventListener("mouseup", function (e) {
            if (panning) { panning = false; return; }
            clearTimeout(holdTimer);
            if (!dragStart) return;
            var end = norm(e), start = dragStart;
            dragStart = null;
            if (holdDone) { holdDone = false; return; }
            if (dragging) {
                send({ t: "swipe", x1: r4(start.x), y1: r4(start.y), x2: r4(end.x), y2: r4(end.y) });
                ripple(e.clientX, e.clientY);
                toast("Swipe");
                dragging = false;
                return;
            }
            // klik: wykrywanie podwojnego klikniecia
            var now = Date.now();
            var isDbl = (now - lastTap.t < 350) &&
                Math.abs(end.x - lastTap.x) < 0.06 && Math.abs(end.y - lastTap.y) < 0.06;
            if (isDbl) {
                send({ t: "dblclick", x: r4(end.x), y: r4(end.y) });
                lastTap.t = 0;
                ripple(e.clientX, e.clientY);
                toast("Podwojny klik");
            } else {
                send({ t: "tap", x: r4(end.x), y: r4(end.y) });
                ripple(e.clientX, e.clientY);
                lastTap = { t: now, x: end.x, y: end.y };
            }
            dragging = false;
        });

        // prawy przycisk = wstecz (zamiast menu przegladarki)
        screenView.addEventListener("contextmenu", function (e) {
            e.preventDefault();
            send({ t: "back" });
            toast("Wstecz (prawy przycisk)");
        });

        // kolo myszy = przewijanie ekranu telefonu; Ctrl+kolo = zoom
        screenView.addEventListener("wheel", function (e) {
            e.preventDefault();
            if (e.ctrlKey || e.metaKey) {
                setZoom(zoom * (e.deltaY < 0 ? 1.18 : 0.85));
                return;
            }
            var now = Date.now();
            if (now - (screenView._lastWheel || 0) < 85) return;
            screenView._lastWheel = now;
            var down = e.deltaY > 0;
            var y1 = down ? 0.66 : 0.34, y2 = down ? 0.34 : 0.66;
            send({ t: "swipe", x1: 0.5, y1: y1, x2: 0.5, y2: y2 });
        }, { passive: false });

        document.getElementById("btnZoomIn").addEventListener("click", function () { setZoom(zoom * 1.35); });
        document.getElementById("btnZoomOut").addEventListener("click", function () { setZoom(zoom / 1.35); });
        document.getElementById("btnZoomFit").addEventListener("click", function () { setZoom(1); });

        // dotyk (telefon/tablet jako pilot)
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
            }, 520);
        }, { passive: true });
        screenView.addEventListener("touchend", function (e) {
            clearTimeout(touchHold);
            if (!touchStart) return;
            var end = norm(e.changedTouches[0]);
            var r = contentRect();
            var dx = (end.x - touchStart.x) * r.width;
            var dy = (end.y - touchStart.y) * r.height;
            if (Math.sqrt(dx * dx + dy * dy) > 12) send({ t: "swipe", x1: r4(touchStart.x), y1: r4(touchStart.y), x2: r4(end.x), y2: r4(end.y) });
            else send({ t: "tap", x: r4(end.x), y: r4(end.y) });
            touchStart = null;
        }, { passive: true });

        // ------------------------------------------------------------------
        // Przyciski sterowania
        // ------------------------------------------------------------------
        function cmdBtn(id, obj, msg) {
            document.getElementById(id).addEventListener("click", function () { send(obj); if (msg) toast(msg); });
        }
        cmdBtn("btnBack", { t: "back" }, "Wstecz");
        cmdBtn("btnHome", { t: "home" }, "Home");
        cmdBtn("btnRecents", { t: "recents" }, "Ostatnie aplikacje");
        cmdBtn("btnLock", { t: "lock" }, "Blokada ekranu");
        cmdBtn("btnPower", { t: "power" }, "Menu zasilania");
        cmdBtn("btnScreenshotPhone", { t: "screenshot" }, "Zrzut na telefonie (galeria)");
        var kbdMode = false;
        function setKbd(on) {
            kbdMode = on;
            document.getElementById("btnKbd").classList.toggle("active", on);
            toast(on ? "Pisanie WLACZONE - pisz na klawiaturze, tekst wejdzie na telefon (Esc = wylacz)"
                     : "Pisanie wylaczone - wracaja skroty B/H/L/F");
        }
        document.getElementById("btnKbd").addEventListener("click", function () { setKbd(!kbdMode); });
        document.getElementById("btnAppsShortcut").addEventListener("click", function () { gotoTab("tabAplikacje"); });
        document.getElementById("btnLogShortcut").addEventListener("click", function () { gotoTab("tabUstawienia"); });

        document.getElementById("btnPause").addEventListener("click", function () {
            paused = !paused;
            this.classList.toggle("active", paused);
            this.innerHTML = paused ? '<span class="ic">&#9654;</span>Wznow' : '<span class="ic">&#10074;&#10074;</span>Pauza';
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

        // ------------------------------------------------------------------
        // Wykrywanie zgaszonego ekranu + nagrywanie wideo (webm)
        // ------------------------------------------------------------------
        function isScreenDark() {
            try {
                darkCtx.drawImage(screenView, 0, 0, 24, 24);
                var d = darkCtx.getImageData(0, 0, 24, 24).data, sum = 0;
                for (var i = 0; i < d.length; i += 4) sum += (d[i] + d[i + 1] + d[i + 2]) / 3;
                return (sum / (d.length / 4)) < 4;   // tylko prawie czysta czern = ekran zgaszony
            } catch (e) { return false; }
        }

        var btnRec = document.getElementById("btnRec");
        btnRec.addEventListener("click", function () {
            if (!screenView.src) { toast("Brak obrazu do nagrania"); return; }
            if (rec) { rec.stop(); return; }   // drugie klikniecie = stop i zapis
            try {
                if (!recCanvas) recCanvas = document.createElement("canvas");
                var nw = screenView.naturalWidth || 720, nh = screenView.naturalHeight || 1280;
                var sc = Math.min(1, 960 / Math.max(nw, nh));
                recCanvas.width = Math.max(2, Math.round(nw * sc));
                recCanvas.height = Math.max(2, Math.round(nh * sc));
                recCtx = recCanvas.getContext("2d");
                var stream = recCanvas.captureStream(15);
                var mime = MediaRecorder.isTypeSupported("video/webm;codecs=vp9") ? "video/webm;codecs=vp9"
                    : (MediaRecorder.isTypeSupported("video/webm;codecs=vp8") ? "video/webm;codecs=vp8" : "video/webm");
                rec = new MediaRecorder(stream, { mimeType: mime, videoBitsPerSecond: 2500000 });
                recChunks = [];
                rec.ondataavailable = function (e) { if (e.data && e.data.size) recChunks.push(e.data); };
                rec.onstop = function () {
                    var blob = new Blob(recChunks, { type: "video/webm" });
                    var a = document.createElement("a");
                    a.href = URL.createObjectURL(blob);
                    a.download = "remote-hub-nagranie-" + Date.now() + ".webm";
                    a.click();
                    setTimeout(function () { URL.revokeObjectURL(a.href); }, 8000);
                    rec = null;
                    btnRec.classList.remove("active");
                    btnRec.innerHTML = "&#9210; Nagrywaj wideo";
                    toast("Nagranie zapisane (" + Math.round((Date.now() - recStart) / 1000) + " s)");
                };
                rec.start(1000);
                recStart = Date.now();
                btnRec.classList.add("active");
                btnRec.innerHTML = "&#9209; STOP - zapisz";
                toast("Nagrywanie wlaczone - zapisuje obraz z telefonu");
            } catch (e) {
                rec = null;
                toast("Nagrywanie niedostepne w tej przegladarce");
            }
        });

        // ------------------------------------------------------------------
        // Szybki tekst + schowek
        // ------------------------------------------------------------------
        function sendQuickTxt(where) {
            var v = document.getElementById("quickTxt").value;
            if (!v) { toast("Wpisz najpierw tekst"); return; }
            if (send(where === "clip" ? { t: "clip", s: v } : { t: "text", s: v })) {
                toast(where === "clip" ? "Skopiowano do schowka telefonu" : "Wyslano na ekran telefonu");
                if (where !== "clip") document.getElementById("quickTxt").value = "";
            } else toast("Brak polaczenia");
        }
        document.getElementById("btnSendTxt").addEventListener("click", function () { sendQuickTxt("text"); });
        document.getElementById("btnClip").addEventListener("click", function () { sendQuickTxt("clip"); });
        document.getElementById("quickTxt").addEventListener("keydown", function (e) {
            if (e.key === "Enter") sendQuickTxt("text");
        });
        document.getElementById("btnPasteHint").addEventListener("click", function () {
            toast("Na telefonie: przytrzymaj palec w polu -> Wklej");
        });

        // ------------------------------------------------------------------
        // Aplikacje
        // ------------------------------------------------------------------
        function requestApps() {
            appsRequestedAt = Date.now();
            if (!send({ t: "apps" })) { toast("Brak polaczenia z serwerem"); appsRequestedAt = 0; return; }
            document.getElementById("appsMeta").textContent = "...pobieranie...";
            toast("Pobieram liste aplikacji z telefonu");
            setTimeout(function () {
                if (appsRequestedAt && !appsList) {
                    appsRequestedAt = 0;
                    document.getElementById("appsMeta").textContent = "";
                    toast("Telefon nie odpowiedzial (apka 1.5 + sterowanie wlaczone)");
                }
            }, 5000);
        }
        document.getElementById("btnAppsRefresh").addEventListener("click", function () {
            appsList = null; requestApps();
        });
        document.getElementById("btnAppsHome").addEventListener("click", function () {
            send({ t: "home" }); toast("Home"); gotoTab("tabPodglad");
        });
        function renderApps() {
            var grid = document.getElementById("appsGrid");
            var meta = document.getElementById("appsMeta");
            if (!appsList || !appsList.length) {
                meta.textContent = "";
                grid.innerHTML = '<div class="empty" style="grid-column:1/-1;"><span class="ic">&#128241;</span>' +
                    "Telefon nie odeslal zadnej listy.<br><span style='font-size:11.5px;'>" +
                    "Wymaga apki 1.5 i wlaczonego zdalnego sterowania.</span></div>";
                return;
            }
            meta.textContent = appsList.length + " szt.";
            var html = "";
            appsList.forEach(function (a) {
                var h = hue(a.p || a.l);
                html += '<div class="apptile" data-pkg="' + a.p + '" title="' + a.p + '">' +
                    '<div class="av" style="background:linear-gradient(135deg,hsl(' + h + ",70%,55%),hsl(" + (h + 55) + ',70%,45%));">' +
                    initials(a.l) + '</div><div class="an">' + a.l + "</div></div>";
            });
            grid.innerHTML = html;
            grid.querySelectorAll(".apptile").forEach(function (el) {
                el.addEventListener("click", function () {
                    if (send({ t: "launch", p: el.dataset.pkg })) {
                        toast("Uruchamiam: " + (el.querySelector(".an").textContent));
                        gotoTab("tabPodglad");
                    } else toast("Brak polaczenia");
                });
            });
        }

        // ------------------------------------------------------------------
        // Skroty klawiszowe
        // ------------------------------------------------------------------
        window.addEventListener("keydown", function (e) {
            if (e.target.tagName === "SELECT" || e.target.tagName === "INPUT" || e.target.tagName === "TEXTAREA") return;
            if (kbdMode) {
                if (e.key === "Escape") { setKbd(false); e.preventDefault(); return; }
                if (e.ctrlKey || e.metaKey || e.altKey) return;
                var kk = e.key;
                if (kk === "Backspace" || kk === "Enter" || kk === "Tab" || (kk.length === 1 && !e.ctrlKey && !e.metaKey)) {
                    if (send({ t: "key", k: kk })) e.preventDefault();
                    else toast("Brak polaczenia z telefonem");
                }
                return;
            }
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
                    if "bat" in obj:
                        c["bat"] = obj.get("bat")
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
                else:
                    # Odpowiedzi telefonu (stat, apps, wyniki komend) -> przegladarki.
                    if "bat" in obj:
                        c["bat"] = obj.get("bat")
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
