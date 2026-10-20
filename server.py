from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

app = FastAPI()

# HTML panelu podglądu serwowany bezpośrednio pod głównym adresem strony
html_content = """
<!DOCTYPE html>
<html lang="pl">
<head>
    <meta charset="UTF-8">
    <title>Panel Master - Zdalne Urządzenia</title>
    <style>
        body {
            background-color: #0d1117;
            color: #c9d1d9;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            margin: 0;
            padding: 20px;
            display: flex;
            flex-direction: column;
            align-items: center;
        }
        h1 {
            color: #58a6ff;
            margin-bottom: 10px;
        }
        #status {
            margin-bottom: 20px;
            font-size: 16px;
            color: #8b949e;
        }
        .device-card {
            background-color: #161b22;
            border: 1px solid #30363d;
            border-radius: 8px;
            padding: 15px;
            width: 360px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.5);
        }
        .device-header {
            font-size: 14px;
            font-weight: bold;
            margin-bottom: 10px;
            color: #8b949e;
            display: flex;
            justify-content: space-between;
        }
        .screen-container {
            width: 100%;
            background-color: #000;
            border-radius: 4px;
            overflow: hidden;
            display: flex;
            align-items: center;
            justify-content: center;
            height: 640px;
        }
        img {
            width: 100%;
            height: auto;
            display: block;
        }
    </style>
</head>
<body>

    <h1>Panel Master - Podgląd na żywo</h1>
    <div id="status">Oczekiwanie na połączenie z urządzenia...</div>
    
    <div class="device-card">
        <div class="device-header">
            <span>Status transmisji</span>
            <span style="color: #238636;">● Aktywny</span>
        </div>
        <div class="screen-container">
            <img id="screenView" alt="Brak obrazu z telefonu..." />
        </div>
    </div>

    <script>
        const statusDiv = document.getElementById("status");
        const screenView = document.getElementById("screenView");

        // Automatyczne wykrywanie protokołu ws/wss na podstawie adresu strony
        const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
        const wsUrl = protocol + "//" + window.location.host;
        const ws = new WebSocket(wsUrl);

        ws.onopen = function() {
            statusDiv.innerText = "Połączono z serwerem. Czekam na obraz z telefonu...";
        };

        ws.onmessage = function(event) {
            // Odebrany Base64 trafia bezpośrednio jako obrazek
            screenView.src = "data:image/jpeg;base64," + event.data;
            statusDiv.innerText = "Streaming aktywny (Na żywo)";
        };

        ws.onclose = function() {
            statusDiv.innerText = "Rozłączono z serwerem. Odśwież stronę...";
            setTimeout(() => { location.reload(); }, 3000);
        };

        ws.onerror = function(error) {
            statusDiv.innerText = "Błąd połączenia WebSocket.";
        };
    </script>

</body>
</html>
"""

# Zbiór przechowujący aktywne połączenia (telefon i przeglądarka)
connected_clients = set()

@app.get("/")
async def get_dashboard():
    return HTMLResponse(content=html_content, status_code=200)

@app.websocket("/")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.add(websocket)
    try:
        while True:
            # Odbieramy dane (np. klatkę z telefonu) i przesyłamy dalej do podglądu w przeglądarce
            data = await websocket.receive_text()
            for client in connected_clients:
                if client != websocket:
                    await client.send_text(data)
    except WebSocketDisconnect:
        connected_clients.remove(websocket)
