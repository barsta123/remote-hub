import asyncio
import websockets
import os

connected_devices = set()

async def handler(websocket):
    connected_devices.add(websocket)
    print(f"[+] Nowe urządzenie połączone. Razem online: {len(connected_devices)}")
    try:
        async for message in websocket:
            # Przekazywanie wiadomości/strumienia do pozostałych połączonych
            for client in connected_devices:
                if client != websocket:
                    await client.send(message)
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        connected_devices.remove(websocket)
        print(f"[-] Urządzenie rozłączyło się. Razem online: {len(connected_devices)}")

async def main():
    port = int(os.environ.get("PORT", 8080))
    async with websockets.serve(handler, "0.0.0.0", port):
        print(f"[*] Serwer przekaźnikowy ruszył na porcie {port}")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
