# Remote Hub – serwer podglądu

Serwer FastAPI, który odbiera obraz z telefonu (aplikacja **Remote Control**)
przez WebSocket i przekazuje go do przeglądarki pod adresem:

**https://remote-hub-server-ojx3.onrender.com**

## Pliki

| Plik | Opis |
|------|------|
| `server.py` | Serwer: strona podglądu (HTTP) + relay WebSocket + `/health` |
| `Procfile` | Polecenie startu dla Render |
| `requirements.txt` | Zależności (fastapi, uvicorn, websockets) |

## Co zostało naprawione (v2)

1. **Błąd relayu** – `WebSocket.send()` w Starlette wymaga `send_text`/`send_bytes`.
   Stary kod przerywał przekazywanie klatek i wyrzucał zdrowych klientów.
2. **Martwe połączenia** – nieudana wysyłka do zamkniętej karty powodowała wyrzucenie
   *zdrowego* telefonu z serwera (teraz kasowani są tylko faktycznie martwi klienci).
3. **`Procfile`** – `python server.py` nie startował serwera (brak `uvicorn.run`),
   teraz działa także `python server.py` lokalnie.
4. **`/health`** – endpoint diagnostyczny (liczba klientów i telefonów).
5. **Strona podglądu** – status serwera/telefonu, FPS, rozdzielczość, automatyczne
   ponawianie połączenia bez masowego odświeżania, przyciski **Wstecz/Home**,
   **pełny ekran** i **kliknięcie w obraz = dotknięcie ekranu telefonu**.
6. **Wsparcie obu formatów klatek** – binarne JPEG i tekst base64 (kompatybilność
   ze starą aplikacją).

## Nowe w v3 – wiele telefonów i sterowanie (20.10.2026)

1. **Lista telefonów po modelu** – aplikacja wysyła `hello` z stabilnym ID i modelem
   (np. `samsung SM-G991B`); strona ma rozwijaną listę wyboru urządzenia
   (opcja „Automatycznie" = podgląd najaktywniejszego telefonu).
2. **Routing klatek i komend** – podglądaskana widzi tylko wybrany telefon;
   klik/swipe/trafi tylko do niego (osobny telefon nie „podsłuchuje" komend).
   Endpoint `GET /phones` zwraca listę urządzeń (JSON).
3. **Zdalna jakość transmisji** – suwaki na stronie wysyłają `cfg` (FPS, jakość JPEG,
   maks. rozdzielczość); apka przełącza się w locie, bez restartu.
4. **Nowe komendy** – `swipe` (przeciągnięcie), `lock` (blokada ekranu, Android 9+),
   `power` (menu zasilania) obok tap/back/home/recents.
5. **Statystyki telefonu** – apka raportuje co 5 s FPS/wymiary/jakość, strona je wyświetla.
6. **Odporność** – keepalive, backoff na obu stronach, bezpieczne czyszczenie
   martwych klientów, `since` liczony od połączenia (nie epoch przy pierwszej klatce),
   kolejność „routing przed timestampem" (tryb auto nie miesza klatek dwóch telefonów).
   Test end-to-end: **11/11 OK** (dwa telefony, routing FPS, tap/cfg, health).

## Wdrożenie na Render

1. Wejdź na https://github.com/barsta123/remote-hub → zakładka **Code → Upload files**
   i wgraj 3 pliki z tego folderu (nadpisując stare) – albo `git push`, jeśli masz skonfigurowane konto.
2. Render przy każdym pushu na gałąź `main` automatycznie przebudowuje i wdraża usługę.
3. Sprawdź: https://remote-hub-server-ojx3.onrender.com/health

Ręcznie (bez Rendera):

```bash
pip install -r requirements.txt
python server.py            # http://localhost:8000
```

## Bezpieczeństwo

Podgląd **nie jest chroniony hasłem** – każdy, kto zna adres URL, zobaczy ekran
telefonu i może go obsługiwać. Traktuj adres jak tajny. Jeśli chcesz, mogę dodać
kod dostępu (np. `?klucz=...`).
