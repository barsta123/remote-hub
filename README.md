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
2. **Routing klatek i komend** – przeglądarka widzi tylko wybrany telefon;
   klik/swipe trafia tylko do niego (osobny telefon nie „podsłuchuje" komend).
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

## Nowe w v4 – caly spis telefonów (20.10.2026)

1. **Menu wyboru = kazdy telefon z apką** – serwer zapamietuje kazde urzadzenie,
   ktore kiedykolwiek sie polaczylo (plik `phones.json` obok `server.py`).
   Na liscie widac takze **offline** telefony z czasem ostatniego widzenia
   („widziany X min temu"). Po restarcie/deployu lista sie wczytuje.
2. **Nazwy wlasne** – przycisk „Zmien nazwe wybranego telefonu" na stronie
   (np. „Telefon taty"); nazwa zapisywana po stronie serwera i widoczna
   w menu kazdej przegladarki.
3. **Komenda `rename`** – `{"t":"rename","id":"...","name":"..."}` (bezposrednio tez
   przez WS lub przycisk na stronie).
   Testy: **8/8 OK** (polaczenie, nazwa, persistencja, offline w menu).

## Nowe w v5 – zakładki, PIN i duperele (20.10.2026)

1. **Trzy zakładki panelu** zamiast jednej strony:
   - **Podgląd** – ekran, sterowanie, jakość transmisji, wykres FPS/transferu na żywo;
   - **Telefony** – tabela wszystkich apek (model, nazwa, online/offline, „widziany X
     temu"), przyciski **Obserwuj** i **Nazwa**, wybór „Automatycznie";
   - **Ustawienia** – PIN panelu, statystyki sesji (czas, klatki, MB, średnie FPS),
     log zdarzeń, informacje o serwerze i skróty klawiszowe.
2. **Kod dostępu do panelu (PIN)** – zmienna środowiskowa `ACCESS_PIN` (szczegóły
   w sekcji „Bezpieczeństwo"):
   - bez niej panel działa otwarty (jak v1–v4),
   - z nią przeglądarka pyta o kod (okno PIN); niezalogowani nie dostają klatek
     ani nie sterują niczym, po 4 złych próbach są rozłączani,
   - apka 1.4 wysyła ten sam kod w `hello` (pole PIN w panelu apki) – telefon bez
     poprawnego kodu serwer odrzuca,
   - porównanie przez `hmac.compare_digest`, 20 s na zalogowanie.
3. **Log zdarzeń** – połączenia/rozłączenia telefonów i próby logowania (ostatnie 40),
   widoczny w zakładce Ustawienia oraz w `GET /api/state`.
4. **Nowe gesty i akcje**:
   - **przytrzymanie** – przytrzymaj kursor/palec 0,55 s nad obrazem = long-press
     na telefonie (apka wykonuje gest 850 ms – menu kontekstowe, zaznaczanie),
   - **Zrzut na telefonie** – komenda `screenshot` robi zrzut ekranu *na telefonie*
     (zapis w galerii, Android 9+).
5. **Ekran nie gaśnie podczas transmisji** – serwis trzyma `SCREEN_BRIGHT_WAKE_LOCK`
   dopóki leci obraz, a otwarty panel apki ma `FLAG_KEEP_SCREEN_ON`.
6. **Statystyki na żywo** – sparkline FPS i KB/s (ostatnie 90 s) + licznik sesji.
7. **Nowe endpointy** – `GET /api/state` (pełny stan: telefony, log, tryb PIN).
   Kompatybilność wstecz: stara apka streamuje dalej, gdy `ACCESS_PIN` jest pusty.
   Testy: **25/25** (PIN on/off, auth, odrzuty, routing) + regresja **11/11** (v3)
   i **8/8** (v4). APK: **v1.4** (versionCode 5).

## Instrukcja dla Androida 13+: „Ograniczony dostęp"

Aplikacja zainstalowana spoza Sklepu Play napotka blokade systemu przy probie
wlaczenia uslugi dostepnosci. Obejscie (jednorazowe, ~15 sekund):

1. **Ustawienia → Aplikacje → Remote Control**
2. Kliknij **⋮** (trzy kropki w rogu) → **Zezwol na ograniczone ustawienia**
3. Wroc do apki i kliknij **„Wlacz teraz"** (lub wlacz przełącznik uslugi)
4. W ustawieniach dostepnosci przesun przełącznik **Remote Control**

Aplika prowadzi za reke: okienko przy starcie i panel maja przycisk
„Otwórz ustawienia aplikacji" dokladnie pod ta opcje.

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

## Bezpieczeństwo (kod dostępu / PIN)

Od v5 panel można chronić kodem dostępu:

1. Na Render: **Environment → + Add Environment Variable**
   - Key: `ACCESS_PIN`
   - Value: np. `4711` (swój własny kod)
2. Render przebuduje usługę (~100 s). Sprawdź:
   `https://remote-hub-server-ojx3.onrender.com/health` → pole `"auth": "on"`.
3. **Kolejność ma znaczenie:** najpierw zainstaluj apkę **1.4** i wpisz ten sam PIN
   w panelu apki (sekcja „Kod dostepu do panelu"), *potem* ustaw `ACCESS_PIN`.
   Telefon ze starą apką (bez pola PIN) nie połączy się z zabezpieczonym serwerem.
4. Aby wrócić do otwartego panelu – usuń zmienną `ACCESS_PIN` na Render.
5. Bez PIN-u nadal obowiązuje: traktuj adres URL jak tajny (każdy, kto go zna,
   widzi ekran i steruje telefonem).
