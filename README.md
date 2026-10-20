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

## Nowe w v6 – wersja 1.5: pisanie, aplikacje, piękniejszy panel (20.10.2026)

1. **PIN domyślnie włączony: 4949** – panel jest chroniony od razu po deployu,
   bez konfigurowania zmiennych. Zmienna `ACCESS_PIN` nadal działa (inny kod),
   a `ACCESS_PIN=""` wyłącza PIN (jak w v1–v4). Szczegóły w sekcji „Bezpieczeństwo".
2. **Redesign panelu „2027”** – ciemny motyw glass/gradienty, pill-tabs, cztery
   zakładki: **Podgląd / Aplikacje / Telefony / Ustawienia**.
3. **Pisanie z klawiatury komputera** – przycisk **⌨ Pisanie** i tryb pisania:
   co wpiszesz na klawiaturze PC (litery, spacja, Enter, Backspace, Tab) trafia
   na pole tekstowe telefonu na żywo. Esc wyłącza tryb.
4. **Lista aplikacji** – zakładka Aplikacje pobiera z telefonu siatkę ikonek
   (do 150 aplikacji) i uruchamia je jednym kliknięciem na telefonie.
5. **Bateria telefonu** – telefon raportuje poziom baterii (hello/stat),
   widoczny jako chip przy nazwie i w tabeli telefonów; apka pokazuje ją też
   w powiadomieniu usługi (żywy status: „Połączony · bateria 87%").
6. **Nowe sterowanie myszą**:
   - **podwójne kliknięcie** = double-tap (zoom w galerii, otwieranie linków),
   - **kółko myszy** = przewijanie strony telefonu,
   - **prawy przycisk myszy** = Wstecz,
   - **Ctrl + kółko** = zoom obrazu podglądu, **Shift + przeciągnięcie** = przesuwanie,
   - ripple na kliku – widać, gdzie trafił dotyk.
7. **Jakość FPS w presetach** – Niski 8 / Średni 15 (domyślny) / Wysoki 24 /
   Ultra 30; apka nadaje do 30 FPS (dawniej limit 10).
8. **Nagrywanie wideo** – przycisk „⏺ Nagrywaj wideo” w panelu Sesja nagrywa
   obraz z telefonu i zapisuje plik `.webm` na komputerze.
9. **Wykrywanie zgaszonego ekranu** – gdy telefon przez chwilę wysyła niemal
   czarny obraz, panel wyświetla chip „💤 Ekran zgaszony".
10. **Lista telefonów: „Aktywne teraz”** – rozwijane menu dzieli telefony na
    aktywne (online, na górze) i zapisane (offline); serwer odświeża listę co 5 s.
11. **Fix i płynność ekranu** – obraz dopasowany do kontenera (object-fit +
    korekta współrzędnych gestów), brak ucinania i lagów, brak ciężkich efektów
    CSS (backdrop-filter), brak dekodowania klatek na ukrytej karcie.
12. **APK 1.5 (versionCode 6)** – poza sterowaniem z panelu: wysyła baterię,
    obsługuje `dblclick`, `key`, `text`, `clip` (schowek), `apps`, `launch`;
    ciemny motyw „Remote Hub” w aplikacji. Testy serwera: **37/37**.

## Nowe w v7 – wersja 1.6: koniec migania, szybsze klatki, dźwięk (20.10.2026)

Naprawa trzech głównych problemów (miganie ekranu, zacięcia/mało FPS, brak
dzwięku) + poprawki pisania i listy aplikacji:

1. **Koniec migania na czarno** – klatki przestają być przerywane w połowie
   dekodowania: nowa klatka czeka, aż poprzednia się zdekoduje („load-gated"),
   a stare adresy URL zwalniane są dopiero po pełnym załadowaniu nowszego
   obrazu. Każda wyświetlona klatka jest kompletna – zero blysków czerni.
2. **Binarne klatki JPEG** – aplikacja wysyła surowe bajty zamiast base64:
   ~25% mniej transferu i CPU (base64 zjadło ~połowę czasu klatki na slabym
   telefonie). Starsze apki (base64) nadal działają – panel obsługuje oba formaty.
3. **Zero alokacji na klatkę w apce** – bitmapy i strumień JPEG są wielokrotnie
   używane (2–4 MB na klatkę = skoki GC = zacięcia). Efekt: realny wzrost FPS
   o ~30–100% przy mniejszych opóźnieniach.
4. **Dźwięk z telefonu na żywo** – przycisk „🔊 Dźwięk” w panelu Sesja:
   telefon przechwytuje dźwięk odtwarzany w aplikacjach (YouTube, gry, filmy;
   Android 10+, wymagane uprawnienie do nagrywania) i strumieniuje go do
   przeglądarki (PCM 32 kHz, jitter-buffer ~120 ms). Suwak głośności.
   Uwaga: DRM (Netflix itp.) i dźwięki systemowe mogą być blokowane przez system.
5. **Szybkie pisanie bez lagów** – znaki z klawiatury PC lecą porcjami co 55 ms
   (jedna komenda `keys` z całą paczką zamiast potoku klawiszy) + podgląd
   wpisywanego tekstu pod ekranem (natychmiastowa informacja zwrotna).
6. **Błędy zamiast cichej awarii** – gdy usługa zdalnego sterowania jest
   wyłączona lub brak pola tekstowego, panel pokazuje komunikat (tekst trafia
   wtedy do schowka telefonu).
7. **Lista aplikacji naprawiona** – Android 11+ ukrywał listę bez wpisu
   `<queries>`/`QUERY_ALL_PACKAGES` w manifeście (była pusta) – dodane.
8. **Domyślny preset „Bardzo szybka” (24 FPS / 1280 px)** + lekkie obniżenie
   jakości JPEG w presetach (mniejsze pliki = mniej lagów przy tych samych FPS).
9. Testy: **39/39** (routing `keys`/`audio` + pełna regresja).
   APK: **v1.6** (versionCode 7).


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

Od v6 panel jest **chroniony domyślnym kodem 4949** – nie trzeba nic konfigurować.

1. Po wejściu na panel wpisz PIN **4949** (okno logowania). Niezalogowani nie
   dostają klatek ani nie sterują niczym; po 4 złych próbach są rozłączani.
2. W apce (sekcja „Kod dostępu do panelu”) wpisz ten sam PIN **4949** – telefon
   bez poprawnego kodu serwer odrzuca. Porównanie przez `hmac.compare_digest`.
3. **Swój własny kod:** na Render ustaw `ACCESS_PIN` (np. `4711`) w
   **Environment → + Add Environment Variable**; Render przebuduje usługę (~100 s),
   a Ty wpiszesz nowy kod i w apce, i w panelu.
4. **Panel otwarty (bez PIN-a)** – ustaw `ACCESS_PIN=""` (pusta wartość);
   wróci wtedy tryb jak w v1–v4 (ale apka 1.5 może wtedy łączyć się bez kodu).
5. Bez PIN-u nadal obowiązuje: traktuj adres URL jak tajny (każdy, kto go zna,
   widzi ekran i steruje telefonem).
