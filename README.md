# UL 801 Live – Neuinstallation

Unbedingt alle Dateien und Ordner in die Wurzel des GitHub-Repositories hochladen. Erwartete Struktur:

- server.js
- package.json
- render.yaml
- public/index.html

Der Ordner `public` und dessen `index.html` dürfen NICHT fehlen. In GitHub bitte prüfen, dass `public/index.html` mehrere KB groß und nicht leer ist.

Render: Web Service / Node / Build command `npm install` / Start command `npm start` / Root Directory leer.

Umgebungsvariablen:

- `TRAFIKLAB_STATIC_KEY` = API-Key für GTFS Regional Static data
- `TRAFIKLAB_REALTIME_KEY` = API-Key für GTFS Regional Realtime

URLs zum Testen:

- `/` zeigt die Webseite
- `/api/health` zeigt, ob beide Keys konfiguriert wurden
- `/api/vehicles` zeigt Daten oder eine konkrete Fehlermeldung

Geheime API-Keys niemals auf GitHub hochladen oder im Chat teilen.

Hinweis: Trafiklab-Datenfeed mit privaten Schlüsseln wurde nicht Ende-zu-Ende getestet. Nur registrierte Live-Fahrzeuge werden angezeigt, kein künstlicher Bus, wenn keine Daten vorhanden sind.
