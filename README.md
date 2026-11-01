# UL 801 – Update V4

Dieses Paket aktualisiert das bestehende GitHub-Repository `ul801-live-v3`. **Nur `server.js` und `public/index.html` ersetzen.** `package.json`, API-Keys und Render-Service bleiben unverändert.

## Neu
- Geplante Ankunftszeiten an den nächsten Haltestellen (keine Echtzeitprognose).
- Geolokalisierte Haltestellen mit Popups.
- Blaue Linie aus tatsächlich beobachteten GPS-Punkten seit dem Start des Render-Servers, maximal 3 Stunden / 300 Punkte pro Fahrt. Es gibt ohne Speicherung vor Serverstart keine historische Fahrtspur.
- Wählbares Abfrageintervall: 45 Sek (Standard), 20 Sek schnell, 90 Sek sparsam. Der Server cached Echtzeitdaten für 20 Sek.
- GPS-Zeitstempel, Warnung bei Verbindungsfehlern.

## Trafiklab-Kontingent
Bronze Realtime: 30.000 Requests pro Monat. Dauerbetrieb alle 20 Sekunden würde das überschreiten. Im Browser nur bei sichtbarem Tab aktive Abfragen; Browser-Tabs kombinieren sich trotzdem zu stärkerem Traffic.

## Deployment
1. Auf GitHub `server.js` im Repo-Hauptverzeichnis ersetzen.
2. `public/index.html` im Unterordner `public` ersetzen.
3. Commit. Render startet automatisch, wenn Auto-Deploy eingeschaltet ist.
4. `/api/health` und `/api/vehicles` prüfen.
