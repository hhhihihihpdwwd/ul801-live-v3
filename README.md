# UL 801 V5 – selektierte gefahrene Route

Bestehendes GitHub/Render Projekt weiterverwenden.

Ersetze `server.js` und `public/index.html`. API-Schlüssel und `package.json` bleiben unverändert.

**Route:** Aus offiziellen GTFS `shapes.txt` wird der planmäßige Straßenverlauf geladen. Nur nachdem man einen Bus ausdrücklich auswählt, erscheint die blaue Linie von Routenbeginn bis zu dessen nächstem Routenpunkt. Dies ist eine **rekonstruierte Strecke**, keine historische GPS-Messung. Keine Linie im Auto-Modus. Bei fehlenden Shape-Daten keine erfundene Strecke.

**Aktualisierung:** Webseite kann alle 5/10/20/45 Sekunden nachsehen; Server cached Trafiklab GPS weiterhin 20 Sekunden, um API-Limits zu schonen. Alle 5 Sekunden ist deshalb **keine neue echte GPS-Position garantiert**.

**Haltestellen:** Fahrplanzeiten, keine Live-Verspätungsprognosen.

**Achtung:** Das zusätzliche Einlesen von `shapes.txt` ist speicherbegrenzt (max. 75.000 Punkte, 80 Shapes) und kann den ersten GTFS-Abruf etwas verlängern. Auf der begrenzten Render-Instanz ohne echte Trafiklab API nicht Ende-zu-Ende getestet.
