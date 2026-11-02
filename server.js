import express from 'express';
import { createWriteStream } from 'node:fs';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';
import yauzl from 'yauzl';
import { parse } from 'csv-parse';
import GtfsRealtimeBindings from 'gtfs-realtime-bindings';

const app = express();
const port = process.env.PORT || 3000;
const staticKey = process.env.TRAFIKLAB_STATIC_KEY;
const realtimeKey = process.env.TRAFIKLAB_REALTIME_KEY;
const base = 'https://opendata.samtrafiken.se';
app.use(express.static(path.join(process.cwd(), 'public')));

let schedule = null, lastStatic = 0, staticPromise = null;
let latest = null, lastLive = 0, livePromise = null;
let cachedPredictions = new Map(), predictionsAt = 0, predictionsPending = null;
// Recent positions observed by THIS running server, not a historical UL feed.
const trails = new Map();
const MAX_TRAIL = 300;
const MAX_AGE_MS = 3 * 60 * 60 * 1000;
function recordTrail(vehicleId, tripId, lat, lon, timestamp) {
  const now = Date.now();
  const key = String(vehicleId) + ':' + String(tripId);
  let points = trails.get(key) || [];
  points = points.filter(p => now - p.t < MAX_AGE_MS);
  const t = timestamp ? Date.parse(timestamp) : now;
  const last = points.at(-1);
  if (Number.isFinite(t) && t <= now + 60000 && (!last || (t > last.t && (Math.abs(lat-last.lat)>0.00001 || Math.abs(lon-last.lon)>0.00001)))) points.push({lat,lon,t});
  if (points.length > MAX_TRAIL) points = points.slice(-MAX_TRAIL);
  trails.set(key, points);
  if (trails.size > 300) for(const [k,pts] of trails) if (!pts.length || now - pts.at(-1).t > MAX_AGE_MS) trails.delete(k);
  return points.map(({lat,lon,t}) => ({lat,lon,timestamp:new Date(t).toISOString()}));
}
const yesterday = 23 * 60 * 60 * 1000;

async function downloadFile(url, filename) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 60000);
  try {
    const response = await fetch(url, { signal: controller.signal, headers: { 'User-Agent': 'UL801Live/2.0' } });
    if (!response.ok) throw new Error(`Trafiklab static HTTP ${response.status}`);
    if (!response.body) throw new Error('Trafiklab lieferte keine Datei');
    await pipeline(Readable.fromWeb(response.body), createWriteStream(filename));
  } finally { clearTimeout(timer); }
}
function openZip(file) {
  return new Promise((resolve, reject) => yauzl.open(file, { lazyEntries: true, autoClose: true }, (err, zip) => err ? reject(err) : resolve(zip)));
}
async function readGtfsFiles(zipPath, handlers) {
  const zip = await openZip(zipPath);
  return new Promise((resolve, reject) => {
    let finished = false;
    const fail = err => { if (!finished) { finished = true; zip.close(); reject(err); } };
    zip.on('error', fail);
    zip.on('end', () => { if (!finished) { finished = true; resolve(); } });
    zip.on('entry', entry => {
      const name = path.posix.basename(entry.fileName).toLowerCase();
      const handler = handlers[name];
      if (!handler) { zip.readEntry(); return; }
      zip.openReadStream(entry, async (err, stream) => {
        if (err) return fail(err);
        try {
          const parser = stream.pipe(parse({ columns: true, bom: true, skip_empty_lines: true, relax_quotes: true }));
          for await (const row of parser) handler(row);
          zip.readEntry();
        } catch (e) { fail(e); }
      });
    });
    zip.readEntry();
  });
}
async function loadSchedule() {
  if (schedule && Date.now() - lastStatic < yesterday) return schedule;
  if (staticPromise) return staticPromise;
  staticPromise = (async () => {
    const directory = await mkdtemp(path.join(tmpdir(), 'ul801-'));
    const filename = path.join(directory, 'ul.zip');
    try {
      await downloadFile(`${base}/gtfs/ul/ul.zip?key=${encodeURIComponent(staticKey)}`, filename);
      const routeIds = new Set(), tripMap = new Map(), stopTimes = new Map(), stops = new Map();
      // The ZIP may contain stop_times before trips; make four bounded passes.
      // Critically, never materialize an entire GTFS CSV into RAM.
      await readGtfsFiles(filename, { 'routes.txt': r => { if (r.route_short_name?.trim() === '801') routeIds.add(r.route_id); } });
      if (!routeIds.size) throw new Error('Route 801 not found in current GTFS');
      await readGtfsFiles(filename, { 'trips.txt': t => { if (routeIds.has(t.route_id)) tripMap.set(t.trip_id, { trip_headsign: t.trip_headsign, direction_id: t.direction_id, shape_id: t.shape_id }); } });
      const usedStops = new Set();
      await readGtfsFiles(filename, { 'stop_times.txt': s => {
        if (!tripMap.has(s.trip_id)) return;
        if (!stopTimes.has(s.trip_id)) stopTimes.set(s.trip_id, []);
        stopTimes.get(s.trip_id).push({ seq: Number(s.stop_sequence), stopId: s.stop_id, arrival: s.arrival_time });
        usedStops.add(s.stop_id);
      }});
      for (const arr of stopTimes.values()) arr.sort((a, b) => a.seq - b.seq);
      await readGtfsFiles(filename, { 'stops.txt': s => { if (usedStops.has(s.stop_id)) stops.set(s.stop_id, { name: s.stop_name, lat: Number(s.stop_lat), lon: Number(s.stop_lon) }); } });
      // GTFS shape coordinates are the planned road-following route. Read only shapes
      // used by route 801, with strict per-shape and global memory caps.
      const shapeUsage = new Map();
      for (const trip of tripMap.values()) if (trip.shape_id) shapeUsage.set(trip.shape_id, (shapeUsage.get(trip.shape_id)||0)+1);
      const wantedShapes = new Set([...shapeUsage].sort((a,b)=>b[1]-a[1]).slice(0,40).map(x=>x[0]));
      const shapes = new Map();
      let totalShapePoints = 0;
      await readGtfsFiles(filename, { 'shapes.txt': r => {
        if (!wantedShapes.has(r.shape_id)) return;
        let arr = shapes.get(r.shape_id);
        if (!arr) { arr = []; shapes.set(r.shape_id, arr); }
        if (arr.length >= 4500 || totalShapePoints >= 95000) return;
        const lat = Number(r.shape_pt_lat), lon = Number(r.shape_pt_lon);
        if (!Number.isFinite(lat) || !Number.isFinite(lon)) return;
        arr.push([Number(lat.toFixed(5)), Number(lon.toFixed(5))]);
        totalShapePoints++;
      }});
      console.log(`UL 801 planned shape lines loaded: ${shapes.size}, points: ${totalShapePoints} (shape cap 4500, global cap 95000)`);
      const result = { tripMap, stopTimes, stops, shapes };
      schedule = result; lastStatic = Date.now();
      console.log(`UL GTFS loaded: ${routeIds.size} routes, ${tripMap.size} trips, ${usedStops.size} stops`);
      return result;
    } finally { await rm(directory, { recursive: true, force: true }); }
  })();
  try { return await staticPromise; } finally { staticPromise = null; }
}
function nearestStop(data, route, lat, lon) {
  let best = Infinity, index = 0;
  route.forEach((r, i) => { const p = data.stops.get(r.stopId); if (!p) return; const d = (p.lat - lat) ** 2 + ((p.lon - lon) * .62) ** 2; if (d < best) { best = d; index = i; } });
  return index;
}
async function downloadRealtime() {
  const controller = new AbortController(); const timer = setTimeout(() => controller.abort(), 20000);
  try {
    const r = await fetch(`${base}/gtfs-rt/ul/VehiclePositions.pb?key=${encodeURIComponent(realtimeKey)}`, { signal: controller.signal });
    if (!r.ok) throw new Error(`Trafiklab realtime HTTP ${r.status}`);
    return Buffer.from(await r.arrayBuffer());
  } finally { clearTimeout(timer); }
}

// GTFS-RT TripUpdates: an actual reported/estimated stop time, not a fabricated delay.
// Feed failure must never prevent vehicle positions from loading.
async function loadPredictions() {
  if (Date.now() - predictionsAt < 20000) return cachedPredictions;
  if (predictionsPending) return predictionsPending;
  predictionsPending = (async () => {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 9000);
    try {
      const r = await fetch(`${base}/gtfs-rt/ul/TripUpdates.pb?key=${encodeURIComponent(realtimeKey)}`, {signal:controller.signal});
      if (!r.ok) throw Error(`TripUpdates HTTP ${r.status}`);
      const feed = GtfsRealtimeBindings.transit_realtime.FeedMessage.decode(Buffer.from(await r.arrayBuffer()));
      const found = new Map();
      for (const e of feed.entity) {
        const tu = e.tripUpdate, id = tu?.trip?.tripId;
        if (!id) continue;
        const stops = new Map();
        for (const st of tu.stopTimeUpdate || []) {
          const event = st.arrival || st.departure;
          const epoch = event?.time == null ? null : Number(event.time.toString());
          const delay = event?.delay == null ? null : Number(event.delay);
          if (Number.isFinite(epoch) && epoch > 0) {
            const obj = {expected: new Date(epoch*1000).toISOString(), delaySeconds: Number.isFinite(delay)?delay:null};
            if (st.stopSequence != null) stops.set(`seq:${st.stopSequence}`, obj);
            if (st.stopId) stops.set(`id:${st.stopId}`, obj);
          }
        }
        found.set(id, stops);
      }
      cachedPredictions = found; predictionsAt = Date.now();
      return found;
    } catch(err) {
      console.warn('TripUpdates not available (schedule-only fallback):', err.message);
      predictionsAt = Date.now();
      return cachedPredictions;
    } finally { clearTimeout(timer); }
  })();
  try { return await predictionsPending; } finally { predictionsPending = null; }
}

async function loadLive() {
  if (latest && Date.now() - lastLive < 20000) return latest;
  if (livePromise) return livePromise;
  livePromise = (async () => {
    const data = await loadSchedule();
    const feed = GtfsRealtimeBindings.transit_realtime.FeedMessage.decode(await downloadRealtime());
    const buses = [];
    const forecasts = await loadPredictions();
    for (const e of feed.entity) {
      const v = e.vehicle, lat = v?.position?.latitude, lon = v?.position?.longitude;
      const tripId = v?.trip?.tripId;
      if (!Number.isFinite(lat) || !Number.isFinite(lon) || !tripId) continue;
      const trip = data.tripMap.get(tripId);
      if (!trip) continue;
      const times = data.stopTimes.get(tripId) || [];
      let ix = v.currentStopSequence ? times.findIndex(s => s.seq >= Number(v.currentStopSequence)) : -1;
      if (ix < 0) ix = nearestStop(data, times, lat, lon);
      const dest = trip.trip_headsign || data.stops.get(times.at(-1)?.stopId)?.name || 'Okänt';
      const enrich = s => {
        const stop = data.stops.get(s.stopId);
        const prediction = forecasts.get(tripId)?.get(`seq:${s.seq}`) || forecasts.get(tripId)?.get(`id:${s.stopId}`) || null;
        return { name: stop?.name || s.stopId, scheduled: s.arrival, expected: prediction?.expected ?? null, delaySeconds: prediction?.delaySeconds ?? null, lat: stop?.lat ?? null, lon: stop?.lon ?? null, sequence: s.seq };
      };
      const nextStops = times.slice(ix).map(enrich);
      const passedStops = times.slice(0, ix).map(enrich);
      const timestamp = v.timestamp ? new Date(Number(v.timestamp.toString()) * 1000).toISOString() : null;
      const id = v.vehicle?.id || e.id;
      const trail = recordTrail(id, tripId, lat, lon, timestamp);
      buses.push({ id, tripId, lat, lon, bearing: v.position.bearing ?? null, direction: trip.direction_id, destination: dest, timestamp, nextStops, passedStops, trail, shape: data.shapes.get(trip.shape_id) || [], routeIsPlanned: true });
    }
    latest = { updated: new Date().toISOString(), buses };
    lastLive = Date.now();
    return latest;
  })();
  try { return await livePromise; } finally { livePromise = null; }
}
app.get('/api/health', (req, res) => res.json({ configured: !!staticKey && !!realtimeKey, staticKeyConfigured: !!staticKey, realtimeKeyConfigured: !!realtimeKey, staticLoaded: !!schedule, lastLive: lastLive ? new Date(lastLive).toISOString() : null }));
app.get('/api/vehicles', async (req, res) => {
  res.set('Cache-Control', 'no-store');
  if (!staticKey || !realtimeKey) return res.status(503).json({ error: 'Missing Trafiklab keys', buses: [] });
  try { res.json(await loadLive()); }
  catch (e) { console.error('Vehicle loading:', e); res.status(503).json({ error: e.message, buses: [], lastSuccess: latest?.updated || null }); }
});
app.listen(port, '0.0.0.0', () => console.log(`UL 801 listening on ${port}`));
