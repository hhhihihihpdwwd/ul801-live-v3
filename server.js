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
let startupError = null, lastViewerAt = 0, nextStaticRetry = 0;
const LIVE_MIN_MS = 35000; // Realtime feed: once per ~35 s, regardless of browser refresh rate.
const BACKGROUND_REFRESH_MS = 45000; // Poll only while people are actively using the site.
const STATIC_RETRY_MS = 90000;

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
  if (!schedule && Date.now() < nextStaticRetry) throw new Error(startupError || 'Väntar på nytt försök att läsa tidtabellen');
  staticPromise = (async () => {
    const directory = await mkdtemp(path.join(tmpdir(), 'ul801-'));
    const filename = path.join(directory, 'ul.zip');
    let handedOff = false;
    try {
      console.log('UL 801: warming up the schedule in background');
      await downloadFile(`${base}/gtfs/ul/ul.zip?key=${encodeURIComponent(staticKey)}`, filename);
      const routeIds = new Set(), tripMap = new Map(), stopTimes = new Map(), allStops = new Map();
      // Streaming CSV reading: only route 801 trips and stop times are retained.
      await readGtfsFiles(filename, { 'routes.txt': r => { if (r.route_short_name?.trim() === '801') routeIds.add(r.route_id); } });
      if (!routeIds.size) throw new Error('Linje 801 saknas i GTFS');
      await readGtfsFiles(filename, { 'trips.txt': t => {
        if (routeIds.has(t.route_id)) tripMap.set(t.trip_id, { trip_headsign:t.trip_headsign, direction_id:t.direction_id, shape_id:t.shape_id });
      }});
      const usedStops = new Set();
      // Stops and times in a single pass. Keeping all stop definitions temporarily
      // is cheaper than an additional full pass over the large stop_times CSV.
      await readGtfsFiles(filename, {
        'stop_times.txt': st => {
          if (!tripMap.has(st.trip_id)) return;
          if (!stopTimes.has(st.trip_id)) stopTimes.set(st.trip_id, []);
          stopTimes.get(st.trip_id).push({seq:Number(st.stop_sequence),stopId:st.stop_id,arrival:st.arrival_time});
          usedStops.add(st.stop_id);
        },
        'stops.txt': st => { allStops.set(st.stop_id, {name:st.stop_name,lat:Number(st.stop_lat),lon:Number(st.stop_lon)}); }
      });
      for (const times of stopTimes.values()) times.sort((a,b)=>a.seq-b.seq);
      const stops = new Map();
      for (const id of usedStops) if (allStops.has(id)) stops.set(id, allStops.get(id));
      const result = { tripMap, stopTimes, stops, shapes:new Map() };
      schedule = result;
      lastStatic = Date.now();
      startupError = null;
      console.log(`UL 801 schedule ready: ${tripMap.size} trips; ${usedStops.size} stops. Preparing shapes separately.`);
      // Shapes are optional for listing buses. Parse them separately so visitors
      // can see vehicles BEFORE this relatively expensive step has finished.
      handedOff = true;
      void (async()=>{
        try {
          const usage = new Map();
          for(const trip of tripMap.values()) if(trip.shape_id) usage.set(trip.shape_id,(usage.get(trip.shape_id)||0)+1);
          const wanted = new Set([...usage].sort((a,b)=>b[1]-a[1]).slice(0,40).map(x=>x[0]));
          const shapes = new Map();
          let total=0;
          await readGtfsFiles(filename, {'shapes.txt': r=>{
            if(!wanted.has(r.shape_id))return;
            let a=shapes.get(r.shape_id);
            if(!a){a=[];shapes.set(r.shape_id,a)}
            if(a.length>=4500||total>=95000)return;
            const lat=Number(r.shape_pt_lat),lon=Number(r.shape_pt_lon);
            if(!Number.isFinite(lat)||!Number.isFinite(lon))return;
            a.push([Number(lat.toFixed(5)),Number(lon.toFixed(5))]);total++;
          }});
          result.shapes=shapes;
          console.log(`UL 801 shapes ready: ${shapes.size}, ${total} points`);
          // The next update will deliver the full road-following route.
        } catch(e){console.warn('UL shape data unavailable; vehicles still work:',e.message)}
        finally {await rm(directory,{recursive:true,force:true})}
      })();
      return result;
    } catch(e) {
      startupError=e.message;
      nextStaticRetry=Date.now()+STATIC_RETRY_MS;
      throw e;
    } finally { if(!handedOff) await rm(directory,{recursive:true,force:true}); }
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
  if (Date.now() - predictionsAt < 90000) return cachedPredictions;
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
  if (latest && Date.now() - lastLive < LIVE_MIN_MS) return latest;
  if (livePromise) return livePromise;
  livePromise = (async () => {
    const data = await loadSchedule();
    const feed = GtfsRealtimeBindings.transit_realtime.FeedMessage.decode(await downloadRealtime());
    const buses = [];
    // Predictions must never delay the actual GPS vehicles response.
    void loadPredictions();
    const forecasts = cachedPredictions;
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
      const allEnrichedStops = times.map(enrich);
      const nextStops = allEnrichedStops.slice(ix);
      const passedStops = allEnrichedStops.slice(0, ix);
      const timestamp = v.timestamp ? new Date(Number(v.timestamp.toString()) * 1000).toISOString() : null;
      const id = v.vehicle?.id || e.id;
      const trail = recordTrail(id, tripId, lat, lon, timestamp);
      buses.push({ id, tripId, lat, lon, bearing: v.position.bearing ?? null, direction: trip.direction_id, destination: dest, timestamp, nextStops, passedStops, allStops:allEnrichedStops, nextStopIndex:ix, trail, shape: data.shapes.get(trip.shape_id) || [], routeIsPlanned: true });
    }
    latest = { updated: new Date().toISOString(), buses };
    startupError = null;
    lastLive = Date.now();
    return latest;
  })();
  try { return await livePromise; } finally { livePromise = null; }
}
// All API requests return quickly. Clients receive a loading state rather than
// blocking inside an HTTP request while the large GTFS archive is processed.
function refreshInBackground() {
  if (!staticKey || !realtimeKey) return;
  if (livePromise || (lastLive && Date.now()-lastLive<LIVE_MIN_MS)) return;
  void loadLive().catch(err=>{
    startupError=err.message;
    console.error('UL 801 background refresh:',err.message);
  });
}
app.get('/api/health', (req,res)=>res.json({
  configured:!!staticKey&&!!realtimeKey,
  staticKeyConfigured:!!staticKey,
  realtimeKeyConfigured:!!realtimeKey,
  staticLoaded:!!schedule,
  lastLive:lastLive?new Date(lastLive).toISOString():null,
  loading:!!staticPromise || !!livePromise,
  error:startupError
}));
app.get('/api/vehicles', (req,res)=>{
  res.set('Cache-Control','private, no-store');
  lastViewerAt=Date.now();
  if(!staticKey||!realtimeKey) return res.status(503).json({error:'Trafiklab-nycklar saknas',buses:[]});
  refreshInBackground();
  if(latest) return res.json({...latest,loading:false,refreshing:!!livePromise,stale:Date.now()-lastLive>180000,error:startupError});
  // A failure is distinguishable from an initial load in the client.
  return res.json({updated:null,buses:[],loading:true,phase:schedule?'live':'schedule',error:startupError});
});
const server=app.listen(port,'0.0.0.0',()=>{
  console.log(`UL 801 listening on ${port}`);
  // Preparing the static data starts as soon as Render starts this process:
  // it does NOT require the visitor to keep their browser tab open.
  setTimeout(()=>refreshInBackground(),200);
});
// When a viewer is active, proactively refresh the shared cache. Browser
// intervals of 5s or 10s do NOT trigger a matching rate of Trafiklab calls.
const timer=setInterval(()=>{
  if(Date.now()-lastViewerAt<120000)refreshInBackground();
},BACKGROUND_REFRESH_MS);
timer.unref?.();
