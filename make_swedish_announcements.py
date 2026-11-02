"""Generate UL 801 stop announcements from the currently available full trip data.
Uses unofficial edge-tts (Microsoft online voice endpoint); no API token required.
"""
import asyncio
import hashlib
import json
import os
import pathlib
import urllib.request

import edge_tts

API = os.environ.get('UL801_API', 'https://ul801-live-v3.onrender.com/api/vehicles')
OUT = pathlib.Path('public/announcements')
VOICE = os.environ.get('UL801_SWEDISH_VOICE', 'sv-SE-SofieNeural')


def collect_stops():
    request = urllib.request.Request(API, headers={'User-Agent': 'UL801-voice-generator/1.0'})
    with urllib.request.urlopen(request, timeout=90) as response:
        payload = json.load(response)
    buses = payload.get('buses', [])
    if not buses:
        raise RuntimeError('No active UL 801 buses in /api/vehicles; retry while buses are operating.')
    names = set()
    for bus in buses:
        # V13 allStops contains the COMPLETE run, not only the next few stops.
        stops = bus.get('allStops')
        if not stops:
            stops = [*bus.get('passedStops', []), *bus.get('nextStops', [])]
        for stop in stops:
            name = str(stop.get('name', '')).strip()
            if name:
                names.add(name)
    if not names:
        raise RuntimeError('API did not provide named stops.')
    return sorted(names)


async def main():
    names = collect_stops()
    OUT.mkdir(parents=True, exist_ok=True)
    manifest_path = OUT / 'manifest.json'
    try:
        manifest = json.loads(manifest_path.read_text('utf-8'))
    except (OSError, ValueError):
        manifest = {}
    count = 0
    for name in names:
        filename = 'sv-' + hashlib.sha256(name.encode('utf-8')).hexdigest()[:20] + '.mp3'
        target = OUT / filename
        if target.exists() and target.stat().st_size > 1000:
            manifest[name] = '/announcements/' + filename
            continue
        print(f'Creating {name}', flush=True)
        # Natural conversational Swedish synthesized once, served as a static file.
        speech = edge_tts.Communicate(f'Nästa hållplats: {name}.', VOICE, rate='-6%')
        for attempt in range(3):
            try:
                await speech.save(str(target))
                break
            except Exception:
                target.unlink(missing_ok=True)
                if attempt == 2:
                    raise
                await asyncio.sleep(4 * (attempt + 1))
                speech = edge_tts.Communicate(f'Nästa hållplats: {name}.', VOICE, rate='-6%')
        manifest[name] = '/announcements/' + filename
        count += 1
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
        await asyncio.sleep(0.7)
    print(f'{len(names)} unique stops found; {count} new MP3 files generated.')


if __name__ == '__main__':
    asyncio.run(main())
