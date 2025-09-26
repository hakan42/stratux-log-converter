#!/usr/bin/env python3
"""
stratux_sensors_to_foreflight_ffcsv.py

Convert Stratux sensors.csv (with GPSLatitude/GPSLongitude/GPSTime) into a
ForeFlight-compatible CSV containing TWO sections:

Section 1 (static metadata):
Pilot,Tail Number,Derived Origin,Start Latitude,Start Longitude,Derived Destination,End Latitude,End Longitude,Start Time,End Time,Total Duration,Total Distance,Initial Attitude Source,Device Model,Device Model Detailed,iOS Version,Battery Level,Battery State,GPS Source,Maximum Vertical Error,Minimum Vertical Error,Average Vertical Error,Maximum Horizontal Error,Minimum Horizontal Error,Average Horizontal Error,Imported From,Route Waypoints
<one static line of values>

Section 2 (time series):
Timestamp,Latitude,Longitude,Altitude,Course,Speed,Bank,Pitch,Horizontal Error,Vertical Error,g Load
<rows>

Usage:
  python stratux_sensors_to_foreflight_ffcsv.py sensors.csv -o foreflight.csv
  # If GPSTime is missing but you have GPSLastFixTimeSinceMidnight:
  python stratux_sensors_to_foreflight_ffcsv.py sensors.csv -o foreflight.csv --sod-date 2025-09-22

Options:
  --debug            Verbose diagnostics (key mapping, units, time range).
  --speed-units      Force speed units (kts/mps/kmh). Default: auto-detect.
  --sod-date         Anchor date (YYYY-MM-DD) for seconds-of-day timestamps (optional).
"""

import argparse, csv, os, sys, logging, math
from datetime import datetime, timezone, timedelta

# -------- Section 1: Static metadata (as requested) --------
META_HEADER = [
    "Pilot","Tail Number","Derived Origin","Start Latitude","Start Longitude",
    "Derived Destination","End Latitude","End Longitude",
    "Start Time","End Time","Total Duration","Total Distance",
    "Initial Attitude Source","Device Model","Device Model Detailed","iOS Version",
    "Battery Level","Battery State","GPS Source",
    "Maximum Vertical Error","Minimum Vertical Error","Average Vertical Error",
    "Maximum Horizontal Error","Minimum Horizontal Error","Average Horizontal Error",
    "Imported From","Route Waypoints"
]

# Keep static for now; you can customize later.
META_VALUES = [
    "", "", "", "", "", "", "", "",                 # Pilot, Tail, Origin, Start Lat/Lon, Dest, End Lat/Lon
    "", "", "", "",                                 # Start/End Time (ms), Total Duration (s), Total Distance (nm or km)
    "Stratux", "Unknown", "Unknown", "",            # Initial Attitude Source, Device Model, Detailed, iOS
    "", "", "Stratux",                              # Battery Level/State, GPS Source
    "", "", "", "", "", "",                         # Vertical/Horizontal Error stats
    "Stratux sensors converter", ""                 # Imported From, Route Waypoints
]

# -------- Section 2: Track schema --------
TRACK_HEADER = [
    "Timestamp","Latitude","Longitude","Altitude","Course","Speed",
    "Bank","Pitch","Horizontal Error","Vertical Error","g Load"
]

# Keys tuned for your sensors.csv (your header included)
LAT_KEYS  = ["gpslatitude","gps_latitude","latitude","lat"]
LON_KEYS  = ["gpslongitude","gps_longitude","longitude","lon"]
ALT_KEYS  = ["gpsaltitudemsl","gps_altitude_msl","gpsaltitude","altitude","baropressurealtitude"]
SPD_KEYS  = ["groundspeed","smoothgroundspeed","gpsspeed","speed"]
TRK_KEYS  = ["gpstruecourse","headinggps","heading","course"]
BANK_KEYS = ["roll","rollgps","bank"]     # 'roll' may be radians; we convert
PITCH_KEYS= ["pitch","pitchgps"]          # may be radians; we convert
HER_KEYS  = ["gpshorizontalaccuracy","horizontalerror"]
VER_KEYS  = ["gpsverticalaccuracy","verticalerror"]
G_KEYS    = ["gload","g-load","g"]

# Prefer Unix epoch GPSTime; fallback seconds-of-day
TIME_KEYS_EPOCH = ["gpstime","timeunix","unixtime","timestamp","epoch"]
TIME_KEYS_SOD   = ["gpslastfixtimesincemidnight","sod","secondsofday"]

def norm(s): return str(s).strip().lower()

def parse_number(v):
    if v is None: return None
    s = str(v).strip()
    if not s or s.startswith("%!f("):  # treat Go "%!f(int=0)" as zero
        return 0.0
    try:
        x = float(s)
    except ValueError:
        for unit in ("ft","feet","kts","knots","mps","kmh","km/h","m/s"):
            if s.lower().endswith(unit):
                try: return float(s[:-len(unit)].strip().replace(",", "."))
                except: return None
        return None
    if x == 999999.0 or abs(x) > 1e12:  # filter Stratux sentinels
        return None
    return x

def find_key(header, candidates):
    h = {norm(k): k for k in header}
    for c in candidates:
        if c in h: return h[c]
    return None

def detect_speed_units(samples):
    vals = [v for v in samples if v is not None]
    if len(vals) < 5: return "kts"
    vals.sort()
    m = vals[len(vals)//2]
    if 40 <= m <= 250: return "kts"
    if 18 <= m <= 100: return "mps"
    if 70 <= m <= 450: return "kmh"
    return "kts"

def spd_to_knots(v, units):
    if v is None: return None
    if units == "kts": return v
    if units == "mps": return v * 1.943844492
    if units == "kmh": return v * 0.539956803
    return v

def maybe_deg_from_rad(x):
    if x is None: return None
    if abs(x) <= math.pi + 0.2:  # looks like radians
        return x * 180.0 / math.pi
    return x  # already degrees

def in_range_latlon(lat, lon):
    return (lat is not None and lon is not None and -90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0)

def parse_epoch_as_dt(x):
    v = parse_number(x)
    if v is None: return None
    try:
        return datetime.fromtimestamp(v, tz=timezone.utc)
    except Exception:
        return None

def parse_sod_as_dt(x, anchor_date=None, fallback_path=None):
    v = parse_number(x)
    if v is None or v < 0 or v > 200000: return None
    if anchor_date:
        base = datetime.strptime(anchor_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        try:
            m = os.path.getmtime(fallback_path) if fallback_path else None
            if m is None: raise RuntimeError
            dt = datetime.fromtimestamp(m, tz=timezone.utc)
            base = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        except Exception:
            now = datetime.now(timezone.utc)
            base = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return base + timedelta(seconds=v)

def main():
    ap = argparse.ArgumentParser(
        description="Convert Stratux sensors.csv to ForeFlight (two-section) CSV",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    ap.add_argument("input", nargs="?", help="Stratux sensors.csv")
    ap.add_argument("-o","--output", help="Output ForeFlight CSV")
    ap.add_argument("--speed-units", choices=["auto","kts","mps","kmh"], default="auto",
                    help="Units of GroundSpeed (auto-detect if not set)")
    ap.add_argument("--sod-date", help="Anchor date YYYY-MM-DD if only seconds-of-day are present")
    ap.add_argument("--debug", action="store_true", help="Verbose diagnostics")
    args = ap.parse_args()

    if not args.input or not args.output:
        ap.print_help(sys.stderr); sys.exit(2)

    logging.basicConfig(level=logging.DEBUG if args.debug else logging.WARNING, format="%(message)s")
    log = logging.getLogger("ffconv")

    # Read header and peek for unit detection
    with open(args.input, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames or []
        hnorm = [norm(h) for h in header]

        lat_k  = find_key(header, [c for c in LAT_KEYS  if c in hnorm])
        lon_k  = find_key(header, [c for c in LON_KEYS  if c in hnorm])
        alt_k  = find_key(header, [c for c in ALT_KEYS  if c in hnorm])
        spd_k  = find_key(header, [c for c in SPD_KEYS  if c in hnorm])
        trk_k  = find_key(header, [c for c in TRK_KEYS  if c in hnorm])
        bank_k = find_key(header, [c for c in BANK_KEYS if c in hnorm])
        pitch_k= find_key(header, [c for c in PITCH_KEYS if c in hnorm])
        her_k  = find_key(header, [c for c in HER_KEYS  if c in hnorm])
        ver_k  = find_key(header, [c for c in VER_KEYS  if c in hnorm])
        g_k    = find_key(header, [c for c in G_KEYS    if c in hnorm])

        time_epoch_k = find_key(header, [c for c in TIME_KEYS_EPOCH if c in hnorm])
        time_sod_k   = find_key(header, [c for c in TIME_KEYS_SOD   if c in hnorm])

        if args.debug:
            log.debug(f"keys: lat={lat_k} lon={lon_k} alt={alt_k} spd={spd_k} trk={trk_k} bank={bank_k} pitch={pitch_k} her={her_k} ver={ver_k} g={g_k}")
            log.debug(f"time keys: epoch={time_epoch_k} sod={time_sod_k}")

        if not (lat_k and lon_k):
            ap.error("Missing GPSLatitude/GPSLongitude (or equivalents) in sensors.csv.")
        if not (time_epoch_k or time_sod_k):
            ap.error("Missing GPSTime (epoch) and GPSLastFixTimeSinceMidnight (seconds-of-day). Need one.")

        # Detect speed units
        speed_samples = []
        peek_rows = []
        for row in reader:
            peek_rows.append(row)
            if spd_k is not None:
                speed_samples.append(parse_number(row.get(spd_k)))
            if len(speed_samples) >= 1000:
                break
        spd_units = args.speed_units if args.speed_units != "auto" else detect_speed_units(speed_samples)
        if args.debug:
            log.debug(f"speed units: {spd_units}")

    # Re-open to iterate fully from top
    with open(args.input, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        tracks = []
        first_dt = last_dt = None

        for r in reader:
            lat = parse_number(r.get(lat_k)); lon = parse_number(r.get(lon_k))
            if not in_range_latlon(lat, lon):
                continue
            # time
            dt = None
            if time_epoch_k:
                dt = parse_epoch_as_dt(r.get(time_epoch_k))
            if (dt is None) and time_sod_k:
                dt = parse_sod_as_dt(r.get(time_sod_k), args.sod_date, args.input)
            if dt is None:
                continue

            alt_ft = parse_number(r.get(alt_k)) if alt_k else None
            spd    = parse_number(r.get(spd_k)) if spd_k else None
            trk    = parse_number(r.get(trk_k)) if trk_k else None
            bank   = parse_number(r.get(bank_k)) if bank_k else None
            pitch  = parse_number(r.get(pitch_k)) if pitch_k else None
            her    = parse_number(r.get(her_k)) if her_k else None
            ver    = parse_number(r.get(ver_k)) if ver_k else None
            gload  = parse_number(r.get(g_k))   if g_k else None

            if trk is not None: trk = trk % 360.0
            spd_kts = spd_to_knots(spd, spd_units) if spd is not None else None
            bank_deg = maybe_deg_from_rad(bank) if bank is not None else None
            pitch_deg= maybe_deg_from_rad(pitch) if pitch is not None else None

            tsec = dt.timestamp()  # float seconds since epoch
            tracks.append({
                "Timestamp": f"{tsec:.10g}",  # ForeFlight often uses scientific notation; %.10g mimics that
                "Latitude": f"{lat:.7f}",
                "Longitude": f"{lon:.7f}",
                "Altitude": "" if alt_ft is None else f"{alt_ft:.1f}",
                "Course":   "" if trk   is None else f"{trk:.1f}",
                "Speed":    "" if spd_kts is None else f"{spd_kts:.1f}",
                "Bank":     "" if bank_deg is None else f"{bank_deg:.2f}",
                "Pitch":    "" if pitch_deg is None else f"{pitch_deg:.2f}",
                "Horizontal Error": "" if her is None else f"{her:.2f}",
                "Vertical Error":   "" if ver is None else f"{ver:.2f}",
                "g Load":   "" if gload is None else f"{gload:.6f}",
            })

            if first_dt is None or dt < first_dt: first_dt = dt
            if last_dt  is None or dt > last_dt:  last_dt  = dt

    if not tracks:
        sys.stderr.write("No usable samples (missing lat/lon/time or all filtered).\n")
        sys.exit(1)

    # Write ForeFlight CSV with two sections
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        # Section 1
        w.writerow(META_HEADER)
        w.writerow(META_VALUES)
        # Section 2
        w.writerow(TRACK_HEADER)
        for row in tracks:
            w.writerow([row[k] for k in TRACK_HEADER])

    if args.debug:
        if first_dt and last_dt:
            print(f"Time range: {first_dt.strftime('%Y-%m-%dT%H:%M:%SZ')} → {last_dt.strftime('%Y-%m-%dT%H:%M:%SZ')}")
        print(f"Wrote {len(tracks)} samples to {args.output}")

if __name__ == "__main__":
    main()
