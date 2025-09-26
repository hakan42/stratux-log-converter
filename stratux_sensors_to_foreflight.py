#!/usr/bin/env python3
"""
Convert Stratux sensors.csv (with GPSLatitude/GPSLongitude/GPSTime) into a
ForeFlight-compatible CSV with TWO sections (metadata + time series).

Aviation defaults:
  - Total distance in nautical miles (nm)
  - Speed interpreted as knots (kts)

Adds lightweight aerodrome detection:
  - Provide --airports airports.csv (e.g., OurAirports format) to auto-fill
    "Derived Origin" and "Derived Destination" with nearest ICAO idents.
  - Configurable radius (--airport-threshold-nm, default 20nm) and ICAO-only filter.

Examples:
  python stratux_sensors_to_foreflight_ffcsv.py sensors.csv -o foreflight.csv
  python stratux_sensors_to_foreflight_ffcsv.py sensors.csv -o foreflight.csv --tail-number DERDG --debug
  python stratux_sensors_to_foreflight_ffcsv.py sensors.csv -o foreflight.csv \
      --airports airports.csv --airport-threshold-nm 25

Options:
  --tail-number             Tail Number for Section 1 (default: DEABC)
  --distance-units          Total distance units: nm|km|mi (default: nm)
  --speed-units             Groundspeed units in input: kts|mps|kmh|auto (default: kts)
  --sod-date                Anchor date YYYY-MM-DD if only seconds-of-day timestamps are present
  --airports                Path to airports CSV (OurAirports format preferred)
  --airport-threshold-nm    Max radius to accept nearest airport (default: 20)
  --icao-only               Only accept 4-letter ICAO idents (default: true)
  --debug                   Verbose diagnostics
"""

import argparse, csv, os, sys, logging, math
from datetime import datetime, timezone, timedelta

# -------- Section 1 (ForeFlight metadata header) --------
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

# -------- Section 2 (ForeFlight track header) --------
TRACK_HEADER = [
    "Timestamp","Latitude","Longitude","Altitude","Course","Speed",
    "Bank","Pitch","Horizontal Error","Vertical Error","g Load"
]

# --- Keys tuned for Stratux sensors.csv (matches your latest header) ---
LAT_KEYS   = ["gpslatitude","gps_latitude","latitude","lat"]
LON_KEYS   = ["gpslongitude","gps_longitude","longitude","lon"]
ALT_KEYS   = ["gpsaltitudemsl","gps_altitude_msl","gpsaltitude","altitude","baropressurealtitude"]
SPD_KEYS   = ["groundspeed","smoothgroundspeed","gpsspeed","speed"]
TRK_KEYS   = ["gpstruecourse","headinggps","heading","course"]
BANK_KEYS  = ["roll","rollgps","bank"]       # radians likely → convert to degrees
PITCH_KEYS = ["pitch","pitchgps"]            # radians likely → convert to degrees
HER_KEYS   = ["gpshorizontalaccuracy","horizontalerror"]
VER_KEYS   = ["gpsverticalaccuracy","verticalerror"]
G_KEYS     = ["gload","g-load","g"]
# Prefer Unix epoch GPSTime; fallback: seconds-of-day
TIME_KEYS_EPOCH = ["gpstime","timeunix","unixtime","timestamp","epoch"]
TIME_KEYS_SOD   = ["gpslastfixtimesincemidnight","sod","secondsofday"]

def norm(s): return str(s).strip().lower()

def parse_number(v):
    if v is None: return None
    s = str(v).strip()
    if not s or s.startswith("%!f("):  # Go "%!f(int=0)" → treat as zero
        return 0.0
    try:
        x = float(s)
    except ValueError:
        for unit in ("ft","feet","kts","knots","mps","kmh","km/h","m/s"):
            if s.lower().endswith(unit):
                try: return float(s[:-len(unit)].strip().replace(",", "."))
                except: return None
        return None
    # Filter Stratux sentinels
    if x == 999999.0 or abs(x) > 1e12:
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
    return x

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

def haversine_m(lat1, lon1, lat2, lon2):
    R = 6371000.0
    phi1 = math.radians(lat1); phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1); dl = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dl/2)**2
    return 2 * R * math.asin(math.sqrt(a))

def meters_to(total_m, units):
    if units == "nm": return total_m / 1852.0
    if units == "km": return total_m / 1000.0
    if units == "mi": return total_m / 1609.344
    return total_m / 1852.0

# ---------- Lightweight aerodrome detection ----------
def load_airports_db(path, icao_only=True, types=None, debug=False):
    """
    Load a small airports DB from CSV (OurAirports format recommended).
    Returns list of tuples: (ident, lat, lon).
    Filters:
      - icao_only: ident length==4, all letters/digits (no hyphen/space)
      - types: keep only these types if provided (e.g., {'large_airport','medium_airport','small_airport'})
    """
    keep_types = set(types) if types else None
    out = []
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        r = csv.DictReader(f)
        # OurAirports typical fields: ident, type, name, latitude_deg, longitude_deg, iso_country, ...
        for row in r:
            ident = row.get("ident") or row.get("icao") or row.get("code")
            t     = (row.get("type") or "").strip().lower()
            lat   = row.get("latitude_deg") or row.get("lat") or row.get("Latitude")
            lon   = row.get("longitude_deg") or row.get("lon") or row.get("Longitude")
            if not ident or lat is None or lon is None:
                continue
            try:
                latf = float(str(lat).strip())
                lonf = float(str(lon).strip())
            except Exception:
                continue
            if keep_types and t not in keep_types:
                continue
            ident_clean = ident.strip().upper()
            if icao_only:
                if len(ident_clean) != 4:  # ICAO is typically 4 chars
                    continue
                if not ident_clean.isalnum():
                    continue
            out.append((ident_clean, latf, lonf))
    if debug:
        print(f"Loaded airports: {len(out)}")
    return out

def nearest_airport_ident(lat, lon, airports, max_nm=20.0):
    best_ident = ""
    best_nm = 1e9
    for ident, alat, alon in airports:
        d_m = haversine_m(lat, lon, alat, alon)
        d_nm = d_m / 1852.0
        if d_nm < best_nm:
            best_nm = d_nm
            best_ident = ident
    return best_ident if best_nm <= max_nm else ""

# -----------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="Convert Stratux sensors.csv to ForeFlight CSV (two sections, dynamic metadata + aerodrome detection)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    ap.add_argument("input", nargs="?", help="Stratux sensors.csv")
    ap.add_argument("-o","--output", help="Output ForeFlight CSV")
    ap.add_argument("--tail-number", default="DEABC", help='Tail Number for Section 1 (default: "DEABC")')
    ap.add_argument("--distance-units", choices=["nm","km","mi"], default="nm",
                    help="Units for Total Distance in metadata")
    ap.add_argument("--speed-units", choices=["kts","mps","kmh","auto"], default="kts",
                    help="Units of GroundSpeed in the input file (converted to knots in output)")
    ap.add_argument("--sod-date", help="Anchor date YYYY-MM-DD if only seconds-of-day are present")
    # Aerodrome detection:
    ap.add_argument("--airports", help="Path to airports CSV (OurAirports format preferred)")
    ap.add_argument("--airport-threshold-nm", type=float, default=20.0, help="Max radius to accept nearest airport")
    ap.add_argument("--icao-only", type=lambda s: s.lower() not in {"0","false","no"}, default=True,
                    help="Only accept 4-letter ICAO idents (true/false). Default: true")
    ap.add_argument("--debug", action="store_true", help="Verbose diagnostics")
    args = ap.parse_args()

    if not args.input or not args.output:
        ap.print_help(sys.stderr); sys.exit(2)

    logging.basicConfig(level=logging.DEBUG if args.debug else logging.WARNING, format="%(message)s")
    log = logging.getLogger("ffconv")

    # Pass 1: read header, map keys, decide speed units
    with open(args.input, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames or []
        hnorm = [norm(h) for h in header]

        def map_key(cands):
            return next((hdr for cand in cands for hdr in header if norm(hdr)==cand), None)

        lat_k   = map_key(LAT_KEYS)
        lon_k   = map_key(LON_KEYS)
        alt_k   = map_key(ALT_KEYS)
        spd_k   = map_key(SPD_KEYS)
        trk_k   = map_key(TRK_KEYS)
        bank_k  = map_key(BANK_KEYS)
        pitch_k = map_key(PITCH_KEYS)
        her_k   = map_key(HER_KEYS)
        ver_k   = map_key(VER_KEYS)
        g_k     = map_key(G_KEYS)

        time_epoch_k = map_key(TIME_KEYS_EPOCH)
        time_sod_k   = map_key(TIME_KEYS_SOD)

        if args.debug:
            log.debug(f"keys: lat={lat_k} lon={lon_k} alt={alt_k} spd={spd_k} trk={trk_k} bank={bank_k} pitch={pitch_k} her={her_k} ver={ver_k} g={g_k}")
            log.debug(f"time keys: epoch={time_epoch_k} sod={time_sod_k}")

        if not (lat_k and lon_k):
            ap.error("Missing GPSLatitude/GPSLongitude (or equivalents) in sensors.csv.")
        if not (time_epoch_k or time_sod_k):
            ap.error("Missing GPSTime (epoch) and GPSLastFixTimeSinceMidnight (seconds-of-day). Need one.")

        if args.speed_units == "auto":
            speed_samples = []
            for i, row in enumerate(reader):
                if spd_k is not None:
                    speed_samples.append(parse_number(row.get(spd_k)))
                if i >= 999: break
            spd_units = detect_speed_units(speed_samples)
        else:
            spd_units = args.speed_units

        if args.debug:
            log.debug(f"Using speed units: {spd_units} (converted to knots in output)")

    # Pass 2: full parse, build tracks, compute metadata
    first_dt = last_dt = None
    first_lat = first_lon = last_lat = last_lon = None
    total_m = 0.0
    her_vals = []
    ver_vals = []
    tracks = []

    with open(args.input, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        prev_lat = prev_lon = None

        for r in reader:
            def to_num(key): return parse_number(r.get(key)) if key else None

            lat = to_num(lat_k); lon = to_num(lon_k)
            if not in_range_latlon(lat, lon):
                continue

            dt = None
            if time_epoch_k:
                dt = parse_epoch_as_dt(r.get(time_epoch_k))
            if (dt is None) and time_sod_k:
                dt = parse_sod_as_dt(r.get(time_sod_k), args.sod_date, args.input)
            if dt is None:
                continue

            alt_ft = to_num(alt_k)
            spd    = to_num(spd_k)
            trk    = to_num(trk_k)
            bank   = to_num(bank_k)
            pitch  = to_num(pitch_k)
            her    = to_num(her_k)
            ver    = to_num(ver_k)
            gload  = to_num(g_k)

            if trk is not None: trk = trk % 360.0
            spd_kts = spd_to_knots(spd, spd_units) if spd is not None else None
            bank_deg  = maybe_deg_from_rad(bank)  if bank is not None else None
            pitch_deg = maybe_deg_from_rad(pitch) if pitch is not None else None

            if prev_lat is not None and prev_lon is not None:
                total_m += haversine_m(prev_lat, prev_lon, lat, lon)
            prev_lat, prev_lon = lat, lon

            if her is not None: her_vals.append(her)
            if ver is not None: ver_vals.append(ver)

            tsec = dt.timestamp()
            tracks.append({
                "Timestamp": f"{tsec:.10g}",
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
            if first_lat is None:
                first_lat, first_lon = lat, lon
            last_lat, last_lon = lat, lon

    if not tracks:
        sys.stderr.write("No usable samples (missing lat/lon/time or all filtered).\n")
        sys.exit(1)

    # Metadata computations
    start_ms = int(first_dt.timestamp() * 1000)
    end_ms   = int(last_dt.timestamp() * 1000)
    total_s  = (last_dt - first_dt).total_seconds()
    total_dist = meters_to(total_m, args.distance_units)

    def stats(vals):
        if not vals: return ("", "", "")
        return (f"{max(vals):.3f}", f"{min(vals):.3f}", f"{(sum(vals)/len(vals)):.6f}")

    ver_max, ver_min, ver_avg = stats(ver_vals)
    her_max, her_min, her_avg = stats(her_vals)

    # ----- Aerodrome detection (optional) -----
    derived_origin = ""
    derived_dest   = ""
    if args.airports and os.path.exists(args.airports):
        try:
            db = load_airports_db(
                args.airports,
                icao_only=args.icao_only,
                types={"large_airport","medium_airport","small_airport"},
                debug=args.debug
            )
            if first_lat is not None and first_lon is not None:
                derived_origin = nearest_airport_ident(first_lat, first_lon, db, max_nm=args.airport_threshold_nm)
            if last_lat is not None and last_lon is not None:
                derived_dest = nearest_airport_ident(last_lat, last_lon, db, max_nm=args.airport_threshold_nm)
            if args.debug:
                print(f"Derived Origin: {derived_origin or '(none)'} | Derived Destination: {derived_dest or '(none)'}")
        except Exception as e:
            if args.debug:
                print(f"Airport DB load/detect failed: {e}")

    # Build Section 1 values (dynamic + detected aerodromes + placeholders)
    meta_values = [
        "",                                  # Pilot
        args.tail_number,                    # Tail Number
        derived_origin,                      # Derived Origin (ICAO or "")
        f"{first_lat:.7f}",                  # Start Latitude
        f"{first_lon:.7f}",                  # Start Longitude
        derived_dest,                        # Derived Destination (ICAO or "")
        f"{last_lat:.7f}",                   # End Latitude
        f"{last_lon:.7f}",                   # End Longitude
        str(start_ms),                       # Start Time (ms)
        str(end_ms),                         # End Time (ms)
        f"{total_s:.1f}",                    # Total Duration (s)
        f"{total_dist:.14f}",                # Total Distance (nm by default)
        "Stratux",                           # Initial Attitude Source
        "", "", "",                          # Device Model, Detailed, iOS Version
        "", "",                              # Battery Level, Battery State
        "Stratux",                           # GPS Source
        ver_max, ver_min, ver_avg,           # Vertical Error stats
        her_max, her_min, her_avg,           # Horizontal Error stats
        "Stratux sensors converter",         # Imported From
        ""                                   # Route Waypoints
    ]

    # Write ForeFlight CSV (two sections)
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        # Section 1 header (unquoted)
        w_header = csv.writer(f)
        w_header.writerow(META_HEADER)
        # Section 1 values (ALL quoted, even empties)
        w_meta_allquoted = csv.writer(f, quoting=csv.QUOTE_ALL)
        w_meta_allquoted.writerow(meta_values)
        # Section 2 (unchanged)
        w_tracks = csv.writer(f)
        w_tracks.writerow(TRACK_HEADER)
        for row in tracks:
            w_tracks.writerow([row[k] for k in TRACK_HEADER])

    if logging.getLogger().level == logging.DEBUG:
        print(f"Rows: {len(tracks)} | Time: {first_dt.strftime('%Y-%m-%dT%H:%M:%SZ')} → {last_dt.strftime('%Y-%m-%dT%H:%M:%SZ')}")
        print(f"Distance: {total_dist:.3f} {args.distance_units}")
        print(f"Speed units (input→output): {args.speed_units}→kts")
        if derived_origin or derived_dest:
            print(f"Aerodromes: origin={derived_origin or '∅'} dest={derived_dest or '∅'}")
        print(f"Output: {args.output}")

if __name__ == "__main__":
    main()
