#!/usr/bin/env python3
"""
South Florida Golf Tee Time Checker
- ForeUp courses: direct API call — fast (~1-2s)
- Chronogolf courses: HTTP API (marketplace teetimes) — fast (~1-2s)
- Direct-book courses: Tee It Up / Club Caddie / Eagle Club — browser or API

Run:  python golf_server.py
Open: http://localhost:5000

On Render (free tier): RENDER=true is set automatically. We use 1 browser, smaller
Chrome footprint, and tighter timeouts/waits so the app stays responsive on 512MB.
For other 512MB hosts set LOW_MEMORY=1. Override with MAX_PARALLEL_BROWSERS=2 if you have more RAM.

Dependencies:
    pip install flask flask-cors requests selenium
    Also needs ChromeDriver matching your Chrome version:
    pip install webdriver-manager   (auto-installs the right ChromeDriver)
"""

from flask import Flask, jsonify, request, send_from_directory, Response, stream_with_context
from flask_cors import CORS
import requests
from datetime import datetime, time as dt_time
import threading
import json
import re
import os
from queue import Queue, Empty

# #region agent log
DEBUG_LOG_PATH = "/Users/tylerchadick/Desktop/Projects/.cursor/debug-bd09fa.log"
DEBUG_ECHO = os.environ.get("DEBUG", "").strip().lower() in ("1", "true", "yes")

def _debug_log(location, message, data, hypothesis_id=None):
    try:
        os.makedirs(os.path.dirname(DEBUG_LOG_PATH), exist_ok=True)
        payload = {"sessionId": "bd09fa", "location": location, "message": message, "data": data, "timestamp": int(datetime.now().timestamp() * 1000)}
        if hypothesis_id:
            payload["hypothesisId"] = hypothesis_id
        with open(DEBUG_LOG_PATH, "a") as f:
            f.write(json.dumps(payload) + "\n")
        if DEBUG_ECHO:
            print(f"  [DEBUG] {location} | {message} | {json.dumps(data)[:200]}")
    except Exception:
        pass
# #endregion

# Timing: set TIMING=1 to print per-phase elapsed seconds to console (see what's slow)
def _timing_enabled():
    return os.environ.get("TIMING", "").strip().lower() in ("1", "true", "yes")


# Request/timeout logging: set LOG=1 (or TIMING=1) to print timeout and speed info to console (e.g. on Render)
def _request_log(msg):
    """Print to console when LOG=1 or TIMING=1 — use for timeouts and request flow to debug speed."""
    if _timing_enabled() or os.environ.get("LOG", "").strip().lower() in ("1", "true", "yes"):
        try:
            print(f"  [LOG] {msg}")
        except Exception:
            pass


def _log_timing(label, start_monotonic, course_name=None):
    """If TIMING=1, print elapsed seconds since start_monotonic. start_monotonic = time.monotonic() at phase start."""
    if not _timing_enabled():
        return
    try:
        import time as _t
        elapsed = _t.monotonic() - start_monotonic
        prefix = f"  [{course_name}] " if course_name else "  "
        print(f"{prefix}{label}: {elapsed:.1f}s")
    except Exception:
        pass


app = Flask(__name__, static_folder=".")

# Render free tier: 512MB, single instance. We optimize driver and waits when RENDER=true.
def _is_render():
    return os.environ.get("RENDER", "").strip().lower() == "true" or os.environ.get("LOW_MEMORY", "").strip() == "1"

# Browser concurrency: 2 parallel by default locally; on Render (512MB) default 1 to avoid OOM.
def _max_browser_workers():
    v = os.environ.get("MAX_PARALLEL_BROWSERS", "").strip().lower()
    if v in ("1", "true", "yes", "low"):
        return 1
    if v == "":
        return 1 if _is_render() else 2
    try:
        n = int(v)
        return max(1, min(6, n))
    except ValueError:
        return 1 if _is_render() else 2

CORS(app)

# ─────────────────────────────────────────────
# COURSE DEFINITIONS
# ─────────────────────────────────────────────
COURSES = [
    {
        "id": 1,
        "name": "Osprey Pointe",
        "location": "Boca Raton, FL",
        "type": "foreup",
        "foreup_id": "21262",
        "schedule_id": "7481",
        "booking_url": "https://app.foreupsoftware.com/index.php/booking/21262/7481#teetimes",
    },
    {
        "id": 2,
        "name": "Park Ridge",
        "location": "Lake Worth, FL",
        "type": "foreup",
        "foreup_id": "21265",
        "schedule_id": "7483",
        "booking_url": "https://app.foreupsoftware.com/index.php/booking/21265/7483#teetimes",
    },
    {
        "id": 3,
        "name": "Okeeheelee",
        "location": "West Palm Beach, FL",
        "type": "foreup",
        "foreup_id": "21263",
        "schedule_id": "7480",
        "booking_url": "https://app.foreupsoftware.com/index.php/booking/21263/7480#teetimes",
    },
    # Chronogolf disabled – was id 4 North Palm Beach CC
    # { "id": 4, "name": "North Palm Beach CC", "location": "North Palm Beach, FL", "type": "chronogolf", "chronogolf_slug": "north-palm-beach-country-club", "booking_url": "https://www.chronogolf.com/club/north-palm-beach-country-club" },
    {
        "id": 6,
        "name": "The Florida Club",
        "location": "Stuart, FL",
        "type": "direct",
        "direct_scraper": "teeitup",
        "teeitup_course_id": "4529",
        "booking_url": "https://the-florida-club.book.teeitup.golf/?course=4529&max=999999",
        "scrape_url": "https://the-florida-club.book.teeitup.golf/?course=4529&max=999999",
    },
    {
        "id": 8,
        "name": "Atlantic National",
        "location": "Lake Worth, FL",
        "type": "direct",
        "direct_scraper": "teeitup",
        "teeitup_course_id": "3495",
        "booking_url": "https://atlantic-national-golf-club.book.teeitup.com/?course=3495&max=999999",
        "scrape_url": "https://atlantic-national-golf-club.book.teeitup.com/?course=3495&max=999999",
    },
    {
        "id": 9,
        "name": "Southwinds",
        "location": "Boca Raton, FL",
        "type": "foreup",
        "foreup_id": "21261",
        "schedule_id": "7476",
        "booking_url": "https://app.foreupsoftware.com/index.php/booking/21261/7476#teetimes",
    },
    {
        "id": 10,
        "name": "Abacoa Golf Club",
        "location": "Jupiter, FL",
        "type": "foreup",
        "foreup_id": "20120",
        "schedule_id": "3710",
        "booking_url": "https://app.foreupsoftware.com/index.php/booking/20120/3710#teetimes",
    },
    # Chronogolf disabled – was id 11 Winston Trails, id 12 Westchester
    # { "id": 11, "name": "Winston Trails Golf Club", "location": "Lake Worth, FL", "type": "chronogolf", "chronogolf_slug": "winston-trails-golf-club", "booking_url": "https://www.chronogolf.com/club/winston-trails-golf-club" },
    # { "id": 12, "name": "Westchester Golf Course", "location": "Boynton Beach, FL", "type": "chronogolf", "chronogolf_slug": "westchester-country-club", "booking_url": "https://www.chronogolf.com/club/westchester-country-club" },
    {
        "id": 13,
        "name": "Boca Raton Golf & Racquet Club",
        "location": "Boca Raton, FL",
        "type": "direct",
        "direct_scraper": "clubcaddie",
        "clubcaddie_slug": "ajedabab",
        "clubcaddie_interaction": "96u0vaf8k5ip4acvf4ccpgfsbi",
        "booking_url": "https://apimanager-cc22.clubcaddie.com/webapi/view/ajedabab/slots",
        "scrape_url": "https://apimanager-cc22.clubcaddie.com/webapi/view/ajedabab/slots",
    },
    {
        "id": 14,
        "name": "Boynton Beach Links",
        "location": "Boynton Beach, FL",
        "type": "direct",
        "direct_scraper": "eagleclub",
        "booking_url": "https://player.eagleclubsystems.online/#/tee-slot?dbname=labb20241201",
        "scrape_url": "https://player.eagleclubsystems.online/#/tee-slot?dbname=labb20241201",
    },
    {
        "id": 15,
        "name": "The Park Golf Course",
        "location": "Lake Worth, FL",
        "type": "chronogolf",
        "chronogolf_club_id": 19070,
        "chronogolf_course_id": 23494,
        "chronogolf_affiliation_type_id": 117644,
        "booking_url": "https://www.chronogolf.com/club/19070/widget?medium=widget&source=club",
    },
    {
        "id": 16,
        "name": "Westchester Country Club",
        "location": "Boynton Beach, FL",
        "type": "chronogolf",
        "chronogolf_club_id": 4586,
        "chronogolf_course_id": 5397,
        "chronogolf_affiliation_type_id": 19166,
        "booking_url": "https://www.chronogolf.com/club/4586/widget?medium=widget&source=club",
    },
    {
        "id": 17,
        "name": "Winston Trails Golf Club",
        "location": "Lake Worth, FL",
        "type": "chronogolf",
        "chronogolf_club_id": 19435,
        "chronogolf_course_id": 27095,
        "chronogolf_affiliation_type_id": 133162,
        "booking_url": "https://www.chronogolf.com/club/19435/widget?medium=widget&source=club",
    },
    {
        "id": 18,
        "name": "Country Club of Coral Springs",
        "location": "Coral Springs, FL",
        "type": "direct",
        "direct_scraper": "teeitup",
        "teeitup_course_id": "4572",
        "booking_url": "https://country-club-of-coral-springs.book.teeitup.com/?course=4572&max=999999",
        "scrape_url": "https://country-club-of-coral-springs.book.teeitup.com/?course=4572&max=999999",
    },
]

FOREUP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://app.foreupsoftware.com/",
    "X-Requested-With": "XMLHttpRequest",
}


# ─────────────────────────────────────────────
# FOREUP
# ─────────────────────────────────────────────
def fetch_foreup_times(course, date_mmddyyyy, players):
    import time as _t
    t0 = _t.monotonic()
    url = "https://app.foreupsoftware.com/index.php/api/booking/times"
    params = {
        "time": "all",
        "date": date_mmddyyyy,
        "holes": "18",
        "players": str(players),
        "booking_class": "",
        "schedule_id": course["schedule_id"],
        "schedule_ids[]": course["schedule_id"],
        "specials_only": "0",
        "api_key": "no_limits",
    }
    try:
        resp = requests.get(url, params=params, headers=FOREUP_HEADERS, timeout=12)
        resp.raise_for_status()
        data = resp.json()

        # ForeUp sometimes returns a dict with an error instead of a list
        if not isinstance(data, list):
            msg = data.get("message") or data.get("error") or "Unexpected response"
            return {"status": "error", "message": str(msg)}

        times = []
        for slot in data:
            # ForeUp may use "openings" or "available_spots" for spots open in the slot
            spots = slot.get("available_spots") or slot.get("openings")
            try:
                spots = int(spots) if spots is not None else 0
            except (TypeError, ValueError):
                spots = 0
            # Only include slots that have enough room for the requested party size
            if spots < players:
                continue
            times.append({
                "time": _foreup_time_to_str(slot.get("time", "")),
                "available_spots": spots,
                "holes": slot.get("holes", 18),
                "green_fee": slot.get("green_fee"),
                "cart_fee": slot.get("cart_fee"),
                "rate_type": slot.get("rate_type", ""),
            })
        elapsed = _t.monotonic() - t0
        _request_log(f"ForeUp {course.get('name', '')} (id={course.get('id')}): ok in {elapsed:.1f}s, {len(times)} times")
        return {"status": "ok", "times": times}
    except requests.exceptions.Timeout:
        elapsed = _t.monotonic() - t0
        _request_log(f"ForeUp {course.get('name', '')} (id={course.get('id')}): REQUEST TIMED OUT after {elapsed:.1f}s")
        return {"status": "error", "message": "Request timed out"}
    except requests.exceptions.HTTPError as e:
        elapsed = _t.monotonic() - t0
        _request_log(f"ForeUp {course.get('name', '')} (id={course.get('id')}): HTTP {e.response.status_code} in {elapsed:.1f}s")
        return {"status": "error", "message": f"HTTP {e.response.status_code}"}
    except Exception as e:
        elapsed = _t.monotonic() - t0
        _request_log(f"ForeUp {course.get('name', '')} (id={course.get('id')}): error in {elapsed:.1f}s — {str(e)[:80]}")
        return {"status": "error", "message": str(e)}


def _foreup_time_to_str(raw):
    """
    ForeUp returns time as a datetime string e.g. '2026-03-07 13:00'
    Convert to 12-hour format: 1:00pm
    """
    raw_str = str(raw).strip()
    try:
        # Format: '2026-03-07 13:00' or '2026-03-07 13:00:00'
        if ' ' in raw_str:
            time_part = raw_str.split(' ')[1][:5]  # grab HH:MM
            h, m = int(time_part.split(':')[0]), int(time_part.split(':')[1])
            period = "am" if h < 12 else "pm"
            h12 = h % 12 or 12
            return f"{h12}:{m:02d}{period}"
        # Already readable like "7:30am"
        if re.search(r'[ap]m', raw_str, re.I):
            return raw_str
        # Plain HH:MM
        if ':' in raw_str:
            h, m = int(raw_str.split(':')[0]), int(raw_str.split(':')[1][:2])
            period = "am" if h < 12 else "pm"
            h12 = h % 12 or 12
            return f"{h12}:{m:02d}{period}"
        # Minutes since midnight
        minutes = int(float(raw_str))
        h = minutes // 60
        m = minutes % 60
        period = "am" if h < 12 else "pm"
        h12 = h % 12 or 12
        return f"{h12}:{m:02d}{period}"
    except Exception:
        return raw_str


# ─────────────────────────────────────────────
# CHRONOGOLF - HTTP API (marketplace teetimes endpoint)
# Courses use: chronogolf_club_id, chronogolf_course_id, chronogolf_affiliation_type_id.
# ─────────────────────────────────────────────

CHRONOGOLF_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.chronogolf.com/",
}


def _chronogolf_time_to_str(raw):
    """Chronogolf returns start_time as '08:00' or '08:12'. Convert to 8:00am, 8:12am."""
    raw_str = str(raw).strip()
    try:
        if ":" in raw_str:
            parts = raw_str.split(":")
            h = int(parts[0])
            m = int(parts[1][:2]) if len(parts) > 1 else 0
            period = "am" if h < 12 else "pm"
            h12 = h % 12 or 12
            return f"{h12}:{m:02d}{period}"
        return raw_str
    except Exception:
        return raw_str


def fetch_chronogolf_times(course, date_iso, players):
    """
    Fetch tee times for a Chronogolf course via HTTP API (no browser).
    GET marketplace/clubs/{club_id}/teetimes?date=YYYY-MM-DD&course_id=...&affiliation_type_ids[]=...&nb_holes=18
    """
    import time as _t
    t0 = _t.monotonic()
    club_id = course.get("chronogolf_club_id")
    course_id = course.get("chronogolf_course_id")
    affiliation_type_id = course.get("chronogolf_affiliation_type_id")
    if not all([club_id, course_id, affiliation_type_id]):
        _request_log(f"Chronogolf API [{course.get('name', '')}]: missing config — club_id={club_id!r} course_id={course_id!r} affiliation_type_id={affiliation_type_id!r}")
        return {"status": "error", "message": "Missing Chronogolf course config (need chronogolf_club_id, chronogolf_course_id, chronogolf_affiliation_type_id)", "times": []}

    url = f"https://www.chronogolf.com/marketplace/clubs/{club_id}/teetimes"
    # API expects affiliation_type_ids[] repeated for group size (e.g. 4 times for 4 players)
    params_list = [("date", date_iso), ("course_id", course_id)]
    for _ in range(max(1, min(4, players))):
        params_list.append(("affiliation_type_ids[]", str(affiliation_type_id)))
    params_list.append(("nb_holes", "18"))

    # #region agent log
    _debug_log("fetch_chronogolf_times:params", "request params", {"params_list": params_list, "url": url}, "H3")
    # #endregion
    _request_log(f"Chronogolf API [{course.get('name', '')}]: GET {url} params: date={date_iso} course_id={course_id} players={players}")
    try:
        resp = requests.get(url, params=params_list, headers=CHRONOGOLF_HEADERS, timeout=15)
        elapsed = _t.monotonic() - t0
        _request_log(f"Chronogolf API [{course.get('name', '')}]: response {resp.status_code} in {elapsed:.1f}s")
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list):
            _request_log(f"Chronogolf API [{course.get('name', '')}]: unexpected body type {type(data).__name__} (expected list)")
            return {"status": "error", "message": "Unexpected response", "times": []}
        _request_log(f"Chronogolf API [{course.get('name', '')}]: got {len(data)} raw slots")
        # #region agent log
        if data:
            s0 = data[0]
            hole_related = {k: s0.get(k) for k in list(s0.keys()) if "hole" in k.lower() or k in ("course", "course_id", "product", "product_name", "round", "format", "nb_holes", "holes")}
            _debug_log("fetch_chronogolf_times:first_slot", "first slot hole-related keys and values", {"slot_keys": list(s0.keys()), "hole_related": hole_related, "green_fees_0_keys": list((s0.get("green_fees") or [{}])[0].keys()) if s0.get("green_fees") else None}, "H1")
            gf0 = (s0.get("green_fees") or [{}])[0] if s0.get("green_fees") else {}
            if isinstance(gf0, dict):
                _debug_log("fetch_chronogolf_times:green_fees_0", "first green_fee hole-related", {k: gf0.get(k) for k in list(gf0.keys()) if "hole" in k.lower() or k in ("nb_holes", "holes", "course")}, "H4")
        # #endregion
    except requests.exceptions.Timeout:
        elapsed = _t.monotonic() - t0
        _request_log(f"Chronogolf API [{course.get('name', '')}]: REQUEST TIMED OUT after {elapsed:.1f}s")
        return {"status": "error", "message": "Request timed out", "times": []}
    except Exception as e:
        elapsed = _t.monotonic() - t0
        _request_log(f"Chronogolf API [{course.get('name', '')}]: error in {elapsed:.1f}s — {type(e).__name__}: {str(e)[:120]}")
        return {"status": "error", "message": str(e), "times": []}

    def _slot_hole_count(slot):
        """Get hole count from slot; API may use nb_holes, holes, or nested. Returns int or None."""
        v = slot.get("nb_holes") or slot.get("holes") or slot.get("number_of_holes") or slot.get("hole_count")
        if v is not None:
            try:
                return int(v)
            except (TypeError, ValueError):
                pass
        # Some APIs return "course": "18 holes" or similar
        course = slot.get("course") or slot.get("product_name") or ""
        if isinstance(course, str):
            if "18" in course and "9" not in course.replace("18", ""):
                return 18
            if "9" in course and "18" not in course:
                return 9
        # Nested product/offer or in first green_fee
        for key in ("product", "offer", "rate"):
            obj = slot.get(key)
            if isinstance(obj, dict):
                n = obj.get("nb_holes") or obj.get("holes")
                if n is not None:
                    try:
                        return int(n)
                    except (TypeError, ValueError):
                        pass
        gfs = slot.get("green_fees") or []
        if gfs and isinstance(gfs[0], dict):
            n = gfs[0].get("nb_holes") or gfs[0].get("holes")
            if n is not None:
                try:
                    return int(n)
                except (TypeError, ValueError):
                    pass
        return None

    times = []
    slot_log_count = [0]
    for slot in data:
        # Only 18-hole times: we request nb_holes=18; exclude only when slot explicitly says 9 holes
        slot_holes = _slot_hole_count(slot)
        included = not (slot_holes is not None and slot_holes != 18)
        if slot_log_count[0] < 8:
            # #region agent log
            _debug_log("fetch_chronogolf_times:slot_filter", "slot hole check", {"start_time": slot.get("start_time"), "slot_holes": slot_holes, "included": included, "slot_keys": list(slot.keys()) if slot_holes is None else None}, "H2")
            slot_log_count[0] += 1
            # #endregion
        # Only 18-hole: exclude when slot explicitly says 9; include when 18 or unknown (we request nb_holes=18)
        if slot_holes is not None and slot_holes != 18:
            continue
        out_of_capacity = slot.get("out_of_capacity", False)
        available_spots = 0 if out_of_capacity else 4  # Chronogolf slot is typically 4 players
        if available_spots < players:
            continue
        start_time = slot.get("start_time", "")
        green_fees = slot.get("green_fees") or []
        green_fee = None
        if green_fees:
            gf = green_fees[0]
            green_fee = gf.get("green_fee") or gf.get("price")
        # We request 18 holes; slots with no price are not available for 18-hole — skip them
        if green_fee is None:
            continue
        times.append({
            "time": _chronogolf_time_to_str(start_time),
            "available_spots": available_spots,
            "holes": 18,
            "green_fee": green_fee,
        })
    elapsed = _t.monotonic() - t0
    # #region agent log
    _debug_log("fetch_chronogolf_times:counts", "raw vs filtered count", {"raw_slots": len(data), "times_returned": len(times)}, "H2")
    if len(data) > 0 and len(times) == 0:
        _request_log(f"Chronogolf API [The Park]: 0 times from {len(data)} raw — first slot keys: {list(data[0].keys())}")
    # #endregion
    _request_log(f"Chronogolf {course.get('name', '')} (id={course.get('id')}): ok in {elapsed:.1f}s, {len(times)} times")
    return {"status": "ok", "times": times}

def _get_driver():
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    chrome_bin = os.environ.get("CHROME_BIN")
    chromedriver_path = os.environ.get("CHROMEDRIVER_PATH")
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--blink-settings=imagesEnabled=false")
    options.add_argument("--log-level=3")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-background-networking")
    options.add_argument("--disable-sync")
    options.add_argument("--disable-translate")
    options.add_argument("--no-first-run")
    options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
    # "none" = get() returns as soon as navigation starts; we wait for content with WebDriverWait. Avoids long "page load" for slow sites (e.g. TeeItUp).
    options.page_load_strategy = "none"
    if _is_render():
        # Free tier 512MB: smaller window, limit renderer and JS heap so Chrome doesn't OOM
        options.add_argument("--window-size=1024,768")
        options.add_argument("--js-flags=--max-old-space-size=128")
        options.add_argument("--renderer-process-limit=1")
    else:
        options.add_argument("--window-size=1280,900")

    def _wrap(driver):
        try:
            if _is_render():
                driver.set_page_load_timeout(28)
                driver.set_script_timeout(18)
            else:
                driver.set_page_load_timeout(45)
                driver.set_script_timeout(25)
        except Exception:
            pass
        return driver

    if chrome_bin and chromedriver_path:
        from selenium.webdriver.chrome.service import Service
        options.binary_location = chrome_bin
        service = Service(chromedriver_path)
        return _wrap(webdriver.Chrome(service=service, options=options))
    try:
        from webdriver_manager.chrome import ChromeDriverManager
        from selenium.webdriver.chrome.service import Service
        service = Service(ChromeDriverManager().install())
        return _wrap(webdriver.Chrome(service=service, options=options))
    except Exception:
        return _wrap(webdriver.Chrome(options=options))


CHRONOGOLF_SLOT_SELECTORS = [
    "[data-cy='teetime']", "[data-cy='tee-time']",
    ".teetime", ".tee-time-slot", "[class*='TeeTime']",
    "[class*='teetime']", "[class*='tee_time']",
    "button[class*='time']", ".booking-slot",
]
# XPath equivalents for lxml (faster than many Selenium find_elements)
CHRONOGOLF_SLOT_XPATHS = [
    "//*[@data-cy='teetime']", "//*[@data-cy='tee-time']",
    "//*[contains(@class, 'teetime')]", "//*[contains(@class, 'tee-time-slot')]",
    "//*[contains(@class, 'TeeTime')]", "//*[contains(@class, 'tee_time')]",
    "//button[contains(@class, 'time')]", "//*[contains(@class, 'booking-slot')]",
]


def _chronogolf_wait_any_slot(driver, timeout=1.0, poll=0.05):
    """Wait up to timeout seconds for any slot selector to appear. Returns (selector, True) or (None, False)."""
    from selenium.webdriver.common.by import By
    import time
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for sel in CHRONOGOLF_SLOT_SELECTORS:
            try:
                if driver.find_elements(By.CSS_SELECTOR, sel):
                    return (sel, True)
            except Exception:
                pass
        time.sleep(poll)
    return (None, False)


# ----- CHRONOGOLF DISABLED: functions below are not called; left in place to re-enable later -----
def _fetch_one_chronogolf_course(course, date_iso, players):
    """
    Fetch tee times for a single Chronogolf course using its own browser.
    Returns (course_id, result). Used so we can run all Chronogolf courses in parallel.
    """
    import time
    from selenium.webdriver.common.by import By
    name = course.get("name", "Chronogolf")
    driver = None
    try:
        t0 = time.monotonic()
        driver = _get_driver()
        _log_timing("get_driver", t0, name)
        slug = course["chronogolf_slug"]
        url = f"https://www.chronogolf.com/club/{slug}?date={date_iso}&step=teetimes&holes=&coursesIds=&deals=false&groupSize={players}"
        t1 = time.monotonic()
        driver.get(url)
        _log_timing("page load", t1, name)

        # Network check: on Render run in thread with tight cap so get_log() can't block; locally 3 direct tries (checkpoint style)
        t2 = time.monotonic()
        times = None
        if _is_render():
            _net_result = [None]
            def _run():
                try:
                    _net_result[0] = _intercept_network(driver, date_iso, deadline_sec=3, max_entries=25, max_body_calls=2)
                except Exception:
                    pass
            _t = threading.Thread(target=_run, daemon=True)
            _t.start()
            _t.join(timeout=4.0)
            times = _net_result[0]
        else:
            for _ in range(3):
                times = _intercept_network(driver, date_iso)
                if times is not None:
                    break
                time.sleep(0.24)
        if times is not None:
            times = [t for t in times if int(t.get("available_spots") or 0) >= players]
            _log_timing("network intercept (hit API)", t2, name)
            if times:
                return (course["id"], {"status": "ok", "times": times, "booking_url": course["booking_url"]})
            return (course["id"], {"status": "ok", "times": [], "booking_url": course["booking_url"]})
        _log_timing("network intercept (no API)", t2, name)

        # Wait for any slot; on Render keep it short so single browser moves on
        t3 = time.monotonic()
        slot_selector, found = _chronogolf_wait_any_slot(driver, timeout=1.0 if _is_render() else 1.5)
        _log_timing("wait_any_slot", t3, name)
        if not found or not slot_selector:
            return (course["id"], {"status": "ok", "times": [], "booking_url": course["booking_url"]})

        view_more_texts = ("view more", "more times", "show more", "see more", "load more", "afficher plus", "plus de créneaux", "more slots")
        view_more_deadline = time.monotonic() + (2.5 if _is_render() else 5.0)
        view_more_els = 20 if _is_render() else 30
        try:
            for tag in ("button", "a", "[role='button']", "span", "div"):
                if time.monotonic() > view_more_deadline:
                    break
                sel = tag if tag.startswith("[") else tag
                els = driver.find_elements(By.CSS_SELECTOR, sel)
                for el in els[:view_more_els]:
                    if time.monotonic() > view_more_deadline:
                        break
                    t = (el.text or "").strip().lower()
                    if t and len(t) <= 60 and any(phrase in t for phrase in view_more_texts):
                        try:
                            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                            time.sleep(0.05)
                            el.click()
                            time.sleep(0.15)
                            break
                        except Exception:
                            pass
                else:
                    continue
                break
        except Exception:
            pass
        try:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(0.05)
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(0.05)
        except Exception:
            pass
        if os.environ.get("CHRONOGOLF_DUMP_DOM") == "1":
            _chronogolf_dump_filter_dom_once(driver)
        _chronogolf_click_player_filter(driver, players)
        time.sleep(0.15)
        slot_selector, _ = _chronogolf_wait_any_slot(driver, timeout=0.5)
        t4 = time.monotonic()
        # Try lxml first (one page_source + xpath vs many find_elements)
        tree = _parse_html_with_lxml(driver)
        if tree is not None:
            try:
                for xpath in CHRONOGOLF_SLOT_XPATHS:
                    nodes = tree.xpath(xpath)
                    if nodes:
                        texts = [(n.text_content() or "").strip() for n in nodes]
                        times = _parse_chronogolf_slot_texts(texts, players)
                        times = [t for t in times if int(t.get("available_spots") or 0) >= players]
                        if times:
                            _log_timing("DOM parse (lxml)", t4, name)
                            return (course["id"], {"status": "ok", "times": times, "booking_url": course["booking_url"]})
            except Exception:
                pass
        if slot_selector:
            try:
                els = driver.find_elements(By.CSS_SELECTOR, slot_selector)
                if els:
                    times = _parse_dom(els, players)
                    times = [t for t in times if int(t.get("available_spots") or 0) >= players]
                    if times:
                        _log_timing("DOM parse (slot_selector)", t4, name)
                        return (course["id"], {"status": "ok", "times": times, "booking_url": course["booking_url"]})
            except Exception:
                pass
        for sel in CHRONOGOLF_SLOT_SELECTORS:
            try:
                els = driver.find_elements(By.CSS_SELECTOR, sel)
                if els:
                    times = _parse_dom(els, players)
                    times = [t for t in times if int(t.get("available_spots") or 0) >= players]
                    if times:
                        _log_timing("DOM parse (fallback selectors)", t4, name)
                        return (course["id"], {"status": "ok", "times": times, "booking_url": course["booking_url"]})
            except Exception:
                continue
        _log_timing("DOM parse (no slots)", t4, name)
        return (course["id"], {"status": "ok", "times": [], "booking_url": course["booking_url"]})
    except Exception as e:
        return (course["id"], {"status": "error", "message": f"Error: {str(e)[:100]}", "booking_url": course.get("booking_url", "")})
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


def _fetch_one_chronogolf_course_with_timeout(course, date_iso, players, timeout_sec=55):
    """Run _fetch_one_chronogolf_course in a thread so we can cap at timeout_sec and avoid blocking on renderer hang."""
    out = [None]
    def _run():
        try:
            out[0] = _fetch_one_chronogolf_course(course, date_iso, players)
        except Exception as e:
            msg = str(e)
            if "receiving message from renderer" in msg.lower():
                msg = "Page timed out; try again or book on the course site."
            out[0] = (course["id"], {"status": "error", "message": msg[:100], "booking_url": course.get("booking_url", ""), "times": []})
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout=timeout_sec)
    if out[0] is not None:
        return out[0]
    return (course["id"], {"status": "error", "message": "Timed out (%ss)" % timeout_sec, "booking_url": course.get("booking_url", ""), "times": []})


def fetch_all_chronogolf(courses, date_iso, players, on_course_done=None):
    """
    Fetch all Chronogolf courses in parallel (one browser per course).
    If on_course_done(course_id, result) is provided, calls it as each course completes.
    Returns dict of {course_id: result}.
    """
    if not courses:
        return {}
    results = {}
    lock = threading.Lock()

    def _store(course_id, result):
        with lock:
            results[course_id] = result
            if on_course_done:
                on_course_done(course_id, result)

    try:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        max_workers = min(len(courses), _max_browser_workers())
        per_course_timeout = 45 if _is_render() else 55
        timeout_chrono = per_course_timeout * len(courses) + (35 if _is_render() else 45)
        timeout_chrono = max(timeout_chrono, 90)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_fetch_one_chronogolf_course_with_timeout, c, date_iso, players, per_course_timeout): c for c in courses}
            for future in as_completed(futures, timeout=timeout_chrono):
                try:
                    course_id, result = future.result(timeout=5)
                    _store(course_id, result)
                except Exception as e:
                    course = futures[future]
                    msg = str(e)
                    if "receiving message from renderer" in msg.lower() or "timeout" in msg.lower():
                        msg = "Page timed out; try again or book on the course site."
                    _store(course["id"], {"status": "error", "message": msg[:100], "booking_url": course.get("booking_url", ""), "times": []})
    except Exception as e:
        msg = str(e)
        if "chromedriver" in msg.lower() or "chrome not" in msg.lower():
            msg = "Chrome/ChromeDriver not found. Install: pip install webdriver-manager and ensure Chrome is installed."
        for course in courses:
            if course["id"] not in results:
                _store(course["id"], {"status": "error", "message": msg, "booking_url": course["booking_url"]})

    return results


def _chronogolf_dump_filter_dom_once(driver):
    """Dump the Group size filter section HTML to chronogolf_filter_dom.html in project root (once per run)."""
    if getattr(_chronogolf_dump_filter_dom_once, "_did_dump", False):
        return
    try:
        html = driver.execute_script("""
            var xpath = "//*[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'group size')]";
            var it = document.evaluate(xpath, document, null, XPathResult.ORDERED_NODE_ITERATOR_TYPE, null);
            var out = [];
            for (var n = it.iterateNext(); n; n = it.iterateNext()) {
                if ((n.textContent || '').toLowerCase().indexOf('any') >= 0 || (n.textContent || '').indexOf('1 player') >= 0)
                    out.push(n.outerHTML.substring(0, 6000));
            }
            return out.length ? out[out.length-1] : '';
        """)
        if html:
            dump_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "chronogolf_filter_dom.html")
            with open(dump_path, "w") as f:
                f.write(html if isinstance(html, str) else (html[-1] if html else ""))
            print(f"  [Chronogolf] Filter DOM saved to chronogolf_filter_dom.html — open it to inspect Group size markup.")
            _chronogolf_dump_filter_dom_once._did_dump = True
    except Exception:
        pass


def _chronogolf_click_player_filter(driver, players):
    """
    On Chronogolf booking page: FILTERS > Group size > click the option for `players` (e.g. 4).
    Chronogolf uses React; the control is under a section with "Group size" and options "Any", "1 player", ... "4 Players".
    We target: (1) input[type=radio] with value=N in that section, (2) then label with "N Players" in that section.
    """
    if not driver or players < 1 or players > 4:
        return
    # #region agent log
    _debug_log("chronogolf:filter", "attempt", {"players": players}, "H5")
    # #endregion
    try:
        from selenium.webdriver.common.by import By
        import time
        target = str(players)
        option_texts = ["%s Players" % target, "%s players" % target, "%s player" % target]

        def _scroll_click(el):
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            time.sleep(0.05)
            el.click()
            time.sleep(0.12)

        # 1) Find Group size section: element containing "group size" and option text ("Any" / "1 player")
        group_size_container = None
        try:
            candidates = driver.find_elements(
                By.XPATH,
                "//*[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'group size')]",
            )
            for el in candidates[:15]:
                pt = (el.get_attribute("innerHTML") or "") + " " + (el.text or "")
                if "any" in pt.lower() or "1 player" in pt.lower() or "2 players" in pt.lower():
                    group_size_container = el
                    break
            if not group_size_container and candidates:
                group_size_container = candidates[0]
        except Exception:
            pass

        if group_size_container:
            # 2a) Prefer clicking the radio input (value="4") so React state updates reliably
            try:
                inputs = group_size_container.find_elements(
                    By.CSS_SELECTOR,
                    "input[type=radio][value=%s]" % json.dumps(target),
                )
                for inp in inputs:
                    if inp.is_displayed():
                        _debug_log("chronogolf:filter", "filter_click", {"scope": "radio", "value": target}, "H3")
                        _scroll_click(inp)
                        _debug_log("chronogolf:filter", "filter_clicked", {"scope": "radio", "value": target}, "H4")
                        return
            except Exception:
                pass
            # 2b) Else click the label "4 Players" within the same section
            for opt in option_texts:
                try:
                    els = group_size_container.find_elements(By.XPATH, ".//*[normalize-space(.)=%s]" % json.dumps(opt))
                    for el in els[:5]:
                        if not el.is_displayed():
                            continue
                        _debug_log("chronogolf:filter", "filter_click", {"scope": "group_size_label", "text": opt}, "H3")
                        _scroll_click(el)
                        _debug_log("chronogolf:filter", "filter_clicked", {"scope": "group_size_label", "text": opt}, "H4")
                        return
                except Exception:
                    continue

        # 3) Fallback: page-level radio with value=N inside any "group size" ancestor
        try:
            radios = driver.find_elements(
                By.XPATH,
                "//*[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'group size')]"
                "//input[@type='radio' and @value=%s]" % json.dumps(target),
            )
            for inp in radios[:5]:
                if inp.is_displayed():
                    _debug_log("chronogolf:filter", "filter_click", {"scope": "fallback_radio", "value": target}, "H3")
                    _scroll_click(inp)
                    _debug_log("chronogolf:filter", "filter_clicked", {"scope": "fallback_radio"}, "H4")
                    return
        except Exception:
            pass
        try:
            for opt in option_texts:
                els = driver.find_elements(
                    By.XPATH,
                    "//*[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'group size')]"
                    "//*[normalize-space(.)=%s]" % json.dumps(opt),
                )
                for el in els[:5]:
                    if el.is_displayed():
                        _scroll_click(el)
                        _debug_log("chronogolf:filter", "filter_clicked", {"scope": "fallback_xpath", "text": opt}, "H4")
                        return
        except Exception:
            pass

        _debug_log("chronogolf:filter", "no_match", {"players": players}, "H1")
    except Exception as e:
        _debug_log("chronogolf:filter", "exception", {"players": players, "error": str(e)[:120]}, "H5")


def _intercept_network(driver, date_iso, deadline_sec=None, max_entries=None, max_body_calls=None):
    """Scan Chrome network logs for Chronogolf tee time API responses. Optional caps for low-memory (Render) so get_log doesn't block."""
    import time as _t
    try:
        deadline = _t.monotonic() + (deadline_sec if deadline_sec is not None else 10.0)
        logs = driver.get_log("performance")
        if _t.monotonic() > deadline:
            return None
        cap_entries = max_entries if max_entries is not None else 50
        cap_body = max_body_calls if max_body_calls is not None else 3
        body_calls = 0
        entries = logs[-cap_entries:] if len(logs) > cap_entries else logs
        for entry in reversed(entries):
            if _t.monotonic() > deadline or body_calls >= cap_body:
                break
            try:
                msg = json.loads(entry["message"])
                params = msg.get("message", {}).get("params", {})
                resp_info = params.get("response", {})
                url = resp_info.get("url", "")
                if any(k in url.lower() for k in ["tee_time", "teetime", "teetimes", "booking/slot", "availability", "slot", "chronogolf"]):
                    req_id = params.get("requestId")
                    if req_id:
                        body_calls += 1
                        body = driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": req_id})
                        data = json.loads(body.get("body", "[]"))
                        times = _parse_chronogolf_json(data)
                        if times:
                            return times
            except Exception:
                continue
    except Exception:
        pass
    return None


def _parse_chronogolf_json(data):
    times = []
    def process(lst):
        for item in lst:
            if not isinstance(item, dict):
                continue
            raw_time = (item.get("start_time") or item.get("time") or
                       item.get("tee_time") or item.get("hour") or "")
            if not raw_time:
                continue
            spots = item.get("available_spots") or item.get("remaining_spots") or item.get("spots") or 4
            try:
                spots = int(spots)
            except Exception:
                spots = 4
            price = item.get("price") or item.get("green_fee") or item.get("rate")
            times.append({
                "time": _normalize_time(str(raw_time)),
                "available_spots": spots,
                "holes": item.get("holes", 18),
                "green_fee": price,
                "cart_fee": item.get("cart_fee"),
                "rate_type": item.get("rate_type", ""),
            })
    if isinstance(data, list):
        process(data)
    elif isinstance(data, dict):
        for key in ["tee_times", "teeTimes", "data", "slots", "results", "items"]:
            if key in data and isinstance(data[key], list):
                process(data[key])
                if times:
                    break
    return times


def _parse_chronogolf_spots_from_text(text):
    """Parse available spots from Chronogolf slot text: '2 spots', '4 spots left', '1 spot', '2/4', etc. Returns int or None if unknown."""
    if not text:
        return None
    t = text.lower()
    # "X spot(s)" or "X spots left" or "X place(s)"
    m = re.search(r'(\d+)\s*spots?\s*(?:left|available)?', t)
    if m:
        return int(m.group(1))
    m = re.search(r'(\d+)\s*places?\s*(?:left|available)?', t)
    if m:
        return int(m.group(1))
    # "X/4" or "2/4" (spots taken / max)
    m = re.search(r'(\d+)\s*/\s*4', t)
    if m:
        return 4 - int(m.group(1))  # spots left
    # "X golfers" or "for X" (capacity)
    m = re.search(r'(?:for|capacity|max)?\s*(\d+)\s*(?:golfers?|players?)', t)
    if m:
        return int(m.group(1))
    return None


def _parse_chronogolf_slot_texts(text_list, players):
    """Parse a list of slot text strings (from lxml or Selenium) into tee time dicts."""
    times = []
    for text in text_list:
        try:
            text = (text or "").strip()
            if not text:
                continue
            m = re.search(r'\b(\d{1,2}:\d{2})\s*(am|pm|AM|PM)?\b', text, re.I)
            if m:
                t_str = m.group(1) + (" " + (m.group(2) or "").upper() if m.group(2) else "")
                spots = _parse_chronogolf_spots_from_text(text)
                if spots is None:
                    spots = 4
                times.append({
                    "time": t_str.strip(),
                    "available_spots": spots,
                    "holes": 18,
                    "green_fee": None,
                    "cart_fee": None,
                    "rate_type": "",
                })
        except Exception:
            continue
    return times


def _parse_dom(elements, players):
    """Parse Selenium elements into tee time dicts (delegates to text parser)."""
    texts = [(el.text or "").strip() for el in elements]
    return _parse_chronogolf_slot_texts(texts, players)


def _regex_times(html, players):
    """Extract tee times from HTML: 12h (9:00 AM) and 24h (09:00, 14:30). Exclude times outside 5 AM–7 PM."""
    seen = set()
    times = []
    # 12-hour with am/pm
    for m in re.finditer(r'\b(\d{1,2}):(\d{2})\s*(am|pm|AM|PM)\b', html, re.I):
        h, min, period = m.group(1), m.group(2), m.group(3).upper()
        key = f"{h}:{min}{period}"
        if key not in seen:
            seen.add(key)
            times.append({"time": f"{h}:{min} {period}", "available_spots": players, "holes": 18, "green_fee": None, "cart_fee": None, "rate_type": ""})
    # 24-hour (typical tee hours 6–18) so we catch SPAs; skip 19+ to avoid store-hours false positives
    for m in re.finditer(r'\b(0?[6-9]|1[0-8]):([0-5]\d)\b', html):
        h, min = int(m.group(1)), int(m.group(2))
        if h <= 12:
            period = "AM" if h < 12 else "PM"
            h12 = h if h != 12 else 12
            t_str = f"{h12}:{min:02d}{period}"
        else:
            h12 = h % 12 or 12
            t_str = f"{h12}:{min:02d}pm"
        if t_str not in seen:
            seen.add(t_str)
            times.append({"time": f"{h12}:{min:02d} {'PM' if h >= 12 else 'AM'}", "available_spots": players, "holes": 18, "green_fee": None, "cart_fee": None, "rate_type": ""})
    # Drop times outside realistic tee window (5:00 AM – 7:00 PM) to avoid store hours / bogus matches
    filtered = []
    for t in times[:120]:
        slot_time = _parse_slot_time(t.get("time", ""))
        if slot_time is None:
            filtered.append(t)
            continue
        if dt_time(5, 0) <= slot_time <= dt_time(19, 0):
            filtered.append(t)
    return filtered


def _normalize_time(raw):
    if not raw:
        return ""
    raw = str(raw).strip()
    iso = re.match(r'\d{4}-\d{2}-\d{2}T(\d{2}):(\d{2})', raw)
    if iso:
        h, m = int(iso.group(1)), int(iso.group(2))
        period = "am" if h < 12 else "pm"
        h12 = h % 12 or 12
        return f"{h12}:{m:02d}{period}"
    if re.search(r'[ap]m', raw, re.I):
        return raw
    return raw


# (fetch_chronogolf_times is defined above — HTTP API only; Selenium fetch_all_chronogolf not used for type=chronogolf)


# ─────────────────────────────────────────────
# DIRECT COURSE SCRAPERS (per-site custom logic)
# ─────────────────────────────────────────────
def _parse_html_with_lxml(driver):
    """Parse current page HTML with lxml (faster than many Selenium find_elements + .text round-trips). Returns root or None."""
    try:
        from lxml import html
        raw = driver.page_source
        if not raw or len(raw) < 100:
            return None
        return html.fromstring(raw)
    except Exception:
        return None


def _selenium_get_visible_text(driver):
    """Get all visible text from the page (works for SPAs)."""
    try:
        return driver.execute_script("return document.body ? document.body.innerText : ''") or ""
    except Exception:
        return ""


def _regex_times_from_containers(driver, players):
    """Run regex time extraction on elements that look like tee-time lists (2+ times) or single tee-time rows (e.g. table row with one time)."""
    from selenium.webdriver.common.by import By
    time_pat = re.compile(r"\b\d{1,2}:\d{2}\s*(?:am|pm|AM|PM)\b", re.I)
    candidates = []
    for tag in ["div", "li", "tr", "section", "article"]:
        for el in driver.find_elements(By.TAG_NAME, tag):
            try:
                text = (el.text or "").strip()
                if len(text) < 8 or len(text) > 800:
                    continue
                matches = time_pat.findall(text)
                if len(matches) >= 1:
                    candidates.append(text)
            except Exception:
                continue
    if not candidates:
        return []
    combined = "\n".join(candidates)
    return _regex_times(combined, players)


def _golfnow_set_date_in_calendar(driver, date_iso):
    """Set the calendar to the requested date so the page shows that day's tee times. Click the day number in the calendar grid."""
    from selenium.webdriver.common.by import By
    import time
    # #region agent log
    _debug_log("golfnow:calendar", "attempt", {"date_iso": date_iso}, "H4")
    # #endregion
    try:
        parts = date_iso.split("-")
        if len(parts) != 3:
            return
        year, month, day = int(parts[0]), int(parts[1]), int(parts[2])
        day_str = str(day)
        # Try date input first
        for sel in ["input[type='date']", "input[name*='date']", "input[name*='Date']"]:
            try:
                el = driver.find_element(By.CSS_SELECTOR, sel)
                el.clear()
                el.send_keys(date_iso)
                time.sleep(1.5)
                _debug_log("golfnow:calendar", "set_via_input", {"date_iso": date_iso}, "H4")
                return
            except Exception:
                continue
        # Open calendar if it's a popover: click date display or placeholder
        for click_sel in ["[data-testid*='date']", "[aria-label*='date']", ".date-picker", "input[type='text']"]:
            try:
                open_el = driver.find_element(By.CSS_SELECTOR, click_sel)
                if open_el.is_displayed():
                    open_el.click()
                    time.sleep(0.8)
                    break
            except Exception:
                continue
        # Click the day number in the calendar (prefer button or clickable that's just the number)
        for el in driver.find_elements(By.XPATH, f"//button[normalize-space()='{day_str}'] | //a[normalize-space()='{day_str}'] | //*[@role='button' and normalize-space()='{day_str}']"):
            try:
                if el.is_displayed():
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                    time.sleep(0.3)
                    el.click()
                    time.sleep(2)
                    _debug_log("golfnow:calendar", "clicked_day", {"date_iso": date_iso, "day_str": day_str}, "H4")
                    return
            except Exception:
                continue
        for el in driver.find_elements(By.XPATH, f"//*[normalize-space()='{day_str}']"):
            try:
                tag = el.tag_name.lower()
                if tag not in ("button", "a", "span", "div", "td"):
                    continue
                if not el.is_displayed() or not el.is_enabled():
                    continue
                # Prefer small text (likely the day number, not a year)
                text = (el.text or "").strip()
                if text != day_str and day_str not in text.split():
                    continue
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                time.sleep(0.2)
                el.click()
                time.sleep(2)
                _debug_log("golfnow:calendar", "clicked_day_fallback", {"date_iso": date_iso, "day_str": day_str}, "H4")
                return
            except Exception:
                continue
    except Exception as e:
        _debug_log("golfnow:calendar", "error", {"date_iso": date_iso, "error": str(e)[:80]}, "H4")


def _golfnow_click_show_more(driver, max_clicks=35):
    """Click 'Show more' / 'Load more' / 'View more' until no more or max_clicks. Scroll to bottom between clicks so new content loads."""
    import time
    from selenium.webdriver.common.by import By
    show_more_texts = ("show more", "load more", "view more", "more times", "see more", "view more tee times")
    for i in range(max_clicks):
        clicked = False
        for sel in ("button", "a", "[role='button']", "span", "div"):
            for el in driver.find_elements(By.CSS_SELECTOR, sel):
                try:
                    t = (el.text or "").strip().lower()
                    if not t or len(t) > 80:
                        continue
                    if any(phrase in t for phrase in show_more_texts):
                        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                        time.sleep(0.2)
                        el.click()
                        time.sleep(1)
                        clicked = True
                        break
                except Exception:
                    continue
            if clicked:
                break
        if not clicked:
            break
        # Scroll to bottom every few clicks so lazy-loaded sections render
        if (i + 1) % 5 == 0:
            try:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)
            except Exception:
                pass
    return


def _parse_golfnow_player_range(text):
    """
    Parse GolfNow player range from card text: "1", "1-2", "1-4" -> (min_players, max_players).
    GolfNow often shows "1-4 golfers" or "18 / 1-4 /"; we prefer explicit 1-4/1-2 when present.
    """
    if not text:
        return None, None
    t = text.upper()
    # Explicit ranges first (so "18 / 1 /" doesn't override a parent that has "1-4 golfers")
    for pat, (lo, hi) in [
        (r"1\s*[-–]\s*4\b|1-4(?!\d)", (1, 4)),
        (r"2\s*[-–]\s*4\b|2-4(?!\d)", (2, 4)),
        (r"3\s*[-–]\s*4\b|3-4(?!\d)", (3, 4)),
        (r"1\s*[-–]\s*2\b|1-2(?!\d)", (1, 2)),
        (r"1\s*[-–]\s*3\b|1-3(?!\d)", (1, 3)),
    ]:
        if re.search(pat, t, re.I):
            return lo, hi
    # Single number: "1 golfer" or "18 / 1 /"
    for m in re.finditer(r"\b(\d{1,2})(?:-(\d{1,2}))?\b", text):
        try:
            lo = int(m.group(1))
            hi = int(m.group(2)) if m.group(2) else lo
            if 1 <= lo <= hi <= 4:
                return lo, hi
        except (ValueError, TypeError):
            continue
    return None, None


# GolfNow page sections we explicitly collect from (heading text -> section id)
_GOLFNOW_SECTION_HEADINGS = [
    ("Morning Tee Times", "morning"),
    ("Mid-day Tee Times", "mid_day"),
    ("Afternoon Tee Times", "afternoon"),
]


def _golfnow_find_section_roots(driver):
    """
    Find the DOM container for each of Morning, Mid-day, and Afternoon tee time sections.
    Prefer the section ancestor (contains heading + cards); else immediate div/section parent.
    Uses case-insensitive heading match so "Morning tee times" etc. are found.
    Returns dict: section_id -> WebElement (root to scan) or None if not found.
    """
    from selenium.webdriver.common.by import By
    section_roots_by_id = {"morning": None, "mid_day": None, "afternoon": None}

    def _xpath_contains_ci(substring):
        """XPath 1.0: element text (lowered) contains substring (lower)."""
        lower = substring.lower()
        return f"contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{lower}')"

    try:
        # Case-insensitive search for each heading
        for heading_text, section_id in _GOLFNOW_SECTION_HEADINGS:
            xpath = f"//*[{_xpath_contains_ci(heading_text)}]"
            for el in driver.find_elements(By.XPATH, xpath):
                try:
                    parent = None
                    try:
                        parent = el.find_element(By.XPATH, "./ancestor::section[1]")
                    except Exception:
                        pass
                    if not parent:
                        parent = el.find_element(By.XPATH, "./ancestor::*[name()='section' or name()='div'][1]")
                    if parent:
                        section_roots_by_id[section_id] = parent
                        break
                except Exception:
                    pass
            if section_roots_by_id[section_id] is None and section_id == "mid_day":
                for variant in ("MID-DAY", "Mid-day", "mid-day", "Mid day"):
                    for el in driver.find_elements(By.XPATH, f"//*[contains(., '{variant}')]"):
                        try:
                            parent = el.find_element(By.XPATH, "./ancestor::section[1]")
                        except Exception:
                            try:
                                parent = el.find_element(By.XPATH, "./ancestor::*[name()='section' or name()='div'][1]")
                            except Exception:
                                parent = None
                        if parent:
                            section_roots_by_id[section_id] = parent
                            break
                    if section_roots_by_id[section_id] is not None:
                        break
    except Exception:
        pass
    return section_roots_by_id


def _golfnow_assert_sections_accounted_for(sections_found_on_page, section_counts, course_id=None, date_iso=None):
    """
    Test that every GolfNow section present on the page was accounted for (we collected from it).
    sections_found_on_page: list of section ids we found (e.g. ["morning", "mid_day", "afternoon"]).
    section_counts: dict section_id -> number of times collected from that section.
    Logs a warning and raises AssertionError if a section was found but not collected from.
    """
    found_set = set(sections_found_on_page)
    collected_set = set(section_counts.keys())
    missing = found_set - collected_set
    _debug_log("golfnow:sections_test", "assert_sections", {
        "course_id": course_id, "date_iso": date_iso,
        "sections_found_on_page": list(found_set),
        "sections_collected_from": list(collected_set),
        "section_counts": section_counts,
        "missing": list(missing),
    }, "H2")
    if missing:
        raise AssertionError(
            f"GolfNow sections not accounted for: {missing}. "
            f"Found on page: {list(found_set)}, collected from: {list(collected_set)}"
        )


def _golfnow_collect_all_times(driver, players, course_id=None, date_iso=None, facility_name=None):
    """
    GolfNow: find every div.promoted-campaign-wrapper on the page, get the time from each
    wrapper's time element, store and return. Caller applies UI criteria (players, before_time).
    """
    from selenium.webdriver.common.by import By
    time_pat = re.compile(r"\b(\d{1,2}):(\d{2})\s*(am|pm|AM|PM)\b", re.I)
    seen = set()
    times = []
    # GolfNow: one light scroll so any in-view content is in DOM (no ChronoGolf-style multi-step or Show more)
    try:
        import time as _time
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        _time.sleep(0.5)
        driver.execute_script("window.scrollTo(0, 0);")
        _time.sleep(0.3)
    except Exception:
        pass
    # Skip text that is SOLD, "next day" teaser, or wrong date (e.g. "Saturday, Mar 07" when we want Mar 13)
    def _is_junk_card(text, requested_date_iso=None):
        t = text.upper()
        if "SOLD" in t:
            return True
        if "0 TEE TIMES MATCHING" in t or "THERE ARE 0 TEE TIMES" in t:
            return True
        if "VIEW THE NEXT DAY" in t or "VIEW NEXT DAY" in t:
            return True
        if "MORE HOT DEALS" in t and "SOLD" in t:
            return True
        # Looks like a real tee time card (time + price) — don't reject for date in section heading
        if re.search(r"\d{1,2}:\d{2}\s*(AM|PM)", t, re.I) and ("$" in t or "RATE" in t or "ONLINE" in t):
            return False
        # Exclude "Saturday, Mar 07 at 7:47 AM" style (next-day teaser)
        if "MAR 06" in t or "MAR 07" in t or "SATURDAY, MAR" in t or "FRIDAY, MAR 06" in t:
            return True
        if requested_date_iso and re.search(r"\b(MON|TUE|WED|THU|FRI|SAT|SUN),?\s+MAR\s+\d{1,2}\b", t):
            try:
                from datetime import datetime
                want = datetime.strptime(requested_date_iso, "%Y-%m-%d")
                want_str = want.strftime("%b %d").upper()
                if want_str not in t and "MAR " in t:
                    return True
            except Exception:
                pass
        return False

    # Florida Club (6): use section-based collection so we get Morning + Mid-day + Afternoon (page-level query often returns only 2–5 wrappers).
    # Other courses: single page-level query (e.g. Atlantic National has 38 wrappers).
    wrappers = []
    if course_id == 6:
        section_roots = _golfnow_find_section_roots(driver)
        for section_id, root in section_roots.items():
            if root:
                try:
                    wrappers.extend(root.find_elements(By.CSS_SELECTOR, "div.promoted-campaign-wrapper"))
                except Exception:
                    pass
    if not wrappers:
        wrappers = driver.find_elements(By.CSS_SELECTOR, "div.promoted-campaign-wrapper")
    # #region agent log
    _debug_log("golfnow:collect", "wrapper_count", {"course_id": course_id, "date_iso": date_iso, "n_wrappers": len(wrappers)}, "H2")
    # #endregion
    for wrapper in wrappers:
        try:
            text = (wrapper.text or "").strip()
            if not text or _is_junk_card(text, requested_date_iso=date_iso):
                continue
            time_str_for_match = ""
            try:
                time_el = wrapper.find_element(By.CSS_SELECTOR, "time[class*='meridian']")
            except Exception:
                try:
                    time_el = wrapper.find_element(By.TAG_NAME, "time")
                except Exception:
                    time_el = None
            if time_el:
                t = (time_el.text or "").strip()
                if t and re.match(r"^\d{1,2}:\d{2}", t):
                    if not re.search(r"\b(AM|PM)\b", t, re.I):
                        try:
                            sub_el = time_el.find_element(By.TAG_NAME, "sub")
                            if sub_el:
                                t = t + " " + (sub_el.text or "").strip()
                        except Exception:
                            pass
                    time_str_for_match = t
            if not time_str_for_match:
                time_str_for_match = text[:80]
            time_m = time_pat.search(time_str_for_match) or time_pat.search(text)
            if not time_m:
                continue
            h, min_, period = time_m.group(1), time_m.group(2), (time_m.group(3) or "").upper()
            t_str = f"{h}:{min_} {period}" if period else f"{h}:{min_}"
            key = t_str.upper().replace(" ", "")
            if key in seen:
                continue
            seen.add(key)
            min_p, max_p = _parse_golfnow_player_range(text)
            if min_p is None:
                min_p, max_p = 1, 4
            tl = text.lower()
            if min_p == 1 and max_p == 1:
                if ("golfers" in tl or "golfer" in tl) and "1 left" not in tl and "1 spot" not in tl and "sold" not in tl:
                    min_p, max_p = 1, 4
                elif re.search(r"18\s*/\s*1\s*/", text) and "1 left" not in tl and "1 spot" not in tl and "sold" not in tl:
                    min_p, max_p = 1, 4
            times.append({
                "time": t_str,
                "min_players": min_p,
                "max_players": max_p,
                "available_spots": max_p,
                "holes": 18,
                "green_fee": None,
                "cart_fee": None,
                "rate_type": "",
                "section": "wrapper",
            })
        except Exception:
            continue

    # Fallbacks: for non–Florida Club run when we have few times. For Florida Club run only when we got 0 (so we don't show "no available" when site has times).
    if course_id != 6:
        if len(times) < 5:
            for tag in ("div", "li", "tr", "article", "section", "a", "button", "span"):
                for el in driver.find_elements(By.TAG_NAME, tag):
                    try:
                        text = (el.text or "").strip()
                        if len(text) < 5 or len(text) > 200 or _is_junk_card(text, requested_date_iso=date_iso):
                            continue
                        matches = list(time_pat.finditer(text))
                        if len(matches) == 1:
                            m = matches[0]
                            h, min_, period = m.group(1), m.group(2), (m.group(3) or "").upper()
                            t_str = f"{h}:{min_} {period}" if period else f"{h}:{min_}"
                            key = t_str.upper().replace(" ", "")
                            if key not in seen:
                                seen.add(key)
                                times.append({"time": t_str, "min_players": 1, "max_players": 4, "available_spots": 4, "holes": 18, "green_fee": None, "cart_fee": None, "rate_type": "", "section": "unknown"})
                    except Exception:
                        continue
        if len(times) < 10:
            container_times = _regex_times_from_containers(driver, players)
            for t in container_times:
                k = (t.get("time") or "").upper().replace(" ", "")
                if k and k not in seen:
                    seen.add(k)
                    t["min_players"] = t.get("min_players", 1)
                    t["max_players"] = t.get("max_players", 4)
                    t.setdefault("section", "unknown")
                    times.append(t)
        if len(times) < 15:
            visible = _selenium_get_visible_text(driver)
            for t in _regex_times(visible, players):
                k = (t.get("time") or "").upper().replace(" ", "")
                if k and k not in seen:
                    seen.add(k)
                    t["min_players"] = 1
                    t["max_players"] = 4
                    t.setdefault("section", "unknown")
                    times.append(t)
    elif course_id == 6:
        # Florida Club: supplement with container + visible regex when we have few times (wrapper path often returns only 1).
        if len(times) < 15:
            for t in _regex_times_from_containers(driver, players):
                k = (t.get("time") or "").upper().replace(" ", "")
                if k and k not in seen:
                    seen.add(k)
                    t["min_players"] = t.get("min_players", 1)
                    t["max_players"] = t.get("max_players", 4)
                    t.setdefault("section", "unknown")
                    times.append(t)
            if len(times) < 15:
                visible = _selenium_get_visible_text(driver)
                for t in _regex_times(visible, players):
                    k = (t.get("time") or "").upper().replace(" ", "")
                    if k and k not in seen:
                        seen.add(k)
                        t["min_players"] = 1
                        t["max_players"] = 4
                        t.setdefault("section", "unknown")
                        times.append(t)
    return times


def _golfnow_filter_by_players(times, requested_players):
    """Keep only slots where requested_players is within the slot's min_players..max_players (e.g. 1-4 accepts 2,3,4)."""
    out = []
    for t in times:
        min_p = t.get("min_players")
        max_p = t.get("max_players")
        if min_p is None:
            min_p = 1
        if max_p is None:
            max_p = 4
        if min_p <= requested_players <= max_p:
            out.append(t)
    return out


def _fetch_direct_golfnow_with_driver(driver, course, date_iso, players):
    """GolfNow-only: load facility page with searchDate, set calendar, wait for list, collect all div.promoted-campaign-wrapper times. No scroll/Show-more (that is ChronoGolf)."""
    base_url = course.get("scrape_url") or course.get("booking_url", "")
    try:
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        import time
        full_url = base_url + ("&" if "?" in base_url else "?") + f"searchDate={date_iso}"
        driver.get(full_url)
        time.sleep(5)
        try:
            WebDriverWait(driver, 12).until_not(
                EC.text_to_be_present_in_element((By.TAG_NAME, "body"), "Fetching more results")
            )
        except Exception:
            pass
        time.sleep(1)
        _golfnow_set_date_in_calendar(driver, date_iso)
        time.sleep(1.5)
        _time_pattern = re.compile(r"\b\d{1,2}:\d{2}\s*(?:AM|PM)\b", re.I)
        def _body_has_tee_times_or_done(d):
            text = (_selenium_get_visible_text(d) or "")
            if "Morning Tee Times" in text or "Mid-day Tee Times" in text or "Afternoon Tee Times" in text:
                return True
            if "no available tee times" in text.lower() or "Unable to Complete" in text:
                return True
            if len(_time_pattern.findall(text)) >= 3:
                return True
            return False
        try:
            WebDriverWait(driver, 18).until(_body_has_tee_times_or_done)
        except Exception:
            pass
        time.sleep(1)
        # Wait for tee time cards to be in DOM (Florida Club often renders only 2-3 initially; wait for more)
        def _has_enough_wrappers(d):
            n = len(d.find_elements(By.CSS_SELECTOR, "div.promoted-campaign-wrapper"))
            return n >= 8
        try:
            WebDriverWait(driver, 12).until(_has_enough_wrappers)
        except Exception:
            pass
        time.sleep(0.8)
        # Scroll results area into view and to bottom to trigger any lazy-loaded cards
        try:
            for sect in driver.find_elements(By.CSS_SELECTOR, "section.search-results.location.content, section[class*='search-results']"):
                try:
                    driver.execute_script("arguments[0].scrollIntoView({block:'start'});", sect)
                    time.sleep(0.3)
                except Exception:
                    pass
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(0.3)
        except Exception:
            pass
        body_snippet = (_selenium_get_visible_text(driver) or "")[:1200]
        try:
            from datetime import datetime
            want = datetime.strptime(date_iso, "%Y-%m-%d")
            want_str = want.strftime("%b %d")
            body_has_requested_date = want_str in body_snippet or ("Mar " + str(want.day) in body_snippet)
        except Exception:
            want_str = ""
            body_has_requested_date = False
        _debug_log("golfnow:page_state", "requested_date_check", {"course_id": course.get("id"), "date_iso": date_iso, "want_str": want_str, "body_contains_requested_date": body_has_requested_date}, "H1")
        _debug_log("golfnow:page_state", "after_wait", {"course_id": course.get("id"), "url": full_url, "body_preview": body_snippet[:600]}, "golfnow_debug")
        body_check = (_selenium_get_visible_text(driver) or "").lower()
        if course.get("id") == 6:
            if "the florida club" not in body_check or "stuart" not in body_check:
                _debug_log("golfnow:page_verify", "wrong_facility", {"course_id": 6}, "golfnow_debug")
                return {"status": "error", "message": "Page is not The Florida Club (Stuart, FL). Try again or book directly.", "booking_url": course.get("booking_url", ""), "times": []}
        elif course.get("id") == 8:
            if "atlantic national" not in body_check or "lake worth" not in body_check:
                _debug_log("golfnow:page_verify", "wrong_facility", {"course_id": 8}, "golfnow_debug")
                return {"status": "error", "message": "Page is not Atlantic National (Lake Worth). Try again or book directly.", "booking_url": course.get("booking_url", ""), "times": []}
        facility_name_for_collect = None if course.get("id") == 6 else course.get("name")
        all_times = _golfnow_collect_all_times(driver, players, course_id=course.get("id"), date_iso=date_iso, facility_name=facility_name_for_collect)
        times = _golfnow_filter_by_players(all_times, players)
        _debug_log("golfnow:filter", "players", {"course_id": course.get("id"), "requested_players": players, "len_raw": len(all_times), "len_after_filter": len(times), "raw_sample": [{"time": t.get("time"), "min": t.get("min_players"), "max": t.get("max_players")} for t in all_times[:10]]}, "golfnow_debug")
        if times:
            _debug_log("direct_golfnow:source", "scrape", {"course_id": course.get("id"), "len_times": len(times), "len_raw": len(all_times), "players": players, "first": [t.get("time") for t in times[:8]]}, "direct")
            return {"status": "ok", "times": times[:120], "booking_url": course.get("booking_url", "")}
        _debug_log("direct_golfnow:source", "no_times", {"course_id": course.get("id"), "message": "No tee times found"}, "direct")
        return {"status": "error", "message": "No tee times found", "booking_url": course.get("booking_url", "")}
    except Exception as e:
        return {"status": "error", "message": str(e)[:120], "booking_url": course.get("booking_url", "")}


def fetch_direct_golfnow(course, date_iso, players):
    """GolfNow: one-off fetch with its own driver. For batch use fetch_all_direct_golfnow."""
    driver = None
    try:
        driver = _get_driver()
        return _fetch_direct_golfnow_with_driver(driver, course, date_iso, players)
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


def fetch_all_direct_golfnow(courses, date_iso, players, before_time=None, on_course_done=None):
    """GolfNow: one shared browser for all courses (avoids deadlock, more reliable). Calls on_course_done(course_id, result) for each."""
    if not courses:
        return
    driver = None
    try:
        driver = _get_driver()
        for course in courses:
            result = _fetch_direct_golfnow_with_driver(driver, course, date_iso, players)
            result["booking_url"] = course.get("booking_url", "")
            if before_time and result.get("status") == "ok":
                result["times"] = apply_time_filter(result.get("times", []), before_time)
            if result.get("status") == "ok" and result.get("times"):
                for t in result["times"]:
                    if t.get("time"):
                        t["time"] = _normalize_tee_time_display(t["time"])
            if on_course_done:
                on_course_done(course["id"], result)
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


# ─────────────────────────────────────────────
# Tee It Up (Florida Club, Atlantic National) — Kenna API (replaces Selenium)
# GET https://phx-api-be-east-1b.kenna.io/v2/tee-times?date=YYYY-MM-DD&facilityIds={id}
# ─────────────────────────────────────────────
KENNA_TEETIMES_URL = "https://phx-api-be-east-1b.kenna.io/v2/tee-times"

def _fetch_teeitup_kenna_api(course, date_iso, players):
    """Tee It Up (Florida Club 4529, Atlantic National 3495, etc.): Kenna API — no browser. Returns { status, times, booking_url }."""
    _kenna_log = os.environ.get("LOG", "").strip().lower() in ("1", "true", "yes") or os.environ.get("KENNA_DEBUG", "").strip().lower() in ("1", "true", "yes")
    facility_id = course.get("teeitup_course_id")
    if not facility_id:
        return {"status": "error", "message": "No teeitup_course_id", "booking_url": course.get("booking_url", ""), "times": []}
    date_iso = (date_iso or "").strip()
    if _kenna_log:
        print(f"  [Kenna API] date_iso={repr(date_iso)} len={len(date_iso)} facility_id={facility_id}")
    if len(date_iso) != 10 or date_iso[4] != "-" or date_iso[7] != "-":
        # Fallback: if frontend sends YYYY-MM only, use first of month so API doesn't 400
        if len(date_iso) == 7 and date_iso[4] == "-" and date_iso[:4].isdigit() and date_iso[5:7].isdigit():
            date_iso = date_iso + "-01"
            if _kenna_log:
                print(f"  [Kenna API] date was YYYY-MM, using {date_iso}")
        else:
            if _kenna_log:
                print(f"  [Kenna API] REJECTED: date not YYYY-MM-DD (will not call API)")
            return {"status": "error", "message": "Date must be YYYY-MM-DD", "booking_url": course.get("booking_url", ""), "times": []}
    players = max(1, min(4, int(players))) if players is not None else 4
    booking_url = course.get("booking_url", "") or ""
    # Kenna API expects Origin/Referer/x-be-alias to match the facility (Florida Club vs Atlantic National use different hosts)
    base_url = (course.get("scrape_url") or booking_url or "").strip()
    if base_url and "?" in base_url:
        base_url = base_url.split("?")[0]
    if base_url and base_url.endswith("/"):
        base_url = base_url.rstrip("/")
    origin = base_url or "https://the-florida-club.book.teeitup.golf"
    referer = origin + "/"
    try:
        from urllib.parse import urlparse
        parsed = urlparse(origin)
        be_alias = parsed.netloc.split(".")[0] if parsed.netloc else "the-florida-club"
    except Exception:
        be_alias = "the-florida-club"
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Origin": origin,
        "Referer": referer,
        "x-be-alias": be_alias,
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    }
    try:
        # API requires YYYY-MM-DD; ensure we never send YYYY-MM only (causes 400)
        if len(date_iso) == 7 and date_iso[4] == "-":
            date_iso = date_iso + "-01"
        params = {"date": date_iso, "facilityIds": facility_id}
        # Kenna phx-api may not support players/groupSize; omit to avoid 400
        url_with_params = f"{KENNA_TEETIMES_URL}?date={date_iso}&facilityIds={facility_id}"
        if _kenna_log:
            print(f"  [Kenna API] GET {url_with_params} (players={players} for filter only) Origin={origin} x-be-alias={be_alias}")
        resp = requests.get(
            KENNA_TEETIMES_URL,
            params=params,
            headers=headers,
            timeout=12,
        )
        if _kenna_log:
            print(f"  [Kenna API] response status={resp.status_code} url={resp.url}")
        resp.raise_for_status()
        data = resp.json()
    except requests.exceptions.Timeout:
        return {"status": "error", "message": "API timeout", "booking_url": booking_url, "times": []}
    except Exception as e:
        if _kenna_log:
            print(f"  [Kenna API] error: {e}")
        return {"status": "error", "message": str(e)[:100], "booking_url": booking_url, "times": []}

    # API can return: (1) a list of one object with "teetimes" inside, or (2) a dict with "teetimes"
    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
        raw_list = data[0].get("teetimes") or []
    elif isinstance(data, dict):
        raw_list = data.get("teetimes") or []
    else:
        raw_list = []
    if not isinstance(raw_list, list):
        raw_list = []
    if _kenna_log:
        print(f"  [Kenna API] teetimes count={len(raw_list)} facility_id={facility_id}")
    if _kenna_log and len(raw_list) == 0:
        if isinstance(data, list) and data and isinstance(data[0], dict):
            print(f"  [Kenna API] response was list[0] keys={list(data[0].keys())}")
        elif isinstance(data, dict):
            print(f"  [Kenna API] response keys={list(data.keys())}")

    # Parse ISO teetime (UTC) to Eastern for display
    try:
        from zoneinfo import ZoneInfo
        eastern = ZoneInfo("America/New_York")
    except Exception:
        eastern = None

    times = []
    seen_key = set()
    for slot in raw_list:
        if not isinstance(slot, dict):
            continue
        teetime_iso = slot.get("teetime") or ""
        if not teetime_iso:
            continue
        try:
            dt = datetime.fromisoformat(teetime_iso.replace("Z", "+00:00"))
            if eastern:
                dt = dt.astimezone(eastern)
            hour, minute = dt.hour, dt.minute
            period = "AM" if hour < 12 else "PM"
            h12 = hour if hour <= 12 else hour - 12
            if h12 == 0:
                h12 = 12
            time_display = f"{h12}:{minute:02d} {period}"
            time_key = f"{hour:02d}:{minute:02d}"
        except Exception:
            continue

        # Use allowedPlayers: include slot only when user's selection is in that array
        allowed = slot.get("allowedPlayers") or slot.get("allowed_players")
        if not allowed or not isinstance(allowed, (list, tuple)):
            # Fallback: no allowedPlayers, use min/max
            slot_min = int(slot.get("minPlayers") or slot.get("min_players") or 1)
            slot_max = int(slot.get("maxPlayers") or slot.get("max_players") or 4)
            allowed = list(range(slot_min, slot_max + 1))
        allowed_set = set(int(a) for a in allowed if a is not None)
        if players not in allowed_set:
            continue
        min_players = min(allowed_set) if allowed_set else 1
        max_players = max(allowed_set) if allowed_set else 4
        dedupe_key = (time_key, min_players, max_players)
        if dedupe_key in seen_key:
            continue
        seen_key.add(dedupe_key)
        rates = slot.get("rates") or []
        green_fee = None
        rate_name = ""
        if rates and isinstance(rates[0], dict):
            r0 = rates[0]
            cents = r0.get("greenFeeCart")
            if cents is not None:
                try:
                    green_fee = int(cents) / 100.0
                except (TypeError, ValueError):
                    pass
            rate_name = (r0.get("name") or "").strip()
        times.append({
            "time": time_display,
            "min_players": min_players,
            "max_players": max_players,
            "available_spots": max_players,
            "holes": 18,
            "green_fee": green_fee,
            "cart_fee": None,
            "rate_type": rate_name or "TeeItUp",
            "section": "kenna_api",
        })

    def _sort_key(t):
        s = t.get("time") or ""
        m = re.search(r"(\d{1,2}):(\d{2})\s*(AM|PM)", s, re.I)
        if m:
            h, min_val = int(m.group(1)), int(m.group(2))
            if (m.group(3) or "").upper() == "PM" and h != 12:
                h += 12
            elif (m.group(3) or "").upper() == "AM" and h == 12:
                h = 0
            return (h, min_val)
        return (0, 0)
    times.sort(key=_sort_key)
    if _kenna_log:
        print(f"  [Kenna API] after filters: times_returned={len(times)} facility_id={facility_id}")
    return {"status": "ok", "times": times, "booking_url": booking_url}


# ─────────────────────────────────────────────
# Tee It Up — Selenium (legacy; Kenna API used for Florida Club & Atlantic National)
# ─────────────────────────────────────────────
def _parse_teeitup_player_range(text):
    """Parse Tee It Up 'available players' text: '1', '1 or 2', '1 - 4' -> (min, max)."""
    if not text:
        return 1, 4
    t = str(text).strip()
    # "1 - 4" or "1-4"
    m = re.search(r"(\d+)\s*[-–]\s*(\d+)", t)
    if m:
        return int(m.group(1)), int(m.group(2))
    # "1 or 2"
    m = re.search(r"(\d+)\s+or\s+(\d+)", t, re.I)
    if m:
        return int(m.group(1)), int(m.group(2))
    # single number
    m = re.search(r"(\d+)", t)
    if m:
        n = int(m.group(1))
        return n, n
    return 1, 4


def _fetch_direct_teeitup_with_driver(driver, course, date_iso, players):
    """Load Tee It Up booking page with date, wait for MUI tiles, collect via data-testid selectors."""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    import time as _time
    base = (course.get("scrape_url") or course.get("booking_url", "")).strip()
    if not base:
        return {"status": "error", "message": "No Tee It Up URL", "booking_url": "", "times": []}
    url = base + ("&" if "?" in base else "?") + "date=" + date_iso
    booking_url_with_date = (course.get("booking_url", "") or base).rstrip("/")
    if "?" in booking_url_with_date:
        booking_url_with_date += "&date=" + date_iso
    else:
        booking_url_with_date += "?date=" + date_iso
    time_pat = re.compile(r"^\s*(\d{1,2}):(\d{2})\s*(am|pm|AM|PM)\s*$", re.I)
    name = course.get("name", "TeeItUp")
    try:
        t0 = _time.monotonic()
        driver.get(url)
        _log_timing("page load", t0, name)
        # Minimal initial delay so JS can start; then wait for tiles (returns as soon as they appear; max 28s on Render for slow FC)
        _time.sleep(2.0)
        t1 = _time.monotonic()
        def _has_tiles_or_done(d):
            try:
                return d.execute_script("return document.querySelectorAll(\"[data-testid='teetimes-tile-time']\").length > 0;")
            except Exception:
                return False
        wait_tiles = 28 if _is_render() else 12
        try:
            WebDriverWait(driver, wait_tiles).until(_has_tiles_or_done)
        except Exception:
            pass
        _time.sleep(0.5)
        _log_timing("wait for tiles", t1, name)
        t2 = _time.monotonic()
        seen = set()
        times = []
        tree = _parse_html_with_lxml(driver)
        if tree is not None:
            try:
                for time_node in tree.xpath("//*[@data-testid='teetimes-tile-time']"):
                    try:
                        t_text = (time_node.text_content() or "").strip()
                        if not t_text:
                            continue
                        m = time_pat.match(t_text) or re.search(r"(\d{1,2}):(\d{2})\s*(am|pm|AM|PM)", t_text, re.I)
                        if not m:
                            continue
                        h, min_, period = m.group(1), m.group(2), (m.group(3) or "").upper()
                        t_str = f"{h}:{min_} {period}" if period else f"{h}:{min_}"
                        key = t_str.upper().replace(" ", "")
                        if key in seen:
                            continue
                        seen.add(key)
                        min_p, max_p = 1, 4
                        try:
                            players_nodes = time_node.xpath(".//ancestor::*[@data-testid='teetimes-tile-header-component'][1]//*[@data-testid='teetimes-tile-available-players']")
                            if players_nodes:
                                min_p, max_p = _parse_teeitup_player_range(players_nodes[0].text_content())
                        except Exception:
                            pass
                        times.append({
                            "time": t_str,
                            "min_players": min_p,
                            "max_players": max_p,
                            "available_spots": max_p,
                            "holes": 18,
                            "green_fee": None,
                            "cart_fee": None,
                            "rate_type": "",
                            "section": "teeitup",
                        })
                    except Exception:
                        continue
            except Exception:
                pass
        _log_timing("parse tiles", t2, name)
        if not times:
            all_tiles = driver.find_elements(By.CSS_SELECTOR, "[data-testid='teetimes-tile-time']")
            for time_el in all_tiles[:80]:
                try:
                    t_text = (time_el.text or "").strip()
                    if not t_text:
                        continue
                    m = time_pat.match(t_text) or re.search(r"(\d{1,2}):(\d{2})\s*(am|pm|AM|PM)", t_text, re.I)
                    if not m:
                        continue
                    h, min_, period = m.group(1), m.group(2), (m.group(3) or "").upper()
                    t_str = f"{h}:{min_} {period}" if period else f"{h}:{min_}"
                    key = t_str.upper().replace(" ", "")
                    if key in seen:
                        continue
                    seen.add(key)
                    min_p, max_p = 1, 4
                    try:
                        header = time_el.find_element(By.XPATH, "./ancestor::*[@data-testid='teetimes-tile-header-component'][1]")
                        players_el = header.find_element(By.CSS_SELECTOR, "[data-testid='teetimes-tile-available-players']")
                        min_p, max_p = _parse_teeitup_player_range(players_el.text)
                    except Exception:
                        pass
                    times.append({
                        "time": t_str,
                        "min_players": min_p,
                        "max_players": max_p,
                        "available_spots": max_p,
                        "holes": 18,
                        "green_fee": None,
                        "cart_fee": None,
                        "rate_type": "",
                        "section": "teeitup",
                    })
                except Exception:
                    continue
        # Fallback: regex scan visible body if no tiles found
        if not times:
            body = _selenium_get_visible_text(driver) or ""
            for m in re.finditer(r"\b(\d{1,2}):(\d{2})\s*(am|pm|AM|PM)\b", body, re.I):
                h, min_, period = m.group(1), m.group(2), (m.group(3) or "").upper()
                t_str = f"{h}:{min_} {period}" if period else f"{h}:{min_}"
                key = t_str.upper().replace(" ", "")
                if key not in seen:
                    seen.add(key)
                    times.append({
                        "time": t_str,
                        "min_players": 1,
                        "max_players": 4,
                        "available_spots": 4,
                        "holes": 18,
                        "green_fee": None,
                        "cart_fee": None,
                        "rate_type": "",
                        "section": "teeitup",
                    })
        # Filter by requested party size (slot must allow this many players)
        if times and players is not None:
            times = [t for t in times if (t.get("min_players") or 1) <= players <= (t.get("max_players") or 4)]
        if times:
            return {"status": "ok", "times": times[:120], "booking_url": booking_url_with_date}
        return {"status": "error", "message": "No tee times found", "booking_url": booking_url_with_date, "times": []}
    except Exception as e:
        return {"status": "error", "message": str(e)[:120], "booking_url": booking_url_with_date, "times": []}


def fetch_direct_teeitup(course, date_iso, players):
    """Tee It Up: one-off fetch with its own driver. For batch use fetch_all_direct_teeitup."""
    driver = None
    try:
        driver = _get_driver()
        return _fetch_direct_teeitup_with_driver(driver, course, date_iso, players)
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


def fetch_all_direct_teeitup(courses, date_iso, players, before_time=None, on_course_done=None):
    """Tee It Up: one shared browser for all courses. Calls on_course_done(course_id, result) for each."""
    if not courses:
        return
    driver = None
    try:
        driver = _get_driver()
        for course in courses:
            result = _fetch_direct_teeitup_with_driver(driver, course, date_iso, players)
            result["booking_url"] = (result.get("booking_url") or course.get("booking_url", ""))
            if before_time and result.get("status") == "ok":
                result["times"] = apply_time_filter(result.get("times", []), before_time)
            if result.get("status") == "ok" and result.get("times"):
                for t in result["times"]:
                    if t.get("time"):
                        t["time"] = _normalize_tee_time_display(t["time"])
            if on_course_done:
                on_course_done(course["id"], result)
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


# ─────────────────────────────────────────────
# Club Caddie (Boca Raton Golf & Racquet) — apimanager-cc22.clubcaddie.com
# API: POST /webapi/TeeTimes returns HTML (same markup as slots view). We call it directly and parse the HTML.
# ─────────────────────────────────────────────
CLUBCADDIE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/html, */*",
    "Accept-Language": "en-US,en;q=0.9",
}


def _fetch_direct_clubcaddie_via_api(course, date_iso, players):
    """Try to get Club Caddie tee times via HTTP POST to /webapi/TeeTimes, then parse HTML. Returns result dict or None to fall back to Selenium."""
    base = (course.get("scrape_url") or course.get("booking_url", "")).strip()
    if not base:
        return None
    try:
        parts = date_iso.split("-")
        if len(parts) != 3:
            return None
        date_mmddyyyy = f"{parts[1]}/{parts[2]}/{parts[0]}"
    except Exception:
        return None
    interaction = course.get("clubcaddie_interaction") or ""
    if not interaction:
        return None
    player = max(1, min(4, int(players) if players else 4))

    # Derive host for API and referer/origin
    from urllib.parse import urlparse
    parsed = urlparse(base)
    if not parsed.scheme or not parsed.netloc:
        return None
    api_base = f"{parsed.scheme}://{parsed.netloc}"
    api_url = f"{api_base}/webapi/TeeTimes"
    referer = base
    headers = dict(CLUBCADDIE_HEADERS)
    headers.update({
        "Origin": api_base,
        "Referer": referer,
        "X-Requested-With": "XMLHttpRequest",
    })

    # Form data closely mirrors the browser request; CourseId and apikey are stable for Boca.
    course_id = "103391"
    apikey = course.get("clubcaddie_slug") or "ajedabab"
    form = {
        "date": date_mmddyyyy,
        "player": str(player),
        "holes": "any",
        "fromtime": "4",
        "totime": "23",
        "minprice": "0",
        "maxprice": "9999",
        "ratetype": "any",
        "HoleGroup": "front",
        "CourseId": course_id,
        "apikey": apikey,
        "Interaction": interaction,
    }

    booking_url_with_date = f"{base}?date={date_mmddyyyy.replace('/', '%2F')}&player={player}&ratetype=any&Interaction={interaction}"

    from lxml import html as _html
    import urllib.parse as _urlparse
    try:
        resp = requests.post(api_url, data=form, headers=headers, timeout=16)
        resp.raise_for_status()
        text = resp.text or ""
    except Exception:
        return None

    if not text.strip():
        return None

    try:
        tree = _html.fromstring(text)
    except Exception:
        return None

    times = []
    time_pat = re.compile(r"\b(\d{1,2}):(\d{2})\s*(AM|PM)\b", re.I)
    seen = set()

    # Each tee time is a form with id TeeTimeSlotFormN
    forms = tree.xpath("//form[starts-with(@id, 'TeeTimeSlotForm')]")
    for form_el in forms:
        try:
            # Visible golfers text: e.g. "1" or "1 - 2" or "2 - 4"
            golfers_text = " ".join(
                (form_el.xpath(".//*[contains(@class,'tt-golfers')]")[0].itertext())
            ).strip() if form_el.xpath(".//*[contains(@class,'tt-golfers')]") else ""
            m_range = re.search(r"(\d+)\s*-\s*(\d+)", golfers_text)
            if m_range:
                min_players = int(m_range.group(1))
                max_players = int(m_range.group(2))
            else:
                m_single = re.search(r"\b(\d+)\b", golfers_text)
                val = int(m_single.group(1)) if m_single else player
                min_players = 1
                max_players = val

            # Available / minimum from encoded slot JSON
            slot_val = form_el.xpath(".//input[@name='slot']/@value")
            players_available = None
            min_available = None
            green_fee = None
            if slot_val:
                try:
                    decoded = _urlparse.unquote(slot_val[0])
                    slot_obj = json.loads(decoded)
                    players_available = int(slot_obj.get("PlayersAvailable", 0))
                    min_available = int(slot_obj.get("MinimumPlayersAvailable", 1))
                    pricing = None
                    plan = (slot_obj.get("PricingPlan") or []) or []
                    if plan:
                        pricing = plan[0].get("HoleRate_18_Pricing") or {}
                        if not pricing and plan[0].get("HoleRate_18") is not None:
                            green_fee = float(plan[0].get("HoleRate_18"))
                    if green_fee is None and pricing:
                        gf = pricing.get("GreensFees")
                        if gf is not None:
                            green_fee = float(gf)
                except Exception:
                    pass
            available_spots = players_available if players_available is not None else max_players
            if available_spots < players:
                continue

            # Visible tee time text, e.g. "03:10 PM"
            tt_nodes = form_el.xpath(".//*[contains(@class,'tt-label') or contains(@class,'tee-time-label')]/text()")
            raw_time = " ".join(t.strip() for t in tt_nodes if t.strip())
            m_time = time_pat.search(raw_time)
            if not m_time:
                continue
            h, min_, period = int(m_time.group(1)), m_time.group(2), (m_time.group(3) or "").upper()
            t_str = f"{h}:{min_} {period}"
            key = t_str.upper().replace(" ", "")
            if key in seen:
                continue
            seen.add(key)

            times.append({
                "time": t_str,
                "min_players": min_players,
                "max_players": max_players,
                "available_spots": available_spots,
                "holes": 18,
                "green_fee": green_fee,
                "cart_fee": None,
                "rate_type": "Club Caddie",
                "section": "clubcaddie_api",
            })
        except Exception:
            continue

    return {
        "status": "ok",
        "times": times[:120],
        "booking_url": booking_url_with_date,
    }


def _fetch_direct_clubcaddie_with_driver(driver, course, date_iso, players):
    """Load Club Caddie slots page with date and player count, wait for JS to render slots, collect times from DOM."""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    import time as _time
    base = (course.get("scrape_url") or course.get("booking_url", "")).strip()
    if not base:
        return {"status": "error", "message": "No Club Caddie URL", "booking_url": "", "times": []}
    # Date: YYYY-MM-DD -> MM/DD/YYYY for Club Caddie
    try:
        parts = date_iso.split("-")
        if len(parts) != 3:
            return {"status": "error", "message": "Invalid date", "booking_url": base, "times": []}
        date_mmddyyyy = f"{parts[1]}/{parts[2]}/{parts[0]}"
    except Exception:
        return {"status": "error", "message": "Invalid date", "booking_url": base, "times": []}
    interaction = course.get("clubcaddie_interaction") or ""
    if not interaction:
        return {"status": "error", "message": "No Club Caddie interaction ID", "booking_url": base, "times": []}
    player = max(1, min(4, int(players) if players else 4))
    url = f"{base}?date={date_mmddyyyy.replace('/', '%2F')}&player={player}&ratetype=any&Interaction={interaction}"
    booking_url_with_date = f"{base}?date={date_mmddyyyy.replace('/', '%2F')}&player={player}&ratetype=any&Interaction={interaction}"
    time_pat = re.compile(r"\b(\d{1,2}):(\d{2})\s*(am|pm|AM|PM)\b", re.I)
    name = course.get("name", "Club Caddie")
    try:
        t0 = _time.monotonic()
        driver.get(url)
        _log_timing("page load", t0, name)
        # Minimal initial delay; then wait for slots (returns as soon as they appear; max 28s on Render)
        _time.sleep(2.0)
        t1 = _time.monotonic()
        def _has_slots_or_done(d):
            try:
                slot_box = d.find_elements(By.CSS_SELECTOR, "#SlotBox .teetime, #SlotBox .itembox, .slot-outer-box .teetime")
                if slot_box:
                    for el in slot_box[:5]:
                        if time_pat.search(el.text or ""):
                            return True
                body = (_selenium_get_visible_text(d) or "").lower()
                if "searching tee times" in body and len(time_pat.findall(body)) == 0:
                    return False
                return len(time_pat.findall(body)) >= 1
            except Exception:
                return False
        wait_slots = 28 if _is_render() else 12
        try:
            WebDriverWait(driver, wait_slots).until(_has_slots_or_done)
        except Exception:
            pass
        _time.sleep(0.5)
        _log_timing("wait for slots", t1, name)
        t2 = _time.monotonic()
        seen = set()
        times = []
        tree = _parse_html_with_lxml(driver)
        if tree is not None:
            try:
                # XPath equivalents (no cssselect dependency): .teetime, .itembox.tt-btn, #SlotBox button, etc.
                xpath_list = (
                    "//*[contains(@class, 'teetime')]",
                    "//*[contains(@class, 'itembox') and contains(@class, 'tt-btn')]",
                    "//*[@id='SlotBox']//button",
                    "//*[contains(@class, 'slot-outer-box')]//button",
                    "//*[@id='SlotBox']//*[contains(@class, 'itembox')]",
                )
                for xpath in xpath_list:
                    for node in tree.xpath(xpath):
                        try:
                            text = (node.text_content() or "").strip()
                            if not text or len(text) > 120:
                                continue
                            m = time_pat.search(text)
                            if not m:
                                continue
                            h, min_, period = m.group(1), m.group(2), (m.group(3) or "").upper()
                            t_str = f"{h}:{min_} {period}" if period else f"{h}:{min_}"
                            key = t_str.upper().replace(" ", "")
                            if key in seen:
                                continue
                            seen.add(key)
                            times.append({
                                "time": t_str,
                                "min_players": 1,
                                "max_players": 4,
                                "available_spots": 4,
                                "holes": 18,
                                "green_fee": None,
                                "cart_fee": None,
                                "rate_type": "",
                                "section": "clubcaddie",
                            })
                        except Exception:
                            continue
                    if times:
                        break
            except Exception:
                pass
        _log_timing("parse slots", t2, name)
        if not times:
            for selector in (".teetime", ".itembox.tt-btn", "#SlotBox button", ".slot-outer-box button", "#SlotBox .itembox"):
                try:
                    for el in driver.find_elements(By.CSS_SELECTOR, selector):
                        try:
                            text = (el.text or "").strip()
                            if not text or len(text) > 120:
                                continue
                            m = time_pat.search(text)
                            if not m:
                                continue
                            h, min_, period = m.group(1), m.group(2), (m.group(3) or "").upper()
                            t_str = f"{h}:{min_} {period}" if period else f"{h}:{min_}"
                            key = t_str.upper().replace(" ", "")
                            if key in seen:
                                continue
                            seen.add(key)
                            times.append({
                                "time": t_str,
                                "min_players": 1,
                                "max_players": 4,
                                "available_spots": 4,
                                "holes": 18,
                                "green_fee": None,
                                "cart_fee": None,
                                "rate_type": "",
                                "section": "clubcaddie",
                            })
                        except Exception:
                            continue
                except Exception:
                    continue
                if times:
                    break
        if not times:
            body = _selenium_get_visible_text(driver) or ""
            for m in time_pat.finditer(body):
                h, min_, period = m.group(1), m.group(2), (m.group(3) or "").upper()
                t_str = f"{h}:{min_} {period}" if period else f"{h}:{min_}"
                key = t_str.upper().replace(" ", "")
                if key not in seen:
                    seen.add(key)
                    times.append({
                        "time": t_str,
                        "min_players": 1,
                        "max_players": 4,
                        "available_spots": 4,
                        "holes": 18,
                        "green_fee": None,
                        "cart_fee": None,
                        "rate_type": "",
                        "section": "clubcaddie",
                    })
        if times:
            return {"status": "ok", "times": times[:120], "booking_url": booking_url_with_date}
        return {"status": "error", "message": "No tee times found", "booking_url": booking_url_with_date, "times": []}
    except Exception as e:
        return {"status": "error", "message": str(e)[:120], "booking_url": base, "times": []}


def fetch_direct_clubcaddie(course, date_iso, players):
    """Club Caddie: one-off fetch with its own driver. For batch use fetch_all_direct_clubcaddie."""
    driver = None
    try:
        driver = _get_driver()
        return _fetch_direct_clubcaddie_with_driver(driver, course, date_iso, players)
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


def fetch_all_direct_clubcaddie(courses, date_iso, players, before_time=None, on_course_done=None):
    """Club Caddie: one shared browser for all courses. Calls on_course_done(course_id, result) for each."""
    if not courses:
        return
    driver = None
    try:
        driver = _get_driver()
        for course in courses:
            result = _fetch_direct_clubcaddie_with_driver(driver, course, date_iso, players)
            result["booking_url"] = (result.get("booking_url") or course.get("booking_url", ""))
            if before_time and result.get("status") == "ok":
                result["times"] = apply_time_filter(result.get("times", []), before_time)
            if result.get("status") == "ok" and result.get("times"):
                for t in result["times"]:
                    if t.get("time"):
                        t["time"] = _normalize_tee_time_display(t["time"])
            if on_course_done:
                on_course_done(course["id"], result)
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


# ─────────────────────────────────────────────
# Eagle Club (Boynton Beach Links) — API (no Selenium)
# ─────────────────────────────────────────────
BOYNTON_BOOKING_URL = "https://player.eagleclubsystems.online/#/tee-slot?dbname=labb20241201"

def _fetch_boynton_beach_api(date_iso, players):
    """
    Direct API call to Eagle Club - replaces Selenium scraper.
    Only returns Championship course times; filters by requested player count.
    """
    date_eagle = date_iso.replace("-", "")
    # Request all slots for the day; we filter by before_time on our side (StrTime = current time often returns nothing due to timezone)
    str_time = "0000"
    players = max(1, min(4, int(players))) if players is not None else 4

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://player.eagleclubsystems.online",
        "Referer": "https://player.eagleclubsystems.online/",
    }
    payload = {
        "BCC": {
            "StrServer": "GSERVER",
            "StrURL": "https://api.eagleclubsystems.online",
            "StrDatabase": "labb20241201",
        },
        "IncludeExisting": False,
        "Master_CarriageID": 158,
        "Master_TeePriceClassIDs": ",85,",
        "OnlineBookingFormat": 0,
        "OnlineBookingMaxDays": 8,
        "StrDate": date_eagle,
        "StrTime": str_time,
        "TeePriceClassID": 85,
    }

    _boynton_log = os.environ.get("BOYNTON_DEBUG", "").strip().lower() in ("1", "true", "yes")
    if _boynton_log:
        print(f"  [Boynton API] request date_iso={date_iso} date_eagle={date_eagle} str_time={str_time} players={players}")

    try:
        response = requests.post(
            "https://api.eagleclubsystems.online/api/online/OnlineAppointmentRetrieve",
            json=payload,
            headers=headers,
            timeout=10,
        )
        response.raise_for_status()
        raw = response.json()
    except requests.exceptions.Timeout:
        if _boynton_log:
            print("  [Boynton API] TIMEOUT")
        return {"status": "error", "message": "API timeout", "booking_url": BOYNTON_BOOKING_URL, "times": []}
    except Exception as e:
        if _boynton_log:
            print(f"  [Boynton API] request error: {e}")
        return {"status": "error", "message": str(e)[:100], "booking_url": BOYNTON_BOOKING_URL, "times": []}

    # API returns a list of slot objects, or a dict with LstAppointment/LstAppointmentAll/BG/etc.
    if isinstance(raw, list):
        data = raw
        if _boynton_log:
            print(f"  [Boynton API] response type=list len={len(data)}")
    elif isinstance(raw, dict):
        if _boynton_log:
            print(f"  [Boynton API] response type=dict keys={list(raw.keys())}")
        # Prefer known slot list keys (often empty; then try BG)
        data = raw.get("LstAppointmentAll") or raw.get("LstAppointment") or raw.get("Data") or raw.get("Appointments") or raw.get("data")
        if not isinstance(data, list) or (data and not isinstance(data[0], dict)):
            data = []
        # If still empty, try BG (nested structure)
        if not data and isinstance(raw.get("BG"), dict):
            bg = raw["BG"]
            if _boynton_log:
                print(f"  [Boynton API] BG keys={list(bg.keys())}")
            for v in bg.values():
                if isinstance(v, list) and v and isinstance(v[0], dict) and ("Time" in v[0] or "NineName" in v[0]):
                    data = v
                    if _boynton_log:
                        print(f"  [Boynton API] using BG list len={len(data)}")
                    break
        if not data:
            # Find any value that is a list of slot dicts
            for v in raw.values():
                if isinstance(v, list) and v and isinstance(v[0], dict) and ("Time" in v[0] or "NineName" in v[0]):
                    data = v
                    break
            else:
                if _boynton_log:
                    for k, v in raw.items():
                        t = type(v).__name__
                        if isinstance(v, list):
                            t += f" len={len(v)}"
                            if v and isinstance(v[0], dict):
                                t += f" first_keys={list(v[0].keys())[:8]}"
                        print(f"  [Boynton API] dict value {k!r}: {t}")
    else:
        data = []
        if _boynton_log:
            print(f"  [Boynton API] response type={type(raw).__name__} (not list/dict)")

    times = []
    seen_time_key = set()
    n_championship = 0
    n_players_ok = 0
    for slot in data:
        if not isinstance(slot, dict):
            continue
        # Only the date user selected (API can return multiple dates)
        slot_date = str(slot.get("Date", "")).strip().replace("-", "")
        if slot_date != date_eagle:
            continue
        course_name = (slot.get("NineName") or "").strip()
        if course_name.lower() != "championship":
            continue
        n_championship += 1
        time_str = str(slot.get("Time", "")).strip()
        if len(time_str) != 4 or not time_str.isdigit():
            continue
        hour = int(time_str[:2])
        minute = time_str[2:]
        period = "AM" if hour < 12 else "PM"
        display_hour = hour if hour <= 12 else hour - 12
        if display_hour == 0:
            display_hour = 12
        time_display = f"{display_hour}:{minute} {period}"
        time_key = f"{hour:02d}:{minute}"
        if time_key in seen_time_key:
            continue
        seen_time_key.add(time_key)

        players_booked = len(slot.get("LstPlayer", []))
        total_slots = int(slot.get("Slots", 4))
        available_spots = total_slots - players_booked
        if available_spots < players:
            continue
        n_players_ok += 1

        fee = slot.get("EighteenFee") or slot.get("NineFee")
        green_fee = None
        if fee is not None:
            try:
                green_fee = float(fee)
            except (TypeError, ValueError):
                pass

        times.append({
            "time": time_display,
            "min_players": 1,
            "max_players": total_slots,
            "available_spots": available_spots,
            "holes": 18,
            "green_fee": green_fee,
            "cart_fee": None,
            "rate_type": "Championship",
            "section": "eagleclub_api",
            "_sort_key": (hour, int(minute)),
        })

    # Chronological order (early to late)
    times.sort(key=lambda t: t.pop("_sort_key", (0, 0)))

    if _boynton_log:
        print(f"  [Boynton API] slots total={len(data)} championship={n_championship} players_ok={n_players_ok} times_returned={len(times)}")
    if not times:
        print(f"  [Boynton API] no times (raw_type={type(raw).__name__} data_len={len(data)} championship={n_championship} players_ok={n_players_ok}) — set BOYNTON_DEBUG=1 for full request/response")
        if isinstance(raw, dict) and len(data) == 0 and raw.get("LstAppointmentAll") is not None and len(raw.get("LstAppointmentAll", [])) == 0:
            print(f"  [Boynton API] LstAppointmentAll is empty — API accepted request but returned no slots. Check browser Network tab on player.eagleclubsystems.online for the real payload (Master_CarriageID, etc.).")
    return {"status": "ok", "times": times, "booking_url": BOYNTON_BOOKING_URL}


# Legacy Selenium path (kept for reference; not used when API is used)
def _fetch_direct_eagleclub_with_driver(driver, course, date_iso, players):
    """Eagle Club: load page, click date card for target date, click player count (1-4), set dropdown to Championship, parse tee time cards."""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait, Select as SelSelect
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.action_chains import ActionChains
    import time as _time
    import datetime as _dt
    base = (course.get("scrape_url") or course.get("booking_url", "")).strip()
    if not base:
        return {"status": "error", "message": "No Eagle Club URL", "booking_url": "", "times": []}
    booking_url = base
    # Match "10:52 AM" or "01:45 PM" in card headers
    time_pat = re.compile(r"\b(\d{1,2}):(\d{2})\s*(am|pm|AM|PM)\b", re.I)
    price_pat = re.compile(r"\$[\d,.]+")
    try:
        driver.get(base)
        # Wait for SPA: Filter Options and main content
        try:
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.XPATH, "//*[contains(translate(., 'FILTER', 'filter'), 'filter')]"))
            )
        except Exception:
            pass
        _time.sleep(3)

        # Parse target date for card match: cards show "Sat 03/14" or "Saturday, 03/14/2026" — we need MM/DD
        target_mm_dd = None
        target_day_abbr = None
        try:
            parts = date_iso.split("-")
            if len(parts) == 3:
                y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
                target_mm_dd = f"{m:02d}/{d:02d}"
                target_day_abbr = _dt.date(y, m, d).strftime("%a")  # Mon, Tue, ...
        except Exception:
            pass

        # 1) Set date — click the date CARD that matches target (horizontal strip: "Tue 03/10", "Wed 03/11", ...)
        if target_mm_dd:
            try:
                # Prefer: clickable element whose text contains MM/DD (e.g. "Sat 03/14")
                for el in driver.find_elements(By.XPATH, "//*[contains(., '%s')]" % target_mm_dd):
                    try:
                        t = (el.text or "").strip()
                        if target_mm_dd not in t or "Time" in t or "Filter" in t:
                            continue
                        if not el.is_displayed():
                            continue
                        # Avoid clicking the big "Saturday, 03/14/2026" label; prefer the small card
                        if len(t) > 20 and target_mm_dd in t and ("/" in t.replace(target_mm_dd, "", 1)):
                            continue
                        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                        _time.sleep(0.2)
                        ActionChains(driver).move_to_element(el).click().perform()
                        _time.sleep(1.2)
                        break
                    except Exception:
                        continue
            except Exception:
                pass

        # 2) Set number of players — Filter Options: buttons "ANY", "1", "2", "3", "4"; click the number
        players_val = max(1, min(4, int(players) if players else 4))
        try:
            num = str(players_val)
            candidates = driver.find_elements(
                By.XPATH,
                "//*[normalize-space(text())='%s' and (self::button or self::a or self::div or self::span)]" % num
            )
            for el in candidates:
                try:
                    if not el.is_displayed():
                        continue
                    # Skip if this is part of a date (e.g. "03/14") or other control
                    parent_text = ""
                    try:
                        parent = el.find_element(By.XPATH, "./..")
                        parent_text = (parent.get_attribute("innerText") or "").lower()
                    except Exception:
                        pass
                    if "player" not in parent_text and "filter" not in parent_text:
                        try:
                            grand = el.find_element(By.XPATH, "./../..")
                            if "player" not in (grand.get_attribute("innerText") or "").lower():
                                continue
                        except Exception:
                            continue
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                    _time.sleep(0.15)
                    el.click()
                    _time.sleep(0.8)
                    break
                except Exception:
                    continue
        except Exception:
            pass

        # 3) Choose Course — dropdown: select "Championship" only (not "Championship Back")
        try:
            # Native <select>: pick option whose text is exactly "Championship" (exclude "Championship Back")
            for sel_el in driver.find_elements(By.CSS_SELECTOR, "select"):
                try:
                    if not sel_el.is_displayed():
                        continue
                    opts = sel_el.find_elements(By.TAG_NAME, "option")
                    chosen = None
                    for opt in opts:
                        t = (opt.text or "").strip().lower()
                        if "back" in t:
                            continue
                        if t == "championship":
                            chosen = opt.text.strip()
                            break
                        if t and "championship" in t and chosen is None:
                            chosen = opt.text.strip()
                    if chosen:
                        SelSelect(sel_el).select_by_visible_text(chosen)
                        _time.sleep(0.8)
                        break
                except Exception:
                    continue
            else:
                # Custom dropdown: click the option that says "Championship" exactly, not "Championship Back"
                for el in driver.find_elements(By.XPATH, "//*[contains(translate(., 'CHAMPIONSHIP', 'championship'), 'championship')]"):
                    try:
                        if not el.is_displayed():
                            continue
                        t = (el.text or "").strip().lower()
                        if "back" in t:
                            continue
                        if t != "championship":
                            continue
                        tag = el.tag_name.lower()
                        if tag in ("button", "div", "span", "a", "li"):
                            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                            _time.sleep(0.15)
                            el.click()
                            _time.sleep(0.6)
                            break
                    except Exception:
                        continue
        except Exception:
            pass

        _time.sleep(2)

        # 4) Parse tee time cards — grid of cards: green header with time (e.g. "10:52 AM"), body with Championship, price, "4 Players"
        seen = set()
        times = []
        # Find all elements that contain a time (card header or whole card)
        for el in driver.find_elements(By.XPATH, "//*[contains(., 'AM') or contains(., 'PM')]"):
            try:
                text = (el.text or "").strip()
                if not text or len(text) > 250:
                    continue
                m = time_pat.search(text)
                if not m:
                    continue
                # Reject sidebar / filter controls
                if "Filter Options" in text or ("Reset" in text and "Time" in text) or ("7AM" in text and "6PM" in text):
                    continue
                h, min_, period = m.group(1), m.group(2), (m.group(3) or "").upper()
                t_str = f"{int(h)}:{min_} {period}"
                key = t_str.upper().replace(" ", "")
                if key in seen:
                    continue
                # Accept if it looks like a tee slot: has time and (Championship or Players or $) or is a short time-only block (card header)
                is_card = "championship" in text.lower() or "player" in text.lower() or "$" in text
                is_header_only = len(text) < 25 and time_pat.search(text)
                if not is_card and not is_header_only:
                    continue
                seen.add(key)
                price_match = price_pat.search(text)
                green_fee = None
                if price_match:
                    try:
                        green_fee = float(price_match.group(0).replace("$", "").replace(",", ""))
                    except Exception:
                        pass
                times.append({
                    "time": t_str,
                    "min_players": 1,
                    "max_players": 4,
                    "available_spots": 4,
                    "holes": 18,
                    "green_fee": green_fee,
                    "cart_fee": None,
                    "rate_type": "",
                    "section": "eagleclub",
                })
            except Exception:
                continue

        # Sort by time and dedupe by key
        def _time_key(t):
            s = t.get("time") or ""
            m = time_pat.search(s)
            if not m:
                return (99, 99)
            h, min_, period = int(m.group(1)), int(m.group(2)), (m.group(3) or "").upper()
            if "PM" in period and h != 12:
                h += 12
            elif "AM" in period and h == 12:
                h = 0
            return (h, min_)

        times.sort(key=_time_key)
        if times:
            return {"status": "ok", "times": times[:120], "booking_url": booking_url}

        # Fallback: any element with a time pattern in main content
        body = _selenium_get_visible_text(driver) or ""
        for m in time_pat.finditer(body):
            h, min_, period = m.group(1), m.group(2), (m.group(3) or "").upper()
            t_str = f"{int(h)}:{min_} {period}"
            key = t_str.upper().replace(" ", "")
            if key not in seen:
                seen.add(key)
                times.append({
                    "time": t_str,
                    "min_players": 1,
                    "max_players": 4,
                    "available_spots": 4,
                    "holes": 18,
                    "green_fee": None,
                    "cart_fee": None,
                    "rate_type": "",
                    "section": "eagleclub",
                })
        times.sort(key=_time_key)
        if times:
            return {"status": "ok", "times": times[:120], "booking_url": booking_url}
        return {"status": "error", "message": "No tee times found", "booking_url": booking_url, "times": []}
    except Exception as e:
        return {"status": "error", "message": str(e)[:120], "booking_url": base, "times": []}


def _fetch_one_direct_course(course, date_iso, players, before_time=None):
    """Fetch one direct course (GolfNow, TeeItUp, Club Caddie, or Eagle Club). Club Caddie and Eagle Club try HTTP API first."""
    driver = None
    scraper = course.get("direct_scraper") or ""
    # Eagle Club (Boynton Beach): API only, no browser
    if scraper == "eagleclub":
        result = _fetch_boynton_beach_api(date_iso, players)
        result["booking_url"] = result.get("booking_url") or course.get("booking_url", "")
        if before_time and result.get("status") == "ok":
            result["times"] = apply_time_filter(result.get("times", []), before_time)
        if result.get("status") == "ok" and result.get("times"):
            for t in result["times"]:
                if t.get("time"):
                    t["time"] = _normalize_tee_time_display(t["time"])
        return (course["id"], result)
    # Club Caddie: try HTTP API first (same URL as widget; no browser needed)
    if scraper == "clubcaddie":
        result = _fetch_direct_clubcaddie_via_api(course, date_iso, players)
        if result is not None:
            result["booking_url"] = result.get("booking_url") or course.get("booking_url", "")
            if before_time and result.get("status") == "ok":
                result["times"] = apply_time_filter(result.get("times", []), before_time)
            if result.get("status") == "ok" and result.get("times"):
                for t in result["times"]:
                    if t.get("time"):
                        t["time"] = _normalize_tee_time_display(t["time"])
            return (course["id"], result)
    # Tee It Up (Florida Club, Atlantic National): Kenna API (no browser)
    if scraper == "teeitup":
        result = _fetch_teeitup_kenna_api(course, date_iso, players)
        result["booking_url"] = result.get("booking_url") or course.get("booking_url", "")
        if before_time and result.get("status") == "ok":
            result["times"] = apply_time_filter(result.get("times", []), before_time)
        if result.get("status") == "ok" and result.get("times"):
            for t in result["times"]:
                if t.get("time"):
                    t["time"] = _normalize_tee_time_display(t["time"])
        return (course["id"], result)
    try:
        driver = _get_driver()
        if scraper == "golfnow":
            result = _fetch_direct_golfnow_with_driver(driver, course, date_iso, players)
        elif scraper == "teeitup":
            result = _fetch_direct_teeitup_with_driver(driver, course, date_iso, players)
        elif scraper == "clubcaddie":
            result = _fetch_direct_clubcaddie_with_driver(driver, course, date_iso, players)
        else:
            result = {"status": "error", "message": "No scraper configured", "booking_url": course.get("booking_url", ""), "times": []}
        result["booking_url"] = result.get("booking_url") or course.get("booking_url", "")
        if before_time and result.get("status") == "ok":
            result["times"] = apply_time_filter(result.get("times", []), before_time)
        if result.get("status") == "ok" and result.get("times"):
            for t in result["times"]:
                if t.get("time"):
                    t["time"] = _normalize_tee_time_display(t["time"])
        return (course["id"], result)
    except Exception as e:
        msg = str(e)
        if "receiving message from renderer" in msg.lower() or "timeout" in msg.lower():
            msg = "Page timed out; try again or book on the course site."
        return (course["id"], {"status": "error", "message": msg[:100], "booking_url": course.get("booking_url", ""), "times": []})
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


def fetch_all_direct_parallel(courses, date_iso, players, before_time=None, on_course_done=None):
    """Run all direct courses; when 1 browser at a time, run sequentially with per-course timeout so one hang doesn't block the rest."""
    if not courses:
        return
    lock = threading.Lock()
    def _done(course_id, result):
        if on_course_done:
            with lock:
                on_course_done(course_id, result)

    max_workers = _max_browser_workers()
    # Per-course timeout: enough for TeeItUp/Club Caddie (Boynton uses API, no browser)
    base_timeout = 58 if _is_render() else 55

    if max_workers == 1:
        # Sequential: run one course at a time with timeout so every course gets a result
        for course in courses:
            out = [None]

            def _run():
                try:
                    out[0] = _fetch_one_direct_course(course, date_iso, players, before_time)
                except Exception as e:
                    out[0] = (course["id"], {"status": "error", "message": str(e)[:100], "booking_url": course.get("booking_url", ""), "times": []})

            t = threading.Thread(target=_run)
            t.start()
            course_timeout = base_timeout
            t.join(timeout=course_timeout)
            if out[0] is not None:
                course_id, result = out[0]
                _done(course_id, result)
            else:
                _request_log(f"TIMEOUT: Direct {course.get('name', '')} (id={course['id']}) after {course_timeout}s (per-course limit)")
                _done(course["id"], {"status": "error", "message": "Timed out (%ss)" % course_timeout, "booking_url": course.get("booking_url", ""), "times": []})
        return

    from concurrent.futures import ThreadPoolExecutor, as_completed
    max_workers = min(len(courses), max_workers)
    timeout_direct = max(120, 60 * len(courses))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_fetch_one_direct_course, c, date_iso, players, before_time): c for c in courses}
        try:
            for future in as_completed(futures, timeout=timeout_direct):
                try:
                    course_id, result = future.result(timeout=5)
                    _done(course_id, result)
                except Exception as e:
                    course = futures.get(future)
                    if course:
                        _done(course["id"], {"status": "error", "message": str(e)[:100], "booking_url": course.get("booking_url", ""), "times": []})
        except Exception:
            for future, course in futures.items():
                if not future.done():
                    _done(course["id"], {"status": "error", "message": "Timeout or error", "booking_url": course.get("booking_url", ""), "times": []})


def fetch_direct_eagleclub(course, date_iso, players):
    """Eagle Club: one-off fetch with its own driver."""
    driver = None
    try:
        driver = _get_driver()
        return _fetch_direct_eagleclub_with_driver(driver, course, date_iso, players)
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


def fetch_direct_times(course, date_iso, players, before_time=None):
    """Dispatch to the correct direct scraper (Tee It Up, Club Caddie, Eagle Club API, etc.)."""
    scraper = course.get("direct_scraper") or ""
    if scraper == "golfnow":
        result = fetch_direct_golfnow(course, date_iso, players)
    elif scraper == "teeitup":
        result = _fetch_teeitup_kenna_api(course, date_iso, players)
        result["booking_url"] = result.get("booking_url") or course.get("booking_url", "")
    elif scraper == "clubcaddie":
        result = fetch_direct_clubcaddie(course, date_iso, players)
    elif scraper == "eagleclub":
        result = _fetch_boynton_beach_api(date_iso, players)
        result["booking_url"] = result.get("booking_url") or course.get("booking_url", "")
    else:
        result = {"status": "error", "message": "No scraper configured", "booking_url": course.get("booking_url", ""), "times": []}
    if before_time and result.get("status") == "ok":
        result["times"] = apply_time_filter(result.get("times", []), before_time)
    # Normalize time strings to consistent "10:18am" / "1:00pm" so UI displays and parses correctly
    if result.get("status") == "ok" and result.get("times"):
        for t in result["times"]:
            if t.get("time"):
                t["time"] = _normalize_tee_time_display(t["time"])
    result["booking_url"] = result.get("booking_url") or course.get("booking_url", "")
    return result


# ─────────────────────────────────────────────
# TIME FILTER — include times at or before cutoff (e.g. "16:00" = 4:00 PM or earlier; times after 4:00 PM excluded)
# ─────────────────────────────────────────────
def _parse_slot_time(raw_str):
    """Parse slot time string to datetime.time. Returns None on failure."""
    if not raw_str:
        return None
    raw = str(raw_str).strip()
    if not raw:
        return None
    # Regex fallback: 1:00 PM, 12:30am, 9:45AM (always works for H:MM or HH:MM + AM/PM)
    m = re.search(r"(\d{1,2}):(\d{2})\s*(AM|PM)", raw, re.I)
    if m:
        h, min_val = int(m.group(1)), int(m.group(2))
        is_pm = (m.group(3) or "").upper() == "PM"
        if h == 12:
            h = 12 if is_pm else 0  # 12:00 PM -> noon (12), 12:00 AM -> midnight (0)
        elif is_pm:
            h += 12
        if 0 <= h <= 23 and 0 <= min_val <= 59:
            return datetime.strptime(f"{h:02d}:{min_val:02d}", "%H:%M").time()
    raw_compact = raw.replace(" ", "").upper()
    if not raw_compact:
        return None
    # 12-hour with am/pm: 1:00PM, 11:30AM
    if re.search(r"[AP]M", raw_compact):
        for fmt in ("%I:%M%p", "%I%p", "%I:%M %p"):
            try:
                return datetime.strptime(raw_compact, fmt).time()
            except ValueError:
                continue
    # 24-hour HH:MM, H:MM, or HH:MM:SS
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return datetime.strptime(raw_compact, fmt).time()
        except ValueError:
            continue
    return None


def _normalize_tee_time_display(raw_str):
    """Return a consistent 12h display string (e.g. '10:18am', '1:00pm') so UI and filter behave the same. Leaves raw_str unchanged if unparseable."""
    if not raw_str:
        return raw_str
    t = _parse_slot_time(str(raw_str).strip())
    if t is None:
        return raw_str
    h12 = t.hour % 12 or 12
    period = "am" if t.hour < 12 else "pm"
    return f"{h12}:{t.minute:02d}{period}"


def apply_time_filter(times, before_time_str):
    if not before_time_str or not str(before_time_str).strip():
        return times
    raw_cutoff = str(before_time_str).strip().upper()
    cutoff = None
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            cutoff = datetime.strptime(raw_cutoff, fmt).time()
            break
        except ValueError:
            continue
    if cutoff is None:
        for fmt in ("%I:%M %p", "%I:%M%p", "%I %p"):
            try:
                cutoff = datetime.strptime(raw_cutoff, fmt).time()
                break
            except ValueError:
                continue
    if cutoff is None:
        return times

    filtered = []
    for t in times:
        raw_time = t.get("time", "")
        slot_time = _parse_slot_time(raw_time)
        if slot_time is None:
            continue
        if slot_time <= cutoff:
            filtered.append(t)
    return filtered


# ─────────────────────────────────────────────
# DISPATCHER
# ─────────────────────────────────────────────
def fetch_course(course, date_iso, players, before_time=None):
    # #region agent log
    _debug_log("fetch_course:entry", "course", {"course_id": course["id"], "name": course.get("name"), "type": course["type"], "date": date_iso, "players": players}, "fetch")
    # #endregion
    try:
        dt = datetime.strptime(date_iso, "%Y-%m-%d")
        foreup_date = dt.strftime("%m-%d-%Y")
    except ValueError:
        out = {"status": "error", "message": "Invalid date", "booking_url": course.get("booking_url", ""), "times": []}
        _debug_log("fetch_course:exit", "invalid_date", {"course_id": course["id"], "message": out["message"]}, "fetch")
        return out

    if course["type"] == "foreup":
        result = fetch_foreup_times(course, foreup_date, players)
    elif course["type"] == "chronogolf":
        result = fetch_chronogolf_times(course, date_iso, players)
    elif course["type"] == "direct":
        result = fetch_direct_times(course, date_iso, players, before_time)
    else:
        result = {"status": "error", "message": "Unknown type", "times": []}

    # Always apply before_time cutoff in one place for all course types (ForeUp, Chronogolf, direct)
    if before_time and result.get("status") == "ok" and result.get("times") is not None:
        result["times"] = apply_time_filter(result.get("times", []), before_time)

    result["booking_url"] = course.get("booking_url", "")
    return result


# ─────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────
@app.route("/api/courses")
def get_courses():
    return jsonify(COURSES)


@app.route("/api/teetimes")
def get_teetimes():
    course_id = request.args.get("course_id", type=int)
    date_str = request.args.get("date")
    players = request.args.get("players", 4, type=int)
    before_time = request.args.get("before_time") or None

    if not course_id or not date_str:
        return jsonify({"status": "error", "message": "Missing course_id or date"}), 400
    course = next((c for c in COURSES if c["id"] == course_id), None)
    if not course:
        return jsonify({"status": "error", "message": "Course not found"}), 404

    return jsonify(fetch_course(course, date_str, players, before_time))


@app.route("/api/all_teetimes")
def get_all_teetimes():
    """
    ForeUp courses: parallel threads (~1-3s)
    Chronogolf courses: single shared browser visiting each sequentially (~30-60s total)
    """
    date_str = request.args.get("date")
    players = request.args.get("players", 4, type=int)
    before_time = request.args.get("before_time") or None
    # #region agent log
    _debug_log("get_all_teetimes", "request", {"date": date_str, "players": players, "before_time": before_time, "repr": repr(before_time)}, "H1")
    # #endregion
    import time as _req_time
    request_start = _req_time.monotonic()
    _request_log(f"all_teetimes start date={date_str} players={players}")
    print(f"  [all_teetimes] date={date_str} players={players} before_time={before_time!r}")

    if not date_str:
        return jsonify({"status": "error", "message": "Missing date"}), 400

    results = {}
    lock = threading.Lock()

    foreup_courses = [c for c in COURSES if c["type"] == "foreup"]
    chrono_courses = [c for c in COURSES if c["type"] == "chronogolf"]
    direct_courses = [c for c in COURSES if c["type"] == "direct"]
    # Slowest direct courses: Boca Raton Golf & Racquet Club (id=13) and Boynton Beach Links (id=14) should run last overall.
    boca_course = next((c for c in direct_courses if c["id"] == 13), None)
    boynton_course = next((c for c in direct_courses if c["id"] == 14), None)
    direct_fast = [c for c in direct_courses if c["id"] not in (13, 14)]

    def course_worker(course):
        result = fetch_course(course, date_str, players, before_time)
        with lock:
            results[course["id"]] = result

    # ForeUp + Chronogolf (both HTTP, fast) in parallel; then browser for direct only.
    # On Render only: delay browser start by 3s so ForeUp/Chronogolf get a head start without Chrome memory contention
    api_courses = foreup_courses + chrono_courses
    api_threads = [threading.Thread(target=course_worker, args=(c,)) for c in api_courses]
    for t in api_threads:
        t.start()

    chrono_results = {}
    def browser_worker():
        nonlocal chrono_results
        if direct_fast:
            def direct_done(cid, res):
                with lock:
                    results[cid] = res
            fetch_all_direct_parallel(direct_fast, date_str, players, before_time=before_time, on_course_done=direct_done)
        chrono_results = {}  # Chronogolf runs in api_threads, not here
        # Run slowest courses sequentially at the end so they never delay others.
        for slow_course in [c for c in (boynton_course, boca_course) if c]:
            def slow_done(cid, res):
                with lock:
                    results[cid] = res
            fetch_all_direct_parallel([slow_course], date_str, players, before_time=before_time, on_course_done=slow_done)

    if _is_render():
        def delayed_browser():
            import time as _t
            _t.sleep(3)
            browser_worker()
        browser_thread = threading.Thread(target=delayed_browser)
    else:
        browser_thread = threading.Thread(target=browser_worker)
    browser_thread.start()

    for t in api_threads:
        t.join(timeout=15)
    api_elapsed = _req_time.monotonic() - request_start
    api_done = [c["id"] for c in api_courses if c["id"] in results]
    api_missing = [c["id"] for c in api_courses if c["id"] not in results]
    if api_missing:
        _request_log(f"ForeUp/Chronogolf join: {len(api_done)}/{len(api_courses)} completed in {api_elapsed:.1f}s; TIMED OUT ids={api_missing}")
    else:
        _request_log(f"ForeUp/Chronogolf join: all {len(api_courses)} completed in {api_elapsed:.1f}s")

    n_browser_courses = len(direct_courses)
    browser_join = max(180, 50 * n_browser_courses) if _max_browser_workers() == 1 else 180
    browser_thread.join(timeout=browser_join)
    results.update(chrono_results)
    _request_log(f"browser_thread join done (timeout was {browser_join}s)")

    # Fill in any missing (timeouts; direct courses already set above)
    for course in COURSES:
        if course["id"] not in results:
            ctype = "ForeUp" if course.get("type") == "foreup" else "Chronogolf" if course.get("type") == "chronogolf" else "Direct"
            _request_log(f"TIMEOUT: {course.get('name', '')} (id={course['id']}) [{ctype}] — filling 'Timed out'")
            results[course["id"]] = {
                "status": "error",
                "message": "Timed out",
                "booking_url": course.get("booking_url", ""),
                "times": [],
            }

    total_elapsed = _req_time.monotonic() - request_start
    _request_log(f"all_teetimes done in {total_elapsed:.1f}s")
    return jsonify(results)


@app.route("/api/all_teetimes_stream")
def get_all_teetimes_stream():
    """Stream one NDJSON line per course as results become available. No timeout per course."""
    date_str = request.args.get("date")
    players = request.args.get("players", 4, type=int)
    before_time = request.args.get("before_time") or None
    if not date_str:
        return jsonify({"status": "error", "message": "Missing date"}), 400

    foreup_courses = [c for c in COURSES if c["type"] == "foreup"]
    chrono_courses = [c for c in COURSES if c["type"] == "chronogolf"]
    direct_courses = [c for c in COURSES if c["type"] == "direct"]
    boca_course = next((c for c in direct_courses if c["id"] == 13), None)
    boynton_course = next((c for c in direct_courses if c["id"] == 14), None)
    direct_fast = [c for c in direct_courses if c["id"] not in (13, 14)]
    q = Queue()
    total = len(COURSES)

    def course_worker(course):
        result = fetch_course(course, date_str, players, before_time)
        q.put((course["id"], result))

    # ForeUp + Chronogolf (HTTP) each get their own thread; browser_worker does direct only
    for c in foreup_courses + chrono_courses:
        threading.Thread(target=course_worker, args=(c,), daemon=True).start()

    def browser_worker():
        try:
            # 1) Stream non-slow direct courses first.
            if direct_fast:
                def direct_done(cid, res):
                    q.put((cid, res))
                fetch_all_direct_parallel(direct_fast, date_str, players, before_time=before_time, on_course_done=direct_done)

            # 2) Chronogolf runs in course_worker threads above (HTTP API).

            # 3) Stream slowest direct courses last so they never delay other courses.
            for slow_course in [c for c in (boynton_course, boca_course) if c]:
                def slow_done(cid, res):
                    q.put((cid, res))
                fetch_all_direct_parallel([slow_course], date_str, players, before_time=before_time, on_course_done=slow_done)
        except Exception as e:
            pass

    # On Render, delay Chrome 3s so ForeUp/Chronogolf get a head start without memory contention
    if _is_render():
        def delayed_browser():
            import time as _t
            _t.sleep(3)
            browser_worker()
        threading.Thread(target=delayed_browser, daemon=True).start()
    else:
        threading.Thread(target=browser_worker, daemon=True).start()

    def generate():
        for _ in range(total):
            try:
                course_id, result = q.get(timeout=600)
                # #region agent log
                _debug_log("stream:yield", "result", {"course_id": course_id, "status": result.get("status"), "len_times": len(result.get("times", [])), "message": result.get("message"), "first_times": [t.get("time") for t in result.get("times", [])[:5]]}, "stream")
                # #endregion
                yield json.dumps({"course_id": course_id, "result": result}) + "\n"
            except Empty:
                _debug_log("stream:yield", "timeout", {"course_id": None, "message": "Timed out"}, "stream")
                yield json.dumps({"course_id": None, "result": {"status": "error", "message": "Timed out", "times": []}}) + "\n"

    return Response(stream_with_context(generate()), mimetype="application/x-ndjson",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/")
def index():
    # Serve UI from same directory as this script (works local and in Docker /app)
    base = os.path.dirname(os.path.abspath(__file__))
    return send_from_directory(base, "golf_ui.html")


# ─────────────────────────────────────────────
# STARTUP
# ─────────────────────────────────────────────
def check_dependencies():
    issues = []
    for pkg, install in [("flask_cors", "flask-cors"), ("requests", "requests")]:
        try:
            __import__(pkg)
        except ImportError:
            issues.append(f"  pip install {install}")
    # Eager-load Selenium in main thread to avoid _ModuleLock deadlock when workers run
    try:
        from selenium.webdriver.common.by import By  # noqa: F401
    except ImportError:
        pass

    if issues:
        print("\n⚠  Missing packages (install before running):")
        for i in issues:
            print(i)
        print()
    else:
        print("   ✓ All packages installed\n")


if __name__ == "__main__":
    print("\n⛳  South Florida Golf Tee Time Checker")
    print("━" * 44)
    check_dependencies()
    n_fore = len([c for c in COURSES if c["type"] == "foreup"])
    n_chrono = len([c for c in COURSES if c["type"] == "chronogolf"])
    n_direct = len([c for c in COURSES if c["type"] == "direct"])
    print(f"   ForeUp courses    ({n_fore}) → ~1-3s each")
    if n_chrono:
        print(f"   Chronogolf (API)  ({n_chrono}) → ~1-3s each")
    if n_direct:
        print(f"   Direct-book only  ({n_direct}) → link to course site")
    w = _max_browser_workers()
    render_mode = " [Render/512MB optimizations ON]" if _is_render() else ""
    print(f"   Browser workers: {w} parallel{render_mode}" + ("" if _is_render() or w == 1 else " (set MAX_PARALLEL_BROWSERS=1 on 512MB)"))
    print()
    port = int(os.environ.get("PORT", 5000))
    host = os.environ.get("HOST", "0.0.0.0")
    print(f"   Server: http://localhost:{port}" + (" (all interfaces)" if host == "0.0.0.0" else ""))
    print(f"   Debug log: {DEBUG_LOG_PATH}")
    print("   (Run with DEBUG=1 to echo debug lines to console)")
    print("   (Run with TIMING=1 to see per-phase seconds for Chronogolf/direct scrapes)")
    print("   (Run with LOG=1 to see timeout and speed logs: ForeUp/Direct duration, which course timed out)")
    print("   Press Ctrl+C to stop\n")
    app.run(debug=False, host=host, port=port, threaded=True)