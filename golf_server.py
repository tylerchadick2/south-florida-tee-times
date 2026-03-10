#!/usr/bin/env python3
"""
South Florida Golf Tee Time Checker
- ForeUp courses (5): direct API call — fast (~1-2s)
- Chronogolf courses (5): Selenium headless scraping — slower (~15-25s)

Run:  python golf_server.py
Open: http://localhost:5000

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

app = Flask(__name__, static_folder=".")

# Browser concurrency: 2 parallel by default (fast results without OOM). On 512MB (e.g. Render) set MAX_PARALLEL_BROWSERS=1.
def _max_browser_workers():
    v = os.environ.get("MAX_PARALLEL_BROWSERS", "").strip().lower()
    if v in ("1", "true", "yes", "low"):
        return 1
    if v == "":
        return 2  # default: 2 parallel for speed without overloading memory
    try:
        n = int(v)
        return max(1, min(6, n))
    except ValueError:
        return 2

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
    {
        "id": 4,
        "name": "North Palm Beach CC",
        "location": "North Palm Beach, FL",
        "type": "chronogolf",
        "chronogolf_slug": "north-palm-beach-country-club",
        "booking_url": "https://www.chronogolf.com/club/north-palm-beach-country-club",
    },
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
    {
        "id": 11,
        "name": "Winston Trails Golf Club",
        "location": "Lake Worth, FL",
        "type": "chronogolf",
        "chronogolf_slug": "winston-trails-golf-club",
        "booking_url": "https://www.chronogolf.com/club/winston-trails-golf-club",
    },
    {
        "id": 12,
        "name": "Westchester Golf Course",
        "location": "Boynton Beach, FL",
        "type": "chronogolf",
        "chronogolf_slug": "westchester-country-club",
        "booking_url": "https://www.chronogolf.com/club/westchester-country-club",
    },
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
        return {"status": "ok", "times": times}
    except requests.exceptions.Timeout:
        return {"status": "error", "message": "Request timed out"}
    except requests.exceptions.HTTPError as e:
        return {"status": "error", "message": f"HTTP {e.response.status_code}"}
    except Exception as e:
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
# CHRONOGOLF - Selenium with single shared browser
# ─────────────────────────────────────────────

CHRONOGOLF_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/html, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

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
    options.add_argument("--window-size=1024,768")
    options.add_argument("--log-level=3")
    # Reduce memory for 512MB limit (Render free tier): one Chrome can use 200–400MB
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-background-networking")
    options.add_argument("--disable-sync")
    options.add_argument("--disable-translate")
    options.add_argument("--no-first-run")
    options.add_argument("--disable-features=site-per-process,TranslateUI")
    options.add_argument("--js-flags=--max-old-space-size=96")
    options.add_argument("--renderer-process-limit=1")
    options.add_argument("--disable-plugins")
    options.add_argument("--disable-default-apps")
    options.add_argument("--disable-hang-monitor")
    options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
    options.page_load_strategy = "eager"

    def _wrap(driver):
        try:
            driver.set_page_load_timeout(20)
            driver.set_script_timeout(12)
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


def _fetch_one_chronogolf_course(course, date_iso, players):
    """
    Fetch tee times for a single Chronogolf course using its own browser.
    Returns (course_id, result). Used so we can run all Chronogolf courses in parallel.
    """
    driver = None
    try:
        driver = _get_driver()
        slug = course["chronogolf_slug"]
        url = f"https://www.chronogolf.com/club/{slug}?date={date_iso}&step=teetimes&holes=&coursesIds=&deals=false&groupSize={players}"
        driver.get(url)

        import time
        from selenium.webdriver.common.by import By

        # Quick network checks — API can fire at 0.3s or a bit later (e.g. Westchester); try 3 times so slow clubs still hit API
        for _ in (0, 1, 2):
            time.sleep(0.15)
            times = _intercept_network(driver, date_iso)
            if times is not None:
                times = [t for t in times if int(t.get("available_spots") or 0) >= players]
                if times:
                    return (course["id"], {"status": "ok", "times": times, "booking_url": course["booking_url"]})
                return (course["id"], {"status": "ok", "times": [], "booking_url": course["booking_url"]})

        # Short wait for any slot (1.0s max, poll every 0.05s so we notice slots quickly)
        slot_selector, found = _chronogolf_wait_any_slot(driver, timeout=1.0)
        if not found or not slot_selector:
            return (course["id"], {"status": "ok", "times": [], "booking_url": course["booking_url"]})

        view_more_texts = ("view more", "more times", "show more", "see more", "load more", "afficher plus", "plus de créneaux", "more slots")
        try:
            for tag in ("button", "a", "[role='button']", "span", "div"):
                sel = tag if tag.startswith("[") else tag
                for el in driver.find_elements(By.CSS_SELECTOR, sel):
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
            time.sleep(0.08)
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(0.05)
        except Exception:
            pass
        if os.environ.get("CHRONOGOLF_DUMP_DOM") == "1":
            _chronogolf_dump_filter_dom_once(driver)
        _chronogolf_click_player_filter(driver, players)
        time.sleep(0.15)
        slot_selector, _ = _chronogolf_wait_any_slot(driver, timeout=0.5)
        if slot_selector:
            try:
                els = driver.find_elements(By.CSS_SELECTOR, slot_selector)
                if els:
                    times = _parse_dom(els, players)
                    times = [t for t in times if int(t.get("available_spots") or 0) >= players]
                    if times:
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
                        return (course["id"], {"status": "ok", "times": times, "booking_url": course["booking_url"]})
            except Exception:
                continue
        return (course["id"], {"status": "ok", "times": [], "booking_url": course["booking_url"]})
    except Exception as e:
        return (course["id"], {"status": "error", "message": f"Error: {str(e)[:100]}", "booking_url": course.get("booking_url", "")})
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


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
        # With 2 parallel, 60s total is enough; with 1 worker allow more for sequential runs
        timeout_chrono = (50 * len(courses)) if _max_browser_workers() == 1 else 60
        timeout_chrono = max(timeout_chrono, 60)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_fetch_one_chronogolf_course, c, date_iso, players): c for c in courses}
            for future in as_completed(futures, timeout=timeout_chrono):
                try:
                    course_id, result = future.result()
                    _store(course_id, result)
                except Exception as e:
                    course = futures[future]
                    _store(course["id"], {"status": "error", "message": str(e)[:100], "booking_url": course.get("booking_url", "")})
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
            for el in candidates:
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
                    for el in els:
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
            for inp in radios:
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
                for el in els:
                    if el.is_displayed():
                        _scroll_click(el)
                        _debug_log("chronogolf:filter", "filter_clicked", {"scope": "fallback_xpath", "text": opt}, "H4")
                        return
        except Exception:
            pass

        _debug_log("chronogolf:filter", "no_match", {"players": players}, "H1")
    except Exception as e:
        _debug_log("chronogolf:filter", "exception", {"players": players, "error": str(e)[:120]}, "H5")


def _intercept_network(driver, date_iso):
    """Scan Chrome network logs for Chronogolf tee time API responses."""
    try:
        logs = driver.get_log("performance")
        for entry in logs:
            try:
                msg = json.loads(entry["message"])
                params = msg.get("message", {}).get("params", {})
                resp_info = params.get("response", {})
                url = resp_info.get("url", "")
                if any(k in url.lower() for k in ["tee_time", "teetime", "teetimes", "booking/slot", "availability", "slot", "chronogolf"]):
                    req_id = params.get("requestId")
                    if req_id:
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


def _parse_dom(elements, players):
    times = []
    for el in elements:
        try:
            text = (el.text or "").strip()
            m = re.search(r'\b(\d{1,2}:\d{2})\s*(am|pm|AM|PM)?\b', text, re.I)
            if m:
                t_str = m.group(1) + (" " + (m.group(2) or "").upper() if m.group(2) else "")
                spots = _parse_chronogolf_spots_from_text(text)
                if spots is None:
                    spots = 4  # assume full foursome when we can't parse capacity from DOM
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


def fetch_chronogolf_times(course, date_iso, players):
    """Single course wrapper - used when called individually."""
    results = fetch_all_chronogolf([course], date_iso, players)
    return results.get(course["id"], {
        "status": "error", "message": "Unknown error", "booking_url": course["booking_url"]
    })


# ─────────────────────────────────────────────
# DIRECT COURSE SCRAPERS (per-site custom logic)
# ─────────────────────────────────────────────
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
# Tee It Up (Florida Club) — the-florida-club.book.teeitup.golf
# DOM: MUI cards; time in p[data-testid="teetimes-tile-time"], players in [data-testid="teetimes-tile-available-players"] (same tile header).
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
    try:
        driver.get(url)
        _time.sleep(1.0)
        # Wait for Tee It Up tile times (data-testid) or "no times" / "Number of teetimes"
        def _has_tiles_or_done(d):
            try:
                if len(d.find_elements(By.CSS_SELECTOR, "[data-testid='teetimes-tile-time']")) > 0:
                    return True
            except Exception:
                pass
            body = (_selenium_get_visible_text(d) or "").lower()
            if "no tee times" in body or "number of teetimes available" in body:
                return True
            return False
        try:
            WebDriverWait(driver, 6).until(_has_tiles_or_done)
        except Exception:
            pass
        _time.sleep(0.3)
        seen = set()
        times = []
        # Primary: use Tee It Up DOM — each slot has p[data-testid="teetimes-tile-time"] and same tile has [data-testid="teetimes-tile-available-players"]
        for time_el in driver.find_elements(By.CSS_SELECTOR, "[data-testid='teetimes-tile-time']"):
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
# DOM: #SlotBox.slot-outer-box, .teetime .itembox.tt-btn; slots loaded via JS after page load.
# ─────────────────────────────────────────────
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
    try:
        driver.get(url)
        _time.sleep(1.0)
        # Wait for slot content: #SlotBox or .teetime with time text
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
        try:
            WebDriverWait(driver, 6).until(_has_slots_or_done)
        except Exception:
            pass
        _time.sleep(0.3)
        seen = set()
        times = []
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
# Eagle Club (Boynton Beach Links) — player.eagleclubsystems.online
# UI: Filter Options sidebar (Players = buttons 1/2/3/4, Choose Course = dropdown).
# Main area: horizontal date cards (e.g. "Sat 03/14"), then grid of tee time cards (green header = time, body = price, "4 Players").
# ─────────────────────────────────────────────
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
        # Wait for SPA (Filter Options); shorter wait so we don't burn timeout on slow loads
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//*[contains(translate(., 'FILTER', 'filter'), 'filter')]"))
            )
        except Exception:
            pass
        _time.sleep(1)

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

        _time.sleep(1.5)

        # 4) Parse tee time cards — checkpoint logic: only from elements that look like a slot (card with Championship/Players/$ or short time-only header)
        seen = set()
        times = []
        for el in driver.find_elements(By.XPATH, "//*[contains(., 'AM') or contains(., 'PM')]"):
            try:
                text = (el.text or "").strip()
                if not text or len(text) > 250:
                    continue
                m = time_pat.search(text)
                if not m:
                    continue
                if "Filter Options" in text or ("Reset" in text and "Time" in text) or ("7AM" in text and "6PM" in text):
                    continue
                h, min_, period = m.group(1), m.group(2), (m.group(3) or "").upper()
                t_str = f"{int(h)}:{min_} {period}"
                key = t_str.upper().replace(" ", "")
                if key in seen:
                    continue
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
    """Fetch one direct course (GolfNow, TeeItUp, Club Caddie, or Eagle Club) with its own browser. Returns (course_id, result)."""
    driver = None
    try:
        driver = _get_driver()
        scraper = course.get("direct_scraper") or ""
        if scraper == "golfnow":
            result = _fetch_direct_golfnow_with_driver(driver, course, date_iso, players)
        elif scraper == "teeitup":
            result = _fetch_direct_teeitup_with_driver(driver, course, date_iso, players)
        elif scraper == "clubcaddie":
            result = _fetch_direct_clubcaddie_with_driver(driver, course, date_iso, players)
        elif scraper == "eagleclub":
            result = _fetch_direct_eagleclub_with_driver(driver, course, date_iso, players)
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
        return (course["id"], {"status": "error", "message": str(e)[:100], "booking_url": course.get("booking_url", ""), "times": []})
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
    # Per-course timeout so we never hang forever on one course (Render 512MB can cause Chrome to hang).
    # Use a shorter cap for most sites, but give Boynton Beach a bit more room since its UI has more steps.
    base_timeout = 40

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
            # Boynton Beach Links (id=14) gets a bit more time for extra UI steps; others use base.
            course_timeout = base_timeout + 15 if course.get("id") == 14 else base_timeout
            t.join(timeout=course_timeout)
            if out[0] is not None:
                course_id, result = out[0]
                _done(course_id, result)
            else:
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
    """Dispatch to the correct direct scraper (Tee It Up, Club Caddie, Eagle Club, etc.)."""
    scraper = course.get("direct_scraper") or ""
    if scraper == "golfnow":
        result = fetch_direct_golfnow(course, date_iso, players)
    elif scraper == "teeitup":
        result = fetch_direct_teeitup(course, date_iso, players)
    elif scraper == "clubcaddie":
        result = fetch_direct_clubcaddie(course, date_iso, players)
    elif scraper == "eagleclub":
        result = fetch_direct_eagleclub(course, date_iso, players)
    else:
        result = {"status": "error", "message": "No scraper configured", "booking_url": course.get("booking_url", ""), "times": []}
    if before_time and result.get("status") == "ok":
        result["times"] = apply_time_filter(result.get("times", []), before_time)
    # Normalize time strings to consistent "10:18am" / "1:00pm" so UI displays and parses correctly
    if result.get("status") == "ok" and result.get("times"):
        for t in result["times"]:
            if t.get("time"):
                t["time"] = _normalize_tee_time_display(t["time"])
    result["booking_url"] = course.get("booking_url", "")
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
    print(f"  [all_teetimes] date={date_str} players={players} before_time={before_time!r}")

    if not date_str:
        return jsonify({"status": "error", "message": "Missing date"}), 400

    results = {}
    lock = threading.Lock()

    foreup_courses = [c for c in COURSES if c["type"] == "foreup"]
    # Run Winston Trails and Westchester first among Chronogolf so they don't hit the end of timeout
    chrono_courses = sorted(
        [c for c in COURSES if c["type"] == "chronogolf"],
        key=lambda c: (0 if c["id"] in (11, 12) else 1, c["id"]),
    )
    direct_courses = [c for c in COURSES if c["type"] == "direct"]
    # Boynton Beach Links (id=14) should run last overall so slow behavior never blocks other results.
    boynton_course = next((c for c in direct_courses if c["id"] == 14), None)
    direct_non_boynton = [c for c in direct_courses if c["id"] != 14]

    def course_worker(course):
        result = fetch_course(course, date_str, players, before_time)
        with lock:
            results[course["id"]] = result

    # ForeUp: parallel (no Selenium)
    foreup_threads = [threading.Thread(target=course_worker, args=(c,)) for c in foreup_courses]
    for t in foreup_threads:
        t.start()

    # Direct and Chronogolf: run Chrome
    chrono_results = {}
    def browser_worker():
        nonlocal chrono_results
        # 1) Run all non-Boynton direct courses.
        if direct_non_boynton:
            def direct_done(cid, res):
                with lock:
                    results[cid] = res
            fetch_all_direct_parallel(direct_non_boynton, date_str, players, before_time=before_time, on_course_done=direct_done)
        # 2) Run Chronogolf courses (second to last).
        r = fetch_all_chronogolf(chrono_courses, date_str, players)
        if before_time:
            for cid, res in r.items():
                if res.get("status") == "ok":
                    res["times"] = apply_time_filter(res.get("times", []), before_time)
        with lock:
            chrono_results.update(r)
        # 3) Run Boynton Beach Links last so it never delays other results.
        if boynton_course:
            def boynton_done(cid, res):
                with lock:
                    results[cid] = res
            fetch_all_direct_parallel([boynton_course], date_str, players, before_time=before_time, on_course_done=boynton_done)
    browser_thread = threading.Thread(target=browser_worker)
    browser_thread.start()

    # Wait for ForeUp
    for t in foreup_threads:
        t.join(timeout=15)
    # With 2 parallel, 180s is enough; with 1 worker allow ~50s per course so all finish or timeout
    n_browser_courses = len(direct_courses) + len(chrono_courses)
    browser_join = max(180, 50 * n_browser_courses) if _max_browser_workers() == 1 else 180
    browser_thread.join(timeout=browser_join)
    results.update(chrono_results)

    # Fill in any missing (timeouts; direct courses already set above)
    for course in COURSES:
        if course["id"] not in results:
            results[course["id"]] = {
                "status": "error",
                "message": "Timed out",
                "booking_url": course.get("booking_url", ""),
                "times": [],
            }

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
    chrono_courses = sorted([c for c in COURSES if c["type"] == "chronogolf"], key=lambda c: (0 if c["id"] in (11, 12) else 1, c["id"]))
    direct_courses = [c for c in COURSES if c["type"] == "direct"]
    boynton_course = next((c for c in direct_courses if c["id"] == 14), None)
    direct_non_boynton = [c for c in direct_courses if c["id"] != 14]
    q = Queue()
    total = len(COURSES)

    def course_worker(course):
        result = fetch_course(course, date_str, players, before_time)
        q.put((course["id"], result))

    def browser_worker():
        try:
            # 1) Stream non-Boynton direct courses first.
            if direct_non_boynton:
                def direct_done(cid, res):
                    q.put((cid, res))
                fetch_all_direct_parallel(direct_non_boynton, date_str, players, before_time=before_time, on_course_done=direct_done)

            # 2) Then stream Chronogolf results (second to last).
            def chrono_done(cid, res):
                if before_time and res.get("status") == "ok":
                    res["times"] = apply_time_filter(res.get("times", []), before_time)
                q.put((cid, res))
            fetch_all_chronogolf(chrono_courses, date_str, players, on_course_done=chrono_done)

            # 3) Stream Boynton Beach Links last so it never delays other courses.
            if boynton_course:
                def boynton_done(cid, res):
                    q.put((cid, res))
                fetch_all_direct_parallel([boynton_course], date_str, players, before_time=before_time, on_course_done=boynton_done)
        except Exception as e:
            for c in chrono_courses:
                q.put((c["id"], {"status": "error", "message": str(e)[:100], "booking_url": c.get("booking_url", ""), "times": []}))

    for c in foreup_courses:
        threading.Thread(target=course_worker, args=(c,), daemon=True).start()
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
    print(f"   Chronogolf courses ({n_chrono}) → ~2-5s each")
    if n_direct:
        print(f"   Direct-book only  ({n_direct}) → link to course site")
    w = _max_browser_workers()
    print(f"   Browser workers: {w} parallel" + (" (set MAX_PARALLEL_BROWSERS=1 on 512MB e.g. Render)" if w > 1 else ""))
    print()
    port = int(os.environ.get("PORT", 5000))
    host = os.environ.get("HOST", "0.0.0.0")
    print(f"   Server: http://localhost:{port}" + (" (all interfaces)" if host == "0.0.0.0" else ""))
    print(f"   Debug log: {DEBUG_LOG_PATH}")
    print("   (Run with DEBUG=1 to echo debug lines to console)")
    print("   Press Ctrl+C to stop\n")
    app.run(debug=False, host=host, port=port, threaded=True)