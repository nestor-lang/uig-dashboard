#!/usr/bin/env python3
"""
update_dashboard.py
Pulls data from Podio, Google Ads, and Meta — generates data.json and pushes to GitHub.
"""

import json, urllib.request, urllib.parse, base64, subprocess, sys, gzip as _gzip, io as _io
from datetime import datetime, timedelta
from collections import defaultdict

SKIP_SMS = "--skip-sms" in sys.argv

# ── Config ──────────────────────────────────────────────────────────────────
GITHUB_TOKEN   = os.environ["PUSH_TOKEN"]
GITHUB_REPO    = "nestor-lang/uig-dashboard"
GITHUB_FILE    = "data.json"

PODIO_CLIENT_ID  = "atlas"
PODIO_SECRET     = os.environ["PODIO_SECRET"]
PODIO_APP_ID     = "25179555"   # leads app
PODIO_APP_TOKEN  = os.environ["PODIO_APP_TOKEN"]
WHITEBOARD_APP_ID    = "25179507"
WHITEBOARD_APP_TOKEN = os.environ["WHITEBOARD_APP_TOKEN"]
CLOSINGS_APP_ID      = "25179510"
CLOSINGS_APP_TOKEN   = os.environ["CLOSINGS_APP_TOKEN"]
CLOSINGS_VIEW_ID     = "61742458"

META_TOKEN       = os.environ["META_TOKEN"]
META_ACCOUNT     = "act_882507283448364"

GOOGLE_ADS_CUSTOMER_ID = "2314793503"
GOOGLE_ADS_CONFIG      = "/tmp/google_ads_dash.yaml"

HOT_STATUS_IDS = {1, 13, 14, 16, 17, 18, 19, 43, 49, 52, 56, 57, 58, 59, 62, 69, 70, 73}
FB_VIEW_ID     = "61715439"  # Facebook leads view
PPC_VIEW_ID    = "61716398"  # PPC leads view
SMS_VIEW_ID    = "61742132"  # SMS leads view

now = datetime.now()
today_str      = now.strftime("%Y-%m-%d")
month_start    = now.strftime("%Y-%m-01")
thirty_days_ago = (now - timedelta(days=30)).strftime("%Y-%m-%d")
seven_days_ago  = (now - timedelta(days=7)).strftime("%Y-%m-%d")
current_month   = now.strftime("%Y-%m")

# ── Helpers ──────────────────────────────────────────────────────────────────
def get(url, headers=None):
    req = urllib.request.Request(url, headers=headers or {})
    return json.loads(urllib.request.urlopen(req, timeout=20).read())

def post(url, data, headers=None, form=False):
    if form or not headers:
        body = urllib.parse.urlencode(data).encode()
        ct = "application/x-www-form-urlencoded"
    else:
        body = json.dumps(data).encode()
        ct = "application/json"
    h = {"Content-Type": ct, "Accept-Encoding": "gzip", **(headers or {})}
    req = urllib.request.Request(url, data=body, headers=h, method="POST")
    with urllib.request.urlopen(req, timeout=20) as r:
        raw = r.read()
        if r.headers.get("Content-Encoding", "") == "gzip":
            raw = _gzip.GzipFile(fileobj=_io.BytesIO(raw)).read()
        return json.loads(raw)

def put_github(path, content_str, sha=None):
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
    payload = {
        "message": f"chore: update {path} [{today_str}]",
        "content": base64.b64encode(content_str.encode()).decode(),
        "branch": "main"
    }
    if sha:
        payload["sha"] = sha
    req = urllib.request.Request(url, data=json.dumps(payload).encode(),
        headers={"Authorization": f"token {GITHUB_TOKEN}", "Content-Type": "application/json"},
        method="PUT")
    return json.loads(urllib.request.urlopen(req, timeout=20).read())

def get_github_sha(path):
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
    try:
        req = urllib.request.Request(url, headers={"Authorization": f"token {GITHUB_TOKEN}"})
        r = json.loads(urllib.request.urlopen(req, timeout=10).read())
        return r.get("sha")
    except Exception as e:
        print(f"  get_sha error: {e}")
        return None

# ── 1. Podio auth ─────────────────────────────────────────────────────────────
print("Authenticating with Podio...")
auth = post("https://podio.com/oauth/token", {
    "grant_type": "app", "app_id": PODIO_APP_ID, "app_token": PODIO_APP_TOKEN,
    "client_id": PODIO_CLIENT_ID, "client_secret": PODIO_SECRET
}, form=True)
podio_token = auth["access_token"]
podio_headers = {"Authorization": f"Bearer {podio_token}", "Content-Type": "application/json"}


# ── 2. Podio leads (via views — no unfiltered pulls) ─────────────────────────
def created_month_ast(item):
    """Convert Podio created_on (UTC) to AST (UTC-4) and return YYYY-MM."""
    created_utc = item.get("created_on", "")
    if not created_utc:
        return ""
    try:
        dt = datetime.strptime(created_utc[:19], "%Y-%m-%d %H:%M:%S") - timedelta(hours=4)
        return dt.strftime("%Y-%m")
    except:
        return created_utc[:7]

def created_date_ast(item):
    """Convert Podio created_on (UTC) to AST (UTC-4) and return YYYY-MM-DD."""
    created_utc = item.get("created_on", "")
    if not created_utc:
        return ""
    try:
        dt = datetime.strptime(created_utc[:19], "%Y-%m-%d %H:%M:%S") - timedelta(hours=4)
        return dt.strftime("%Y-%m-%d")
    except:
        return created_utc[:10]

def fetch_view_items(view_id, label, max_pages=5, cutoff_date=None):
    """Fetch items from a Podio view, up to max_pages * 200 items.
    If cutoff_date (YYYY-MM-DD) is set, filters server-side via Podio's created_on filter
    so Podio only scans recent items — avoids timeouts on large views."""
    print(f"Pulling {label} leads from view...")
    all_items = []
    offset = 0
    body = {"limit": 200, "offset": offset, "sort_by": "created_on", "sort_desc": True}
    if cutoff_date:
        # Push date filter to Podio server-side — critical for large views (e.g. SMS)
        body["filters"] = {
            "created_on": {
                "from": f"{cutoff_date} 00:00:00",
                "to": f"{today_str} 23:59:59"
            }
        }
    for _ in range(max_pages):
        body["offset"] = offset
        result = post(f"https://api.podio.com/item/app/{PODIO_APP_ID}/filter/{view_id}/",
            body, podio_headers)
        items = result.get("items", [])
        if not items:
            break
        all_items.extend(items)
        if len(all_items) >= result.get("total", 0):
            break
        offset += 200
    return all_items

fb_view_items  = fetch_view_items(FB_VIEW_ID,  "FB")
ppc_view_items = fetch_view_items(PPC_VIEW_ID, "PPC")
def fetch_sms_items(cutoff_date, max_pages=5):
    """Fetch SMS leads by querying the app directly with a date filter.
    Bypasses the view (which times out on large datasets) and uses the base
    /item/app/{id}/filter/ endpoint with a created_on range filter."""
    print("Pulling SMS leads from view...")
    all_items = []
    offset = 0
    body = {
        "limit": 200,
        "sort_by": "created_on",
        "sort_desc": True,
        "filters": {
            "created_on": {
                "from": f"{cutoff_date} 00:00:00",
                "to":   f"{today_str} 23:59:59"
            }
        }
    }
    for _ in range(max_pages):
        body["offset"] = offset
        result = post(f"https://api.podio.com/item/app/{PODIO_APP_ID}/filter/",
            body, podio_headers)
        items = result.get("items", [])
        if not items:
            break
        # Filter client-side to SMS-sourced leads only
        sms_items = []
        for item in items:
            fields = {f["external_id"]: f.get("values") for f in item.get("fields", [])}
            ch_field = fields.get("source-2") or []
            if ch_field and ch_field[0].get("value"):
                ch = ch_field[0]["value"].get("title", "")
            else:
                sn_field = fields.get("source-name") or []
                ch = sn_field[0]["value"] if sn_field and sn_field[0].get("value") else ""
            if "sms" in ch.lower() or "text" in ch.lower():
                sms_items.append(item)
        all_items.extend(sms_items)
        if len(items) < 200:
            break
        offset += 200
    print(f"  SMS: {len(all_items)} items fetched")
    return all_items

if SKIP_SMS:
    print("Pulling SMS leads from view...")
    print("  SMS fetch skipped (--skip-sms)")
    sms_view_items = []
    sms_fetch_error = None
else:
    try:
        sms_view_items = fetch_sms_items(thirty_days_ago)
        sms_fetch_error = None
    except Exception as e:
        print(f"  ⚠️ SMS fetch failed ({e}), continuing without SMS leads...")
        sms_view_items = []
        sms_fetch_error = str(e)

def parse_lead(item):
    fields = {f["external_id"]: f.get("values") for f in item.get("fields", [])}
    created = created_date_ast(item)
    month   = created[:7]

    ch_field = fields.get("source-2") or []
    if ch_field and ch_field[0].get("value"):
        channel = ch_field[0]["value"].get("title", "Unknown")
    else:
        sn_field = fields.get("source-name") or []
        channel = sn_field[0]["value"] if sn_field and sn_field[0].get("value") else "Unknown"

    status_field = fields.get("forecast") or fields.get("status") or []
    status_id = status_field[0]["value"]["id"] if status_field and status_field[0].get("value") else 0

    addr_field = fields.get("address") or fields.get("property-address") or []
    address = addr_field[0].get("formatted", addr_field[0].get("value", "")) if addr_field else ""

    phone_field = fields.get("phone") or fields.get("phone-number-2") or []
    phone = phone_field[0]["value"] if phone_field else ""

    return {
        "created_on": created,
        "month": month,
        "channel": channel,
        "status_id": status_id,
        "address": address,
        "phone": phone,
        "name": item.get("title", "Unknown"),
        "is_hot": status_id in HOT_STATUS_IDS
    }

# Build all_leads from all views
all_leads = [parse_lead(i) for i in fb_view_items + ppc_view_items + sms_view_items]

leads_30d = [l for l in all_leads if l["created_on"] >= thirty_days_ago]
leads_7d  = [l for l in all_leads if l["created_on"] >= seven_days_ago]
leads_mtd = [l for l in all_leads if l["month"] == current_month]

call_leads_30d = len([l for l in leads_30d if "call" in l["channel"].lower() or "answer" in l["channel"].lower()])

fb_leads_mtd  = sum(1 for item in fb_view_items  if created_month_ast(item) == current_month)
ppc_leads_mtd = sum(1 for item in ppc_view_items if created_month_ast(item) == current_month)

ppc_leads_30d = ppc_leads_mtd
fb_leads_30d  = fb_leads_mtd

# Leads by channel by month
fb_leads_by_month  = defaultdict(int)
ppc_leads_by_month = defaultdict(int)
for item in fb_view_items:
    mk = created_month_ast(item)
    if mk: fb_leads_by_month[mk] += 1
for item in ppc_view_items:
    mk = created_month_ast(item)
    if mk: ppc_leads_by_month[mk] += 1

# hot leads
hot_leads = [l for l in all_leads if l["is_hot"]][:10]

# ── ROA pipeline rules (same as KPI sheet) ────────────────────────────────────
ACTIVE_OPP_EXCLUDE = ["Not an Opportunity", "No Contact"]
BOOKED_EXCLUDE     = ["Potential Opportunity", "Not an Opportunity", "No Contact"]
OFFERED_EXCLUDE    = ["Potential Opportunity", "Opportunity | Need to Offer", "Not an Opportunity", "No Contact", "Unreported", ""]
ACCEPTED_INCLUDE   = ["Opportunity | Accepted", "Opportunity | Contract Available",
                      "Opportunity | Contract Assigned", "Opportunity | Contract Closed",
                      "Opportunity | Contract Dead"]
AVAILABLE_ROA      = ["Opportunity | Contract Available"]
PENDING_ROA        = ["Opportunity | Contract Assigned"]
CLOSED_ROA         = ["Opportunity | Contract Closed"]

def get_roa(item):
    for f in item.get("fields", []):
        if f.get("external_id") == "admin-use-internal-dispositions" and f.get("values"):
            v = f["values"][0].get("value", "")
            if isinstance(v, dict): return v.get("text", "")
            return str(v) if v else ""
    return ""

def calc_pipeline(items, month):
    leads = [i for i in items if created_month_ast(i) == month]
    roas  = [get_roa(i) for i in leads]
    return {
        "leads":          len(leads),
        "active":         sum(1 for r in roas if r not in ACTIVE_OPP_EXCLUDE),
        "booked":         sum(1 for r in roas if r not in BOOKED_EXCLUDE),
        "offered":        sum(1 for r in roas if r not in OFFERED_EXCLUDE),
        "accepted":       sum(1 for r in roas if r in ACCEPTED_INCLUDE),
        "under_contract": sum(1 for r in roas if r in AVAILABLE_ROA + PENDING_ROA),
        "closed":         sum(1 for r in roas if r in CLOSED_ROA),
    }

fb_pipeline  = calc_pipeline(fb_view_items,  current_month)
ppc_pipeline = calc_pipeline(ppc_view_items, current_month)

# Pipeline by month for all months (so month picker works)
all_pipeline_months = sorted(set(
    [created_month_ast(i) for i in fb_view_items + ppc_view_items if created_month_ast(i)]
))
fb_pipeline_by_month  = {mk: calc_pipeline(fb_view_items,  mk) for mk in all_pipeline_months}
ppc_pipeline_by_month = {mk: calc_pipeline(ppc_view_items, mk) for mk in all_pipeline_months}

# Combined under_contract for top KPI card
under_contract = fb_pipeline["under_contract"] + ppc_pipeline["under_contract"]
closed_total   = fb_pipeline["closed"] + ppc_pipeline["closed"]

# ── 3. Google Ads ─────────────────────────────────────────────────────────────
print("Pulling Google Ads...")
import yaml
from google.ads.googleads.client import GoogleAdsClient

google_config = {
    "developer_token": os.environ["GOOGLE_DEV_TOKEN"],
    "client_id": os.environ["GOOGLE_CLIENT_ID"],
    "client_secret": os.environ["GOOGLE_CLIENT_SECRET"],
    "refresh_token": os.environ["GOOGLE_REFRESH_TOKEN_ADS"],
    "login_customer_id": "3944732021",
    "use_proto_plus": True
}
with open(GOOGLE_ADS_CONFIG, "w") as f:
    yaml.dump(google_config, f)

ga_client = GoogleAdsClient.load_from_storage(GOOGLE_ADS_CONFIG)
ga_service = ga_client.get_service("GoogleAdsService")

mtd_query = f"""
    SELECT campaign.name, campaign.status, metrics.cost_micros, metrics.clicks, metrics.average_cpc
    FROM campaign
    WHERE segments.date BETWEEN '{month_start}' AND '{today_str}'
    AND campaign.status != 'REMOVED'
"""
campaigns_mtd = {}
for row in ga_service.search(customer_id=GOOGLE_ADS_CUSTOMER_ID, query=mtd_query):
    campaigns_mtd[row.campaign.name] = {
        "spend": row.metrics.cost_micros / 1_000_000,
        "clicks": row.metrics.clicks,
        "cpc": row.metrics.average_cpc / 1_000_000 if row.metrics.average_cpc else 0,
        "status": row.campaign.status.name
    }

# Today's spend per campaign
today_query = f"""
    SELECT campaign.name, metrics.cost_micros
    FROM campaign
    WHERE segments.date = '{today_str}'
    AND campaign.status != 'REMOVED'
"""
campaigns_today = {}
for row in ga_service.search(customer_id=GOOGLE_ADS_CUSTOMER_ID, query=today_query):
    campaigns_today[row.campaign.name] = row.metrics.cost_micros / 1_000_000

# Google spend by month (Jan–current)
google_monthly_query = """
    SELECT segments.month, metrics.cost_micros, metrics.clicks
    FROM campaign
    WHERE segments.date BETWEEN '2026-01-01' AND '""" + today_str + """'
    AND campaign.status != 'REMOVED'
"""
google_spend_by_month  = defaultdict(float)
google_clicks_by_month = defaultdict(int)
for row in ga_service.search(customer_id=GOOGLE_ADS_CUSTOMER_ID, query=google_monthly_query):
    month_key = str(row.segments.month)[:7]
    google_spend_by_month[month_key]  += row.metrics.cost_micros / 1_000_000
    google_clicks_by_month[month_key] += row.metrics.clicks

total_google_spend = sum(c["spend"] for c in campaigns_mtd.values())
total_google_clicks = sum(c["clicks"] for c in campaigns_mtd.values())
google_cpc = round(total_google_spend / total_google_clicks, 2) if total_google_clicks else None
google_cpl = round(total_google_spend / ppc_leads_30d, 2) if ppc_leads_30d else None

google_campaigns = []
for name, c in campaigns_mtd.items():
    if c["spend"] <= 0:
        continue  # skip campaigns with no spend this month
    google_campaigns.append({
        "name": name,
        "spend": round(c["spend"], 2),
        "spend_today": round(campaigns_today.get(name, 0), 2),
        "clicks": c["clicks"],
        "cpc": round(c["cpc"], 2),
        "crm_leads": 0,
        "cpl": google_cpl
    })

# ── 4. Meta Ads ───────────────────────────────────────────────────────────────
print("Pulling Meta Ads...")
meta_params = urllib.parse.urlencode({
    "fields": "campaign_name,spend,clicks,impressions,cpc",
    "time_range": json.dumps({"since": month_start, "until": today_str}),
    "level": "campaign",
    "access_token": META_TOKEN
})
meta_data = get(f"https://graph.facebook.com/v18.0/{META_ACCOUNT}/insights?{meta_params}")
meta_campaigns_raw = meta_data.get("data", [])

# Today's Meta spend
meta_today_params = urllib.parse.urlencode({
    "fields": "campaign_name,spend",
    "time_range": json.dumps({"since": today_str, "until": today_str}),
    "level": "campaign",
    "access_token": META_TOKEN
})
meta_today_raw = get(f"https://graph.facebook.com/v18.0/{META_ACCOUNT}/insights?{meta_today_params}").get("data", [])
meta_today = {c.get("campaign_name"): float(c.get("spend", 0)) for c in meta_today_raw}

total_meta_spend = sum(float(c.get("spend", 0)) for c in meta_campaigns_raw)

# Meta spend by month (Jan–current)
META_MONTH_RANGES = [
    ("2026-01","2026-01-01","2026-01-31"),("2026-02","2026-02-01","2026-02-28"),
    ("2026-03","2026-03-01","2026-03-31"),("2026-04","2026-04-01","2026-04-30"),
    ("2026-05","2026-05-01","2026-05-31"),("2026-06","2026-06-01","2026-06-30"),
    ("2026-07","2026-07-01","2026-07-31"),("2026-08","2026-08-01","2026-08-31"),
    ("2026-09","2026-09-01","2026-09-30"),("2026-10","2026-10-01","2026-10-31"),
    ("2026-11","2026-11-01","2026-11-30"),("2026-12","2026-12-01","2026-12-31"),
]
meta_spend_by_month  = {}
meta_clicks_by_month = {}
for mk, since, until in META_MONTH_RANGES:
    if since > today_str: break
    p = urllib.parse.urlencode({
        "fields": "spend,clicks",
        "time_range": json.dumps({"since": since, "until": min(until, today_str)}),
        "level": "account", "access_token": META_TOKEN
    })
    data = get(f"https://graph.facebook.com/v18.0/{META_ACCOUNT}/insights?{p}")
    meta_spend_by_month[mk]  = sum(float(d.get("spend", 0)) for d in data.get("data", []))
    meta_clicks_by_month[mk] = sum(int(d.get("clicks", 0)) for d in data.get("data", []))
total_meta_clicks = sum(int(c.get("clicks", 0)) for c in meta_campaigns_raw)
meta_cpc = round(total_meta_spend / total_meta_clicks, 2) if total_meta_clicks else None
meta_cpl = round(total_meta_spend / fb_leads_30d, 2) if fb_leads_30d else None
hot_fb = len([l for l in hot_leads if "facebook" in l["channel"].lower() or "fb" in l["channel"].lower()])
cost_per_hot = round(total_meta_spend / hot_fb, 2) if hot_fb else None

meta_campaigns = []
for c in meta_campaigns_raw:
    spend = float(c.get("spend", 0))
    name = c.get("campaign_name", "Unknown")
    meta_campaigns.append({
        "name": name,
        "spend": round(spend, 2),
        "spend_today": round(meta_today.get(name, 0), 2),
        "clicks": int(c.get("clicks", 0)),
        "crm_leads": 0,
        "cpl_crm": None
    })
# Assign leads to top campaign
if meta_campaigns and fb_leads_30d:
    meta_campaigns[0]["crm_leads"] = fb_leads_30d
    meta_campaigns[0]["cpl_crm"] = round(meta_campaigns[0]["spend"] / fb_leads_30d, 2) if fb_leads_30d else None

# ── 5. KPI sheet — SMS spend + ROA by month ───────────────────────────────────
print("Pulling SMS spend from KPI sheet...")
sheets_data = urllib.parse.urlencode({
    "client_id": os.environ["GOOGLE_CLIENT_ID"],
    "client_secret": os.environ["GOOGLE_CLIENT_SECRET"],
    "refresh_token": os.environ["GOOGLE_REFRESH_TOKEN_SHEETS"],
    "grant_type": "refresh_token"
}).encode()
sheets_req = urllib.request.Request("https://oauth2.googleapis.com/token", data=sheets_data,
    headers={"Content-Type": "application/x-www-form-urlencoded"}, method="POST")
with urllib.request.urlopen(sheets_req) as r:
    sheets_access = json.loads(r.read())["access_token"]

sms_sheet_req = urllib.request.Request(
    f"https://sheets.googleapis.com/v4/spreadsheets/18bW_nutcjWVWl9abxcl3d0OYoFgCgQDFPEAGaABjBDg/values/SMS!A1:C40",
    headers={"Authorization": f"Bearer {sheets_access}"})
with urllib.request.urlopen(sms_sheet_req) as r:
    sms_rows = json.loads(r.read()).get("values", [])

MONTH_KEYS = {
    "January":"2026-01","February":"2026-02","March":"2026-03","April":"2026-04",
    "May":"2026-05","June":"2026-06","July":"2026-07","Aug":"2026-08","Sept":"2026-09",
    "August":"2026-08","September":"2026-09",
    "Oct":"2026-10","October":"2026-10","Nov":"2026-11","November":"2026-11",
    "Dec":"2026-12","December":"2026-12"
}

def parse_dollar(s):
    try: return float(str(s).replace("$","").replace(",","").strip())
    except: return 0.0

sms_spend_by_month = {}
roa_by_month = {}  # {month_key: {closed, pending, spend}}

# Parse SMS tab (40 rows, cols A-AG)
sms_sheet_req = urllib.request.Request(
    f"https://sheets.googleapis.com/v4/spreadsheets/18bW_nutcjWVWl9abxcl3d0OYoFgCgQDFPEAGaABjBDg/values/SMS!A1:AG40",
    headers={"Authorization": f"Bearer {sheets_access}"})
with urllib.request.urlopen(sms_sheet_req) as r:
    sms_rows = json.loads(r.read()).get("values", [])

for row in sms_rows:
    if len(row) >= 3 and row[0] in MONTH_KEYS and (len(row) > 1 and row[1] == "TEAM"):
        mk = MONTH_KEYS[row[0]]
        sms_spend_by_month[mk] = parse_dollar(row[2])
        if mk not in roa_by_month:
            roa_by_month[mk] = {"closed": 0, "pending": 0, "spend": 0}
        roa_by_month[mk]["closed"]  += parse_dollar(row[26]) if len(row) > 26 else 0  # AA
        roa_by_month[mk]["pending"] += parse_dollar(row[25]) if len(row) > 25 else 0  # Z
        roa_by_month[mk]["spend"]   += parse_dollar(row[2])  if len(row) > 2  else 0  # C

# FB/PPC cols: D=3,E=4,G=6,I=8,K=10,N=13,O=14,P=15,Q=16,S=18(pending$),T=19(closed$)
# SMS cols:    K=10,N=13,P=15,R=17,U=20,V=21,W=22,X=23,Z=25(pending$),AA=26(closed$)
def blank_rep():
    return {"leads":0,"active":0,"booked":0,"offers":0,"accepted":0,
            "available":0,"pending":0,"dead":0,"closed":0,
            "pending_income":0.0,"closed_revenue":0.0}

reps_by_month = {}  # {month_key: {Charles: {...}, Julian: {...}}}

for tab in ["Facebook", "PPC", "SMS"]:
    is_sms = (tab == "SMS")
    col_map = {
        "leads":    10 if is_sms else 3,
        "active":   11 if is_sms else 4,
        "booked":   13 if is_sms else 6,
        "offers":   15 if is_sms else 8,
        "accepted": 17 if is_sms else 10,
        "available":20 if is_sms else 13,
        "pending":  21 if is_sms else 14,
        "dead":     22 if is_sms else 15,
        "closed":   23 if is_sms else 16,
        "pending_income":  25 if is_sms else 18,
        "closed_revenue":  26 if is_sms else 19,
    }
    tab_req = urllib.request.Request(
        f"https://sheets.googleapis.com/v4/spreadsheets/18bW_nutcjWVWl9abxcl3d0OYoFgCgQDFPEAGaABjBDg/values/{urllib.parse.quote(tab)}!A1:AB40",
        headers={"Authorization": f"Bearer {sheets_access}"})
    with urllib.request.urlopen(tab_req) as r:
        tab_rows = json.loads(r.read()).get("values", [])

    current_month_key = None
    for row in tab_rows:
        if not row: continue
        month_name = row[0] if row[0] else None
        agent_name = row[1] if len(row) > 1 else None

        if month_name and month_name in MONTH_KEYS:
            current_month_key = MONTH_KEYS[month_name]
            # TEAM row — ROA data
            if agent_name == "TEAM":
                mk = current_month_key
                if mk not in roa_by_month:
                    roa_by_month[mk] = {"closed": 0, "pending": 0, "spend": 0}
                roa_by_month[mk]["closed"]  += parse_dollar(row[19]) if not is_sms and len(row) > 19 else (parse_dollar(row[26]) if is_sms and len(row) > 26 else 0)
                roa_by_month[mk]["pending"] += parse_dollar(row[18]) if not is_sms and len(row) > 18 else (parse_dollar(row[25]) if is_sms and len(row) > 25 else 0)
                roa_by_month[mk]["spend"]   += parse_dollar(row[2])  if len(row) > 2 else 0
        elif agent_name in ["Charles", "Julian"] and current_month_key:
            mk = current_month_key
            if mk not in reps_by_month:
                reps_by_month[mk] = {"Charles": blank_rep(), "Julian": blank_rep()}
            if agent_name not in reps_by_month[mk]:
                reps_by_month[mk][agent_name] = blank_rep()
            rep = reps_by_month[mk][agent_name]
            for field, col in col_map.items():
                if len(row) > col and row[col]:
                    if field in ("pending_income", "closed_revenue"):
                        rep[field] += parse_dollar(row[col])
                    else:
                        try: rep[field] += int(str(row[col]).replace(",","").strip())
                        except: pass

# ── 6. Whiteboard pipeline income ─────────────────────────────────────────────
print("Pulling Whiteboard income data...")
whiteboard_error = None
try:
    wb_auth = post("https://podio.com/oauth/token", {
        "grant_type": "app", "app_id": WHITEBOARD_APP_ID, "app_token": WHITEBOARD_APP_TOKEN,
        "client_id": PODIO_CLIENT_ID, "client_secret": PODIO_SECRET
    }, form=True)
    wb_token = wb_auth["access_token"]
    wb_headers = {"Authorization": f"Bearer {wb_token}", "Content-Type": "application/json"}
except Exception as e:
    print(f"  ⚠️ Whiteboard auth failed ({e}), skipping whiteboard data...")
    whiteboard_error = str(e)
    wb_token = None
    wb_headers = None

def wb_fetch(status_ids):
    """Fetch all whiteboard items for given status-2 ids."""
    if not wb_headers:
        return []
    items_all = []
    offset = 0
    while True:
        body = json.dumps({"filters": {"status-2": status_ids}, "limit": 200, "offset": offset}).encode()
        req = urllib.request.Request(
            f"https://api.podio.com/item/app/{WHITEBOARD_APP_ID}/filter/",
            data=body, headers=wb_headers, method="POST")
        result = json.loads(urllib.request.urlopen(req, timeout=20).read())
        items = result.get("items", [])
        items_all.extend(items)
        if len(items) < 200:
            break
        offset += 200
    return items_all

def get_field(fields_list, ext_id):
    for f in fields_list:
        if f.get("external_id") == ext_id:
            vals = f.get("values", [])
            if not vals: return None
            v = vals[0].get("value")
            return v
    return None

def gross(fields_list):
    v = get_field(fields_list, "gross-profit-2")
    return float(v or 0)

# Available deals → potential income by stage
available_items = wb_fetch([1]) if not whiteboard_error else []
potential_income = round(sum(gross(i.get("fields", [])) for i in available_items))

by_stage = defaultdict(float)
for item in available_items:
    fields = item.get("fields", [])
    stage_val = get_field(fields, "stage-new")
    stage_name = stage_val.get("text", "Unknown") if isinstance(stage_val, dict) else "Unknown"
    by_stage[stage_name] += gross(fields)
by_stage = {k: round(v) for k, v in sorted(by_stage.items(), key=lambda x: -x[1])}

# Buyer Found / Sent to Title → assigned income by acq agent / dispo agent
assigned_items = wb_fetch([6, 3, 10, 2]) if not whiteboard_error else []
assigned_income = round(sum(gross(i.get("fields", [])) for i in assigned_items))

by_agent = defaultdict(float)
for item in assigned_items:
    fields = item.get("fields", [])
    gp = gross(fields)
    import re as _re
    ABBR_TO_STATE = {
        "AL":"Alabama","AK":"Alaska","AZ":"Arizona","AR":"Arkansas","CA":"California",
        "CO":"Colorado","CT":"Connecticut","DE":"Delaware","FL":"Florida","GA":"Georgia",
        "HI":"Hawaii","ID":"Idaho","IL":"Illinois","IN":"Indiana","IA":"Iowa","KS":"Kansas",
        "KY":"Kentucky","LA":"Louisiana","ME":"Maine","MD":"Maryland","MA":"Massachusetts",
        "MI":"Michigan","MN":"Minnesota","MS":"Mississippi","MO":"Missouri","MT":"Montana",
        "NE":"Nebraska","NV":"Nevada","NH":"New Hampshire","NJ":"New Jersey","NM":"New Mexico",
        "NY":"New York","NC":"North Carolina","ND":"North Dakota","OH":"Ohio","OK":"Oklahoma",
        "OR":"Oregon","PA":"Pennsylvania","RI":"Rhode Island","SC":"South Carolina",
        "SD":"South Dakota","TN":"Tennessee","TX":"Texas","UT":"Utah","VT":"Vermont",
        "VA":"Virginia","WA":"Washington","WV":"West Virginia","WI":"Wisconsin","WY":"Wyoming"
    }
    state = "Unknown"
    for f in fields:
        if f.get("external_id") == "property-address-text":
            vals = f.get("values", [])
            if vals:
                addr_text = str(vals[0].get("value", "")).upper()
                m = _re.search(r',\s*([A-Z]{2})\s+\d{5}', addr_text)
                if m:
                    state = ABBR_TO_STATE.get(m.group(1), m.group(1).title())
            break
    if state == "Unknown":
        for f in fields:
            if f.get("external_id") == "address":
                vals = f.get("values", [])
                if vals:
                    raw = (vals[0].get("state") or "").strip().upper()
                    if raw:
                        state = ABBR_TO_STATE.get(raw, raw.title())
                break
    by_agent[state] += gp
by_agent = {k: round(v) for k, v in sorted(by_agent.items(), key=lambda x: -x[1])}

# ── 6. Closings — revenue by month ────────────────────────────────────────────
print("Pulling Closings data...")
closed_by_month = defaultdict(float)
try:
    cl_auth = post("https://podio.com/oauth/token", {
        "grant_type": "app", "app_id": CLOSINGS_APP_ID, "app_token": CLOSINGS_APP_TOKEN,
        "client_id": PODIO_CLIENT_ID, "client_secret": PODIO_SECRET
    }, form=True)
    cl_token = cl_auth["access_token"]
    cl_headers = {"Authorization": f"Bearer {cl_token}", "Content-Type": "application/json"}

    offset = 0
    while True:
        body = json.dumps({"limit": 200, "offset": offset}).encode()
        req = urllib.request.Request(
            f"https://api.podio.com/item/app/{CLOSINGS_APP_ID}/filter/{CLOSINGS_VIEW_ID}/",
            data=body, headers=cl_headers, method="POST")
        result = json.loads(urllib.request.urlopen(req, timeout=20).read())
        items = result.get("items", [])
        for item in items:
            created_utc = item.get("created_on", "")
            try:
                dt = datetime.strptime(created_utc[:19], "%Y-%m-%d %H:%M:%S") - timedelta(hours=4)
                month_key = dt.strftime("%Y-%m")
            except:
                month_key = created_utc[:7]
            for f in item.get("fields", []):
                if f.get("external_id") == "closing-price":
                    vals = f.get("values", [])
                    if vals:
                        closed_by_month[month_key] += float(vals[0].get("value", 0) or 0)
        if len(items) < 200:
            break
        offset += 200
except Exception as e:
    print(f"  ⚠️ Closings data failed ({e}), continuing with empty closings...")

closed_by_month = {k: round(v) for k, v in sorted(closed_by_month.items())}

# Also compute closed revenue per agent per month from Closings (using Whiteboard gross-profit-2)
closed_by_agent_month = defaultdict(lambda: defaultdict(float))  # {month: {agent_first: amount}}
try:
    wb_auth2 = post("https://podio.com/oauth/token", {
        "grant_type": "app", "app_id": WHITEBOARD_APP_ID, "app_token": WHITEBOARD_APP_TOKEN,
        "client_id": PODIO_CLIENT_ID, "client_secret": PODIO_SECRET
    }, form=True)
    wb_token2 = wb_auth2["access_token"]

    offset2 = 0
    while True:
        body = json.dumps({"limit": 200, "offset": offset2}).encode()
        req = urllib.request.Request(
            f"https://api.podio.com/item/app/{CLOSINGS_APP_ID}/filter/{CLOSINGS_VIEW_ID}/",
            data=body, headers=cl_headers, method="POST")
        result2 = json.loads(urllib.request.urlopen(req, timeout=20).read())
        items2 = result2.get("items", [])
        for item in items2:
            created_utc = item.get("created_on", "")
            try:
                dt = datetime.strptime(created_utc[:19], "%Y-%m-%d %H:%M:%S") - timedelta(hours=4)
                mk = dt.strftime("%Y-%m")
            except:
                mk = created_utc[:7]
            if mk < "2026-01": continue  # only 2026
            member_name = ""
            wb_item_id = None
            for f in item.get("fields", []):
                ext = f.get("external_id","")
                vals = f.get("values",[])
                if ext == "member" and vals:
                    v = vals[0].get("value",{})
                    member_name = v.get("name","") if isinstance(v,dict) else str(v)
                if ext == "whiteboard" and vals:
                    wb_item_id = vals[0].get("value",{}).get("item_id")
            # Map full name to first name
            first = member_name.split()[0] if member_name else ""
            if first not in {"Julian", "Charles"} or not wb_item_id:
                continue
            # Fetch gross profit from whiteboard
            try:
                wb_req = urllib.request.Request(f"https://api.podio.com/item/{wb_item_id}",
                    headers={"Authorization": f"OAuth2 {wb_token2}"})
                wb_data = json.loads(urllib.request.urlopen(wb_req, timeout=15).read())
                for f in wb_data.get("fields", []):
                    if f.get("external_id") == "gross-profit-2":
                        v = f.get("values",[])
                        gp = float(v[0].get("value",0) or 0) if v else 0
                        closed_by_agent_month[mk][first] += gp
            except: pass
        if len(items2) < 200: break
        offset2 += 200
    print(f"  Closed by agent: {dict({mk: dict(v) for mk,v in closed_by_agent_month.items()})}")
except Exception as e:
    print(f"  ⚠️ Closed by agent fetch failed ({e})")

# ── 7. Discoveries by agent by month (Qualified On date, Julian + Charles only) ──
print("Pulling discoveries...")
DISCOVERY_AGENTS = {"Julian", "Charles"}
discoveries_by_month = {}  # {"2026-03": {"Julian": 38, "Charles": 29}}
OFFERS_VIEW_ID    = "61748357"
ACCEPTED_VIEW_ID  = "61748408"
DISC_MONTH_RANGES = [
    ("2026-01", "2026-01-01", "2026-01-31"),
    ("2026-02", "2026-02-01", "2026-02-28"),
    ("2026-03", "2026-03-01", "2026-03-31"),
    ("2026-04", "2026-04-01", "2026-04-30"),
    ("2026-05", "2026-05-01", "2026-05-31"),
    ("2026-06", "2026-06-01", "2026-06-30"),
    ("2026-07", "2026-07-01", "2026-07-31"),
    ("2026-08", "2026-08-01", "2026-08-31"),
    ("2026-09", "2026-09-01", "2026-09-30"),
    ("2026-10", "2026-10-01", "2026-10-31"),
    ("2026-11", "2026-11-01", "2026-11-30"),
    ("2026-12", "2026-12-01", "2026-12-31"),
]
try:
    for mk, since, until in DISC_MONTH_RANGES:
        if since > today_str:
            break
        offset = 0
        month_disc = {}
        while True:
            body = json.dumps({"limit": 200, "offset": offset,
                "filters": {"qualified-on": {"from": since, "to": until}}}).encode()
            req = urllib.request.Request(f"https://api.podio.com/item/app/{PODIO_APP_ID}/filter/",
                data=body, headers={"Content-Type": "application/json",
                "Authorization": f"Bearer {podio_token}", "Accept-Encoding": "gzip"}, method="POST")
            with urllib.request.urlopen(req, timeout=30) as r:
                raw = r.read()
                result = json.loads(raw)
            items = result.get("items", [])
            for item in items:
                agent_full = ""
                for f in item.get("fields", []):
                    if f.get("external_id") == "agent-name-hidden":
                        vals = f.get("values", [])
                        agent_full = str(vals[0].get("value", "")) if vals else ""
                for agent in DISCOVERY_AGENTS:
                    if agent in agent_full:
                        month_disc[agent] = month_disc.get(agent, 0) + 1
            if len(items) < 200: break
            offset += 200
        discoveries_by_month[mk] = month_disc
    total_disc = sum(sum(v.values()) for v in discoveries_by_month.values())
    print(f"  Discoveries pulled: {total_disc} across {len(discoveries_by_month)} months")
except Exception as e:
    print(f"  ⚠️ Discoveries fetch failed ({e})")
    discoveries_by_month = {}

# Inject discoveries into reps_by_month
for mk, agents in discoveries_by_month.items():
    if mk not in reps_by_month:
        reps_by_month[mk] = {}
    for agent, count in agents.items():
        if agent not in reps_by_month[mk]:
            reps_by_month[mk][agent] = blank_rep()
        reps_by_month[mk][agent]["leads"] = count

# ── 7b. Offers by agent by month (sort by offered-on-hidden desc, stop at pre-2026) ──
print("Pulling offers...")
offers_by_month = {}
try:
    offset = 0
    while True:
        body = json.dumps({"limit": 100, "offset": offset,
            "sort_by": "offered-on-hidden", "sort_desc": True}).encode()
        req = urllib.request.Request(
            f"https://api.podio.com/item/app/{PODIO_APP_ID}/filter/{OFFERS_VIEW_ID}/",
            data=body, headers={"Content-Type": "application/json",
            "Authorization": f"Bearer {podio_token}", "Accept-Encoding": "gzip"}, method="POST")
        with urllib.request.urlopen(req, timeout=60) as r:
            import gzip as _gz2, io as _io2
            raw = r.read()
            if r.headers.get("Content-Encoding","") == "gzip":
                raw = _gz2.GzipFile(fileobj=_io2.BytesIO(raw)).read()
            result = json.loads(raw.decode("utf-8"))
        items = result.get("items", [])
        if not items: break
        stop = False
        for item in items:
            fields = item.get("fields", [])
            agent_full = offered_on = ""
            for f in fields:
                ext = f.get("external_id","")
                vals = f.get("values", [])
                if ext == "agent-name-hidden" and vals:
                    agent_full = str(vals[0].get("value", ""))
                if ext == "offered-on-hidden" and vals:
                    offered_on = (vals[0].get("start_date") or vals[0].get("start",""))[:10]
            if offered_on and offered_on < "2026-01-01":
                stop = True
                break
            if offered_on:
                mk = offered_on[:7]
                offers_by_month.setdefault(mk, {})
                for agent in DISCOVERY_AGENTS:
                    if agent in agent_full:
                        offers_by_month[mk][agent] = offers_by_month[mk].get(agent, 0) + 1
        if stop or len(items) < 100: break
        offset += 100
    total_off = sum(sum(v.values()) for v in offers_by_month.values())
    print(f"  Offers pulled: {total_off} across {len(offers_by_month)} months")
except Exception as e:
    print(f"  ⚠️ Offers fetch failed ({e})")
    offers_by_month = {}

# Inject offers into reps_by_month
for mk, agents in offers_by_month.items():
    if mk not in reps_by_month:
        reps_by_month[mk] = {}
    for agent, count in agents.items():
        if agent not in reps_by_month[mk]:
            reps_by_month[mk][agent] = blank_rep()
        reps_by_month[mk][agent]["offers"] = count

# ── 7c. Accepted (Under Contract) by agent by month ──
print("Pulling accepted (under contract)...")
accepted_by_month = {}
try:
    offset = 0
    while True:
        body = json.dumps({"limit": 100, "offset": offset,
            "sort_by": "under-contract", "sort_desc": True}).encode()
        req = urllib.request.Request(
            f"https://api.podio.com/item/app/{PODIO_APP_ID}/filter/{ACCEPTED_VIEW_ID}/",
            data=body, headers={"Content-Type": "application/json",
            "Authorization": f"Bearer {podio_token}", "Accept-Encoding": "gzip"}, method="POST")
        with urllib.request.urlopen(req, timeout=60) as r:
            import gzip as _gz3, io as _io3
            raw = r.read()
            if r.headers.get("Content-Encoding","") == "gzip":
                raw = _gz3.GzipFile(fileobj=_io3.BytesIO(raw)).read()
            result = json.loads(raw.decode("utf-8"))
        items = result.get("items", [])
        if not items: break
        stop = False
        for item in items:
            fields = item.get("fields", [])
            agent_full = contract_on = ""
            for f in fields:
                ext = f.get("external_id","")
                vals = f.get("values", [])
                if ext == "agent-name-hidden" and vals:
                    agent_full = str(vals[0].get("value",""))
                if ext == "under-contract" and vals:
                    contract_on = (vals[0].get("start_date") or vals[0].get("start",""))[:10]
            if contract_on and contract_on < "2026-01-01":
                stop = True
                break
            if contract_on:
                mk = contract_on[:7]
                accepted_by_month.setdefault(mk, {})
                for agent in DISCOVERY_AGENTS:
                    if agent in agent_full:
                        accepted_by_month[mk][agent] = accepted_by_month[mk].get(agent, 0) + 1
        if stop or len(items) < 100: break
        offset += 100
    total_acc = sum(sum(v.values()) for v in accepted_by_month.values())
    print(f"  Accepted pulled: {total_acc} across {len(accepted_by_month)} months")
except Exception as e:
    print(f"  ⚠️ Accepted fetch failed ({e})")
    accepted_by_month = {}

# Inject accepted into reps_by_month
for mk, agents in accepted_by_month.items():
    if mk not in reps_by_month:
        reps_by_month[mk] = {}
    for agent, count in agents.items():
        if agent not in reps_by_month[mk]:
            reps_by_month[mk][agent] = blank_rep()
        reps_by_month[mk][agent]["accepted"] = count

# Inject closed revenue per agent per month
for mk, agents in closed_by_agent_month.items():
    if mk not in reps_by_month:
        reps_by_month[mk] = {}
    for agent, amount in agents.items():
        if agent not in reps_by_month[mk]:
            reps_by_month[mk][agent] = blank_rep()
        reps_by_month[mk][agent]["closed_revenue"] = round(amount, 2)

# ── 7d. Whiteboard pipeline snapshot per rep (contracts by status) ─────────────
print("Pulling Whiteboard pipeline per rep...")
AGENT_PROFILE_IDS = {"Julian": 271855534, "Charles": 272037927}

def wb_pipeline_for_agent(profile_id, token):
    """Pull all whiteboard deals for a given agent profile_id and bucket by status."""
    h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json", "Accept-Encoding": "gzip"}
    buckets = {"closed": 0, "pending": 0, "available": 0, "dead": 0, "on_hold": 0, "other": 0}
    gp_buckets = {"closed": 0.0, "pending": 0.0, "available": 0.0, "dead": 0.0, "on_hold": 0.0, "other": 0.0}
    total_deals = 0
    offset = 0
    while True:
        body = json.dumps({
            "limit": 200, "offset": offset,
            "filters": {"agent": [profile_id]},
            "sort_by": "created_on", "sort_desc": True
        }).encode()
        req = urllib.request.Request(
            f"https://api.podio.com/item/app/{WHITEBOARD_APP_ID}/filter/",
            data=body, headers=h, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                raw = r.read()
                if r.headers.get("Content-Encoding","") == "gzip":
                    raw = _gzip.GzipFile(fileobj=_io.BytesIO(raw)).read()
                result = json.loads(raw)
        except Exception as e:
            print(f"  ⚠️ WB pipeline fetch error: {e}")
            break
        items = result.get("items", [])
        for item in items:
            fields = {f["external_id"]: f.get("values") for f in item.get("fields", [])}
            status_vals = fields.get("status-2") or []
            status = ""
            if status_vals:
                sv = status_vals[0].get("value", {})
                status = sv.get("text","") if isinstance(sv, dict) else str(sv)
            gp_vals = fields.get("gross-profit-2") or []
            gp = 0.0
            try: gp = float(gp_vals[0].get("value", 0) or 0) if gp_vals else 0.0
            except: pass
            status_lower = status.lower()
            if "closed" in status_lower:
                key = "closed"
            elif "title" in status_lower or "pending" in status_lower or "assigned" in status_lower:
                key = "pending"
            elif "available" in status_lower:
                key = "available"
            elif "dead" in status_lower:
                key = "dead"
            elif "hold" in status_lower:
                key = "on_hold"
            else:
                key = "other"
            buckets[key] += 1
            gp_buckets[key] += gp
            total_deals += 1
        if len(items) < 200:
            break
        offset += 200
    return {"counts": buckets, "gp": {k: round(v) for k, v in gp_buckets.items()}, "total": total_deals}

# Re-auth whiteboard for this pull
try:
    wb_auth3 = post("https://podio.com/oauth/token", {
        "grant_type": "app", "app_id": WHITEBOARD_APP_ID, "app_token": WHITEBOARD_APP_TOKEN,
        "client_id": PODIO_CLIENT_ID, "client_secret": PODIO_SECRET
    }, form=True)
    wb_token3 = wb_auth3["access_token"]
    wb_pipeline = {}
    for agent_name, profile_id in AGENT_PROFILE_IDS.items():
        wb_pipeline[agent_name] = wb_pipeline_for_agent(profile_id, wb_token3)
        c = wb_pipeline[agent_name]["counts"]
        print(f"  {agent_name}: {wb_pipeline[agent_name]['total']} total | closed={c['closed']} pending={c['pending']} available={c['available']} dead={c['dead']} on_hold={c['on_hold']}")
except Exception as e:
    print(f"  ⚠️ WB pipeline per rep failed: {e}")
    wb_pipeline = {}

# ── 8. Build data.json ────────────────────────────────────────────────────────
print("Building data.json...")
data = {
    "report_date": now.strftime("%B %d, %Y"),
    "last_updated": now.strftime("%Y-%m-%d %H:%M AST"),

    "google": {
        "spend_mtd": round(google_spend_by_month.get(current_month, 0), 2),
        "clicks_mtd": total_google_clicks,
        "crm_leads": ppc_leads_30d,
        "cpc": google_cpc,
        "cpl": google_cpl,
        "cost_per_booked": None,
        "booked": 0,
        "offers": 0,
        "accepted": 0,
        "campaigns": google_campaigns
    },

    "meta": {
        "spend_30d": round(total_meta_spend, 2),
        "clicks_30d": total_meta_clicks,
        "crm_leads": fb_leads_30d,
        "cpc": meta_cpc,
        "cpl_crm": meta_cpl,
        "cost_per_hot": cost_per_hot,
        "campaigns": meta_campaigns
    },

    "potential_income": potential_income,
    "assigned_income": assigned_income,
    "by_stage": by_stage,
    "by_agent": by_agent,
    "closed_by_month": closed_by_month,
    "roa_by_month": {k: v for k, v in sorted(roa_by_month.items()) if k <= current_month},
    "reps_by_month": {k: v for k, v in sorted(reps_by_month.items()) if k <= current_month},
    "wb_pipeline": wb_pipeline,
    "spend_by_month": {
        mk: round(meta_spend_by_month.get(mk, 0) + google_spend_by_month.get(mk, 0) + sms_spend_by_month.get(mk, 0), 2)
        for mk, since, _ in META_MONTH_RANGES if since <= today_str
    },
    "channel_by_month": {
        mk: {
            "google_spend":  round(google_spend_by_month.get(mk, 0), 2),
            "meta_spend":    round(meta_spend_by_month.get(mk, 0), 2),
            "ppc_leads":     ppc_leads_by_month.get(mk, 0),
            "fb_leads":      fb_leads_by_month.get(mk, 0),
            "google_cpc":    round(google_spend_by_month.get(mk,0) / google_clicks_by_month[mk], 2) if google_clicks_by_month.get(mk) else None,
            "meta_cpc":      round(meta_spend_by_month.get(mk,0)  / meta_clicks_by_month[mk],   2) if meta_clicks_by_month.get(mk)  else None,
            "pipeline_ppc":  ppc_pipeline_by_month.get(mk, {}),
            "pipeline_fb":   fb_pipeline_by_month.get(mk, {}),
        }
        for mk, since, _ in META_MONTH_RANGES if since <= today_str
    },

    "pipeline": {
        "new_leads_30d": ppc_leads_mtd + fb_leads_mtd,
        "hot": len(hot_leads),
        "booked": fb_pipeline["booked"] + ppc_pipeline["booked"],
        "offers": fb_pipeline["offered"] + ppc_pipeline["offered"],
        "under_contract": under_contract,
        "closed": closed_total
    },

    "pipeline_fb": fb_pipeline,
    "pipeline_ppc": ppc_pipeline,

    "sources": {
        "ppc": ppc_leads_30d,
        "facebook": fb_leads_30d,
        "ppc_inbound_call": call_leads_30d
    },

    "hot_leads": [
        {
            "name": l["name"],
            "address": l["address"],
            "phone": l["phone"],
            "source": l["channel"]
        }
        for l in hot_leads[:5]
    ],

    "sms_status": f"⚠️ SMS data unavailable: {sms_fetch_error}" if sms_fetch_error else "SMS data via Podio."
}

# ── 6. Push to GitHub ─────────────────────────────────────────────────────────
print("Pushing to GitHub...")
content_str = json.dumps(data, indent=2)

# Always push current data.json
sha = get_github_sha(GITHUB_FILE)
result = put_github(GITHUB_FILE, content_str, sha)

# Save monthly snapshots for all past + current months (so month picker works)
all_months_in_data = sorted(set(list(reps_by_month.keys()) + [current_month]), reverse=True)
for snap_month in all_months_in_data:
    if snap_month > current_month:
        continue
    # Build a month-specific payload with filtered reps_by_month
    snap_data = dict(data)
    # Keep full reps_by_month in every snapshot so multi-month charts work on all views
    snap_str = json.dumps(snap_data, default=str)
    snap_file = f"data-{snap_month}.json"
    snap_sha = get_github_sha(snap_file)
    put_github(snap_file, snap_str, snap_sha)

# Keep months.json updated
try:
    months_req = urllib.request.Request(
        f"https://api.github.com/repos/{GITHUB_REPO}/contents/months.json",
        headers={"Authorization": f"token {GITHUB_TOKEN}"})
    months_data = json.loads(urllib.request.urlopen(months_req, timeout=10).read())
    existing_months = json.loads(base64.b64decode(months_data["content"].replace("\n","")).decode())
    months_sha = months_data["sha"]
except:
    existing_months = []
    months_sha = None

# Only include months that actually have snapshot files (no future months)
correct_months = sorted(all_months_in_data, reverse=True)
if correct_months != existing_months:
    put_github("months.json", json.dumps(correct_months), months_sha)

print(f"✅ Dashboard updated: https://nestor-lang.github.io/uig-dashboard/")
print(f"   Commit: {result.get('commit', {}).get('sha', '')[:8]}")
print(f"   Leads (30d): {len(leads_30d)} | PPC: {ppc_leads_30d} | FB: {fb_leads_30d}")
print(f"   Google spend MTD: ${total_google_spend:,.2f} | Meta spend 30d: ${total_meta_spend:,.2f}")
