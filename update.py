#!/usr/bin/env python3
"""
UIG Daily Dashboard Updater
Pulls data from Podio, Google Ads, and Meta Ads → writes data.json → pushes to GitHub → notifies Telegram.
Runs daily via OpenClaw on Mac Mini at 7:30 AM AST.
"""

import os
import json
import logging
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

# ─── Logging ───────────────────────────────────────────────────────────────────

LOG_DIR = Path.home() / "atlas" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "dashboard.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("uig-dashboard")

# ─── Environment ───────────────────────────────────────────────────────────────

def env(key):
    val = os.environ.get(key)
    if not val:
        raise EnvironmentError(f"Missing env var: {key}")
    return val

# ─── Podio API ─────────────────────────────────────────────────────────────────

class PodioClient:
    BASE = "https://api.podio.com"

    def __init__(self):
        self.client_id = env("PODIO_CLIENT_ID")
        self.client_secret = env("PODIO_CLIENT_SECRET")
        self.app_id = env("PODIO_APP_ID")
        self.token = None

    def authenticate(self):
        """Authenticate with Podio using app credentials."""
        resp = requests.post(f"{self.BASE}/oauth/token", data={
            "grant_type": "app",
            "app_id": self.app_id,
            "app_token": self.client_secret,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        })
        resp.raise_for_status()
        self.token = resp.json()["access_token"]
        log.info("Podio authenticated")

    def _headers(self):
        return {"Authorization": f"OAuth2 {self.token}"}

    def filter_items(self, filters, limit=500):
        """Filter items in the Podio app. Returns list of items."""
        all_items = []
        offset = 0
        while True:
            resp = requests.post(
                f"{self.BASE}/item/app/{self.app_id}/filter/",
                headers=self._headers(),
                json={"filters": filters, "limit": limit, "offset": offset},
            )
            resp.raise_for_status()
            data = resp.json()
            all_items.extend(data.get("items", []))
            if len(all_items) >= data.get("total", 0):
                break
            offset += limit
        return all_items

    def get_field_value(self, item, field_label):
        """Extract field value from a Podio item by external_id or label."""
        for field in item.get("fields", []):
            if field.get("external_id") == field_label or field.get("label") == field_label:
                values = field.get("values", [])
                if not values:
                    return None
                val = values[0].get("value", {})
                if isinstance(val, dict):
                    return val.get("text", val.get("id", str(val)))
                return val
        return None


def fetch_podio_data():
    """Fetch all pipeline, source, and hot lead data from Podio."""
    log.info("Fetching Podio data...")
    podio = PodioClient()
    podio.authenticate()

    thirty_days_ago = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # All leads created in last 30 days
    recent_items = podio.filter_items({
        "created_on": {"from": thirty_days_ago, "to": today}
    })
    log.info(f"Podio: {len(recent_items)} items in last 30 days")

    # Count pipeline stages
    pipeline = {
        "new_leads_30d": len(recent_items),
        "hot": 0,
        "booked": 0,
        "offers": 0,
        "under_contract": 0,
        "closed": 0,
    }

    sources = {"ppc": 0, "facebook": 0, "ppc_inbound_call": 0, "sms": 0}
    hot_leads = []

    for item in recent_items:
        disposition = podio.get_field_value(item, "call-disposition") or ""
        call_status = podio.get_field_value(item, "call-status") or ""
        lead_source = podio.get_field_value(item, "lead-source") or ""

        # Normalize to lowercase for matching
        disposition_lower = str(disposition).lower()
        call_status_lower = str(call_status).lower()
        lead_source_lower = str(lead_source).lower()

        # Pipeline counts
        if "hot lead" in disposition_lower:
            pipeline["hot"] += 1
        if "appointment booked" in call_status_lower:
            pipeline["booked"] += 1

        # Source breakdown
        if lead_source_lower == "ppc":
            sources["ppc"] += 1
        elif lead_source_lower == "facebook":
            sources["facebook"] += 1
        elif lead_source_lower in ("ppc - inbound call", "ppc-inbound call", "ppc inbound call"):
            sources["ppc_inbound_call"] += 1
        elif lead_source_lower == "sms":
            sources["sms"] += 1

    # Stage-based counts (not date-filtered — these are current pipeline state)
    # Under Contract
    uc_items = podio.filter_items({"created_on": {"from": thirty_days_ago, "to": today}})
    for item in recent_items:
        # Check for stage/status fields that indicate contract or closed
        disposition = str(podio.get_field_value(item, "call-disposition") or "").lower()
        call_status = str(podio.get_field_value(item, "call-status") or "").lower()

        if "under contract" in disposition or "under contract" in call_status:
            pipeline["under_contract"] += 1
        if "closed" in disposition or "won" in disposition or "closed" in call_status:
            pipeline["closed"] += 1
        if "offer" in disposition or "offer" in call_status:
            pipeline["offers"] += 1

    # Hot leads detail (most recent 10)
    for item in recent_items:
        disposition = str(podio.get_field_value(item, "call-disposition") or "").lower()
        if "hot lead" not in disposition:
            continue
        lead_source = podio.get_field_value(item, "lead-source") or ""
        hot_leads.append({
            "name": item.get("title", "Unknown"),
            "address": podio.get_field_value(item, "property-address") or podio.get_field_value(item, "address") or "",
            "phone": podio.get_field_value(item, "phone") or podio.get_field_value(item, "phone-number") or "",
            "source": str(lead_source),
        })
        if len(hot_leads) >= 10:
            break

    return pipeline, sources, hot_leads


# ─── Google Ads API ────────────────────────────────────────────────────────────

def fetch_google_ads_data():
    """Fetch MTD spend and clicks per campaign from Google Ads."""
    log.info("Fetching Google Ads data...")

    from google.ads.googleads.client import GoogleAdsClient

    config = {
        "developer_token": env("GOOGLE_ADS_DEVELOPER_TOKEN"),
        "client_id": env("GOOGLE_ADS_CLIENT_ID"),
        "client_secret": env("GOOGLE_ADS_CLIENT_SECRET"),
        "refresh_token": env("GOOGLE_ADS_REFRESH_TOKEN"),
        "use_proto_plus": True,
    }
    client = GoogleAdsClient.load_from_dict(config)
    customer_id = env("GOOGLE_ADS_CUSTOMER_ID").replace("-", "")

    ga_service = client.get_service("GoogleAdsService")

    query = """
        SELECT
            campaign.name,
            metrics.cost_micros,
            metrics.clicks
        FROM campaign
        WHERE
            segments.date DURING THIS_MONTH
            AND campaign.status = 'ENABLED'
    """

    response = ga_service.search_stream(customer_id=customer_id, query=query)

    campaigns = {}
    total_spend = 0
    total_clicks = 0

    for batch in response:
        for row in batch.results:
            name = row.campaign.name
            spend = row.metrics.cost_micros / 1_000_000
            clicks = row.metrics.clicks

            if name not in campaigns:
                campaigns[name] = {"name": name, "spend": 0, "clicks": 0}
            campaigns[name]["spend"] += spend
            campaigns[name]["clicks"] += clicks
            total_spend += spend
            total_clicks += clicks

    campaign_list = []
    for c in campaigns.values():
        c["spend"] = round(c["spend"], 2)
        c["cpc"] = round(c["spend"] / c["clicks"], 2) if c["clicks"] else 0
        # crm_leads and cpl will be computed after Podio data merge
        c["crm_leads"] = 0
        c["cpl"] = None
        campaign_list.append(c)

    # Sort by spend descending
    campaign_list.sort(key=lambda x: x["spend"], reverse=True)

    return {
        "spend_mtd": round(total_spend, 2),
        "clicks_mtd": total_clicks,
        "campaigns": campaign_list,
    }


# ─── Meta Ads API ──────────────────────────────────────────────────────────────

def fetch_meta_ads_data():
    """Fetch last 30d spend and clicks per campaign from Meta Ads."""
    log.info("Fetching Meta Ads data...")

    access_token = env("META_ACCESS_TOKEN")
    ad_account_id = env("META_AD_ACCOUNT_ID")

    # Ensure ad_account_id has act_ prefix
    if not ad_account_id.startswith("act_"):
        ad_account_id = f"act_{ad_account_id}"

    url = f"https://graph.facebook.com/v21.0/{ad_account_id}/insights"
    params = {
        "access_token": access_token,
        "date_preset": "last_30d",
        "level": "campaign",
        "fields": "campaign_name,spend,clicks",
        "limit": 100,
    }

    resp = requests.get(url, params=params)
    resp.raise_for_status()
    data = resp.json().get("data", [])

    campaigns = []
    total_spend = 0
    total_clicks = 0

    for row in data:
        spend = float(row.get("spend", 0))
        clicks = int(row.get("clicks", 0))
        total_spend += spend
        total_clicks += clicks
        campaigns.append({
            "name": row.get("campaign_name", "Unknown"),
            "spend": round(spend, 2),
            "clicks": clicks,
            "crm_leads": 0,  # Will be set from Podio
            "cpl_crm": None,
        })

    # Sort by spend descending
    campaigns.sort(key=lambda x: x["spend"], reverse=True)

    return {
        "spend_30d": round(total_spend, 2),
        "clicks_30d": total_clicks,
        "campaigns": campaigns,
    }


# ─── Assemble data.json ───────────────────────────────────────────────────────

def assemble_data(pipeline, sources, hot_leads, google, meta):
    """Combine all data sources into the final data.json structure."""

    ast = timezone(timedelta(hours=-4))
    now = datetime.now(ast)

    # Google channel lead counts from Podio
    google_crm_leads = sources["ppc"] + sources["ppc_inbound_call"]
    google["crm_leads"] = google_crm_leads
    google["cpl"] = round(google["spend_mtd"] / google_crm_leads, 2) if google_crm_leads else None
    google["booked"] = pipeline["booked"]
    google["cost_per_booked"] = round(google["spend_mtd"] / pipeline["booked"], 2) if pipeline["booked"] else None
    google["offers"] = pipeline["offers"]
    google["accepted"] = pipeline["under_contract"]

    # Meta channel lead counts from Podio
    meta_crm_leads = sources["facebook"]
    meta["crm_leads"] = meta_crm_leads
    meta["cpl_crm"] = round(meta["spend_30d"] / meta_crm_leads, 2) if meta_crm_leads else None
    meta["cost_per_hot"] = round(meta["spend_30d"] / pipeline["hot"], 2) if pipeline["hot"] else None

    data = {
        "report_date": now.strftime("%B %d, %Y").replace(" 0", " "),
        "last_updated": now.strftime("%Y-%m-%d %H:%M") + " AST",
        "google": google,
        "meta": meta,
        "pipeline": pipeline,
        "sources": sources,
        "hot_leads": hot_leads,
        "sms_status": None,
    }

    return data


# ─── Git push ──────────────────────────────────────────────────────────────────

def git_push(repo_path):
    """Commit and push data.json to GitHub."""
    log.info("Pushing to GitHub...")
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M")

    def run(cmd):
        result = subprocess.run(cmd, cwd=repo_path, capture_output=True, text=True)
        if result.returncode != 0:
            log.error(f"git error: {result.stderr}")
            raise RuntimeError(f"git failed: {' '.join(cmd)}\n{result.stderr}")
        return result.stdout

    # Configure git to use token for this push
    token = env("GITHUB_TOKEN")
    username = env("GITHUB_USERNAME")
    repo = env("GITHUB_REPO")
    remote_url = f"https://{username}:{token}@github.com/{username}/{repo}.git"

    run(["git", "remote", "set-url", "origin", remote_url])
    run(["git", "add", "data.json"])

    # Check if there are changes to commit
    status = run(["git", "status", "--porcelain"])
    if not status.strip():
        log.info("No changes to commit")
        return

    run(["git", "commit", "-m", f"data update {date_str}"])
    run(["git", "push", "origin", "main"])
    log.info("Pushed to GitHub successfully")


# ─── Telegram notification ─────────────────────────────────────────────────────

def send_telegram(data, error=None):
    """Send confirmation or error message via Telegram."""
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        log.warning("Telegram credentials not set, skipping notification")
        return

    username = os.environ.get("GITHUB_USERNAME", "user")
    repo = os.environ.get("GITHUB_REPO", "uig-dashboard")

    if error:
        msg = f"⚠️ Dashboard update failed — {data.get('report_date', 'unknown date')}\nError: {error}\nView: https://{username}.github.io/{repo}/"
    else:
        g_spend = data.get("google", {}).get("spend_mtd", 0)
        m_spend = data.get("meta", {}).get("spend_30d", 0)
        total_leads = data.get("pipeline", {}).get("new_leads_30d", 0)
        hot = data.get("pipeline", {}).get("hot", 0)
        msg = (
            f"✅ Dashboard updated — {data.get('report_date', '')}\n"
            f"Spend: Google ${g_spend:,.0f} | Meta ${m_spend:,.0f}\n"
            f"CRM Leads: {total_leads} | Hot: {hot}\n"
            f"View: https://{username}.github.io/{repo}/"
        )

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    requests.post(url, json={"chat_id": chat_id, "text": msg}, timeout=10)
    log.info("Telegram notification sent")


# ─── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("UIG Dashboard update starting")

    repo_path = env("REPO_LOCAL_PATH")
    errors = []
    data = {}

    # Step 1: Podio
    pipeline, sources, hot_leads = None, None, None
    try:
        pipeline, sources, hot_leads = fetch_podio_data()
    except Exception as e:
        log.error(f"Podio failed: {e}")
        errors.append(f"Podio: {e}")
        pipeline = {"new_leads_30d": 0, "hot": 0, "booked": 0, "offers": 0, "under_contract": 0, "closed": 0}
        sources = {"ppc": 0, "facebook": 0, "ppc_inbound_call": 0, "sms": 0}
        hot_leads = []

    # Step 2: Google Ads
    google = None
    try:
        google = fetch_google_ads_data()
    except Exception as e:
        log.error(f"Google Ads failed: {e}")
        errors.append(f"Google Ads: {e}")
        google = {"spend_mtd": 0, "clicks_mtd": 0, "campaigns": []}

    # Step 3: Meta Ads
    meta = None
    try:
        meta = fetch_meta_ads_data()
    except Exception as e:
        log.error(f"Meta Ads failed: {e}")
        errors.append(f"Meta Ads: {e}")
        meta = {"spend_30d": 0, "clicks_30d": 0, "campaigns": []}

    # Step 4–5: Assemble
    data = assemble_data(pipeline, sources, hot_leads, google, meta)

    if errors:
        data["error"] = "; ".join(errors)

    # Step 6: Write data.json
    data_path = Path(repo_path) / "data.json"
    with open(data_path, "w") as f:
        json.dump(data, f, indent=2)
    log.info(f"Wrote {data_path}")

    # Step 7: Push to GitHub
    try:
        git_push(repo_path)
    except Exception as e:
        log.error(f"Git push failed: {e}")
        errors.append(f"Git push: {e}")

    # Step 8: Telegram
    try:
        send_telegram(data, error="; ".join(errors) if errors else None)
    except Exception as e:
        log.error(f"Telegram failed: {e}")

    if errors:
        log.warning(f"Completed with errors: {errors}")
        sys.exit(1)
    else:
        log.info("Dashboard update complete ✓")


if __name__ == "__main__":
    main()
