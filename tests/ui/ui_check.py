"""Headless UI check of the redesigned dashboard (run manually: .venv/bin/python tests/ui/ui_check.py [base_url] [--feed URL]).
Desktop 1280x800 + mobile 390x844: zero console errors, zero failed API requests, every section renders, Demo/Live modal,
stake + auto-trade controls round-trip through the API (restored afterwards). Screenshots -> data/."""
import json, sys, time, os
from playwright.sync_api import sync_playwright
BASE = sys.argv[1] if len(sys.argv) > 1 and not sys.argv[1].startswith("--") else "http://127.0.0.1:8700"
FEED = sys.argv[sys.argv.index("--feed") + 1] if "--feed" in sys.argv else None
OUT = os.path.join(os.path.dirname(__file__), "..", "..", "data")
TABS = ["Live", "Trades", "History", "Model", "Settings"]
res = {"errors": [], "failed": [], "checks": []}
def ok(name, cond, extra=""):
    res["checks"].append((name, bool(cond), extra)); print(("PASS " if cond else "FAIL ") + name, extra, flush=True)

def wire(page, tag):
    page.on("console", lambda m: m.type == "error" and res["errors"].append(f"{tag}: {m.text}"))
    page.on("pageerror", lambda e: res["errors"].append(f"{tag}: pageerror {e}"))
    page.on("requestfailed", lambda r: res["failed"].append(f"{tag}: {r.url} {r.failure}"))
    page.on("response", lambda r: r.status >= 400 and "/api/" in r.url and not getattr(r.request, "_expected", False) and res["failed"].append(f"{tag}: {r.status} {r.url}"))

def tabs(page, tag, nav):
    for t in TABS:
        page.click(f"{nav} [data-t='{t}']"); page.wait_for_timeout(1500)
        vis = page.locator(f"section[data-tab='{t}']").is_visible()
        txt = page.locator(f"section[data-tab='{t}']").inner_text()
        ok(f"{tag} {t} renders", vis and len(txt) > 80, f"{len(txt)} chars")

with sync_playwright() as p:
    b = p.chromium.launch()
    url = BASE + "/" + (f"?feed={FEED}" if FEED else "")
    # ------------------------------------------------ desktop
    ctx = b.new_context(viewport={"width": 1280, "height": 800}, device_scale_factor=2)
    pg = ctx.new_page(); wire(pg, "desktop"); pg.goto(url); pg.wait_for_timeout(4000)
    ok("desktop top bar mode pill", pg.locator("#modeSeg button.on").count() == 1, pg.locator("#modeSeg button.on").inner_text() if pg.locator("#modeSeg button.on").count() else "")
    ok("desktop price shown", "$" in pg.inner_text("#px"), pg.inner_text("#px"))
    ok("desktop trading strip", "balance" in pg.inner_text("#strip").lower(), pg.inner_text("#strip").replace("\n", " | ")[:160])
    pg.click("#tabs [data-t='Live']"); pg.wait_for_timeout(2500)
    pg.screenshot(path=os.path.join(OUT, "redesign-preview.png" if not FEED else "redesign-feed-desktop.png"), full_page=False)
    tabs(pg, "desktop", "#tabs")
    if not FEED:
        # stake quick control round trip
        pg.click("#tabs [data-t='Trades']"); pg.wait_for_timeout(1200)
        before = json.loads(pg.evaluate("fetch('/api/trading').then(r=>r.text())"))["limits"]
        pg.click("#stakeQs button[data-v='10']"); pg.wait_for_timeout(1500)
        mid = json.loads(pg.evaluate("fetch('/api/trading').then(r=>r.text())"))["limits"]["stake_usd"]
        ok("stake quick-set via UI", mid == 10, f"stake now {mid}")
        pg.fill("#stakeIn", str(before["stake_usd"])); pg.click("#stakeSave"); pg.wait_for_timeout(1500)
        back = json.loads(pg.evaluate("fetch('/api/trading').then(r=>r.text())"))["limits"]["stake_usd"]
        ok("stake restored via input", back == before["stake_usd"], f"stake {back}")
        # auto-trade toggle round trip (top bar)
        a0 = json.loads(pg.evaluate("fetch('/api/trading').then(r=>r.text())"))["auto_trade"]
        pg.click("label.sw:has(#autoT)"); pg.wait_for_timeout(1500)
        a1 = json.loads(pg.evaluate("fetch('/api/trading').then(r=>r.text())"))["auto_trade"]
        pg.click("label.sw:has(#autoT)"); pg.wait_for_timeout(1500)
        a2 = json.loads(pg.evaluate("fetch('/api/trading').then(r=>r.text())"))["auto_trade"]
        ok("auto-trade toggle flips and restores", a1 == (not a0) and a2 == a0, f"{a0}->{a1}->{a2}")
        # live modal - real wallet state (not configured on this host)
        pg.click("#modeSeg button[data-m='live']"); pg.wait_for_timeout(1200)
        mt = pg.inner_text("#mbox") if pg.locator("#modal.on").count() else ""
        ok("LIVE without wallet -> instructions modal, stays demo", "wallet" in mt.lower() and "never paste" in mt.lower(), mt[:80].replace("\n", " "))
        pg.click("#mNo"); pg.wait_for_timeout(300)
        # live confirm modal with a mocked configured wallet; mode POST intercepted (never reaches the server)
        pg.route("**/api/trading/wallet", lambda r: r.fulfill(status=200, content_type="application/json", body=json.dumps({"configured": True, "connected": True, "funder": "0x1234…abcd", "usdc_balance": 50.0, "how_to_enable": []})))
        sent = []
        def mode_route(r):
            sent.append(json.loads(r.request.post_data or "{}")); r.fulfill(status=409, content_type="application/json", body=json.dumps({"detail": "test intercept - not sent"}))
        pg.route("**/api/trading/mode", mode_route)
        pg.click("#modeSeg button[data-m='live']"); pg.wait_for_timeout(1000)
        mt = pg.inner_text("#mbox")
        ok("LIVE confirm modal shows dollar limits", "Stake per round" in mt and "Max daily loss" in mt and "$" in mt, mt[:120].replace("\n", " "))
        ok("Enable LIVE disabled until acknowledged", pg.locator("#mYes").is_disabled())
        pg.screenshot(path=os.path.join(OUT, "redesign-live-modal.png"))
        pg.check("#ack"); ok("Enable LIVE enabled after acknowledgement", not pg.locator("#mYes").is_disabled())
        pg.click("#mYes"); pg.wait_for_timeout(1200)
        ok("confirm POST carries confirm + limits", sent and sent[0].get("confirm") is True and sent[0].get("mode") == "live" and "stake_usd" in (sent[0].get("limits") or {}), str(sent[:1])[:140])
        pg.unroute("**/api/trading/wallet"); pg.unroute("**/api/trading/mode")
        st = json.loads(pg.evaluate("fetch('/api/trading').then(r=>r.text())"))
        ok("server still in DEMO after test", st["mode"] == "demo")
        res["failed"] = [f for f in res["failed"] if "/api/trading/mode" not in f]   # the intercepted 409 is expected
        res["errors"] = [e for e in res["errors"] if "status of 409" not in e]
    pg.click("#tabs [data-t='Trades']"); pg.wait_for_timeout(1500); pg.screenshot(path=os.path.join(OUT, "redesign-trades.png" if not FEED else "redesign-feed-trades.png"))
    ctx.close()
    # ------------------------------------------------ mobile
    ctx = b.new_context(viewport={"width": 390, "height": 844}, device_scale_factor=3, is_mobile=True, has_touch=True)
    pg = ctx.new_page(); wire(pg, "mobile"); pg.goto(url); pg.wait_for_timeout(4000)
    ok("mobile bottom nav visible", pg.locator("#bnav").is_visible())
    ok("mobile no horizontal overflow", pg.evaluate("document.documentElement.scrollWidth<=window.innerWidth+1"), str(pg.evaluate("[document.documentElement.scrollWidth,window.innerWidth]")))
    pg.click("#bnav [data-t='Live']"); pg.wait_for_timeout(2500)
    pg.screenshot(path=os.path.join(OUT, "redesign-mobile.png" if not FEED else "redesign-feed-mobile.png"), full_page=False)
    pg.screenshot(path=os.path.join(OUT, "redesign-mobile-full.png" if not FEED else "redesign-feed-mobile-full.png"), full_page=True)
    tabs(pg, "mobile", "#bnav")
    for t in TABS:
        pg.click(f"#bnav [data-t='{t}']"); pg.wait_for_timeout(600)
        ok(f"mobile {t} no overflow", pg.evaluate("document.documentElement.scrollWidth<=window.innerWidth+1"))
    ctx.close(); b.close()
ok("zero JS console errors", not res["errors"], "; ".join(res["errors"])[:600])
ok("zero failed API requests", not res["failed"], "; ".join(res["failed"])[:600])
fails = [c for c in res["checks"] if not c[1]]
print(f"\n{len(res['checks']) - len(fails)}/{len(res['checks'])} UI checks passed"); sys.exit(1 if fails else 0)
