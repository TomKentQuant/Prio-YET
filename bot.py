"""Single-server Discord bot for public YouTube uploads and live starts."""

import asyncio
import hashlib
import hmac
import logging
import os
import re
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote, urlsplit

import aiohttp
from aiohttp import web
from defusedxml import ElementTree
import discord
from discord import app_commands

LOG = logging.getLogger("youtube-alerts")
FEED = "https://www.youtube.com/xml/feeds/videos.xml"
HUB = "https://pubsubhubbub.appspot.com/subscribe"
CHANNEL = re.compile(r"UC[A-Za-z0-9_-]{22}")
VIDEO = re.compile(r"[A-Za-z0-9_-]{11}")
NS = {"atom": "http://www.w3.org/2005/Atom", "yt": "http://www.youtube.com/xml/schemas/2015"}
DAY = 86400


def timestamp(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() if value else 0


def channel_filter(value):
    value = value.strip()
    if value.lower().startswith(("youtube.com/", "www.youtube.com/", "m.youtube.com/")):
        value = "https://" + value
    if "://" in value:
        url = urlsplit(value)
        if url.scheme not in ("http", "https") or url.hostname not in (
            "youtube.com", "www.youtube.com", "m.youtube.com"
        ):
            raise ValueError("Use a YouTube channel link, @handle, or channel ID.")
        parts = unquote(url.path).strip("/").split("/")
        value = parts[1] if len(parts) > 1 and parts[0] == "channel" else parts[0]
    if CHANNEL.fullmatch(value):
        return {"id": value}
    if value.startswith("@") and 1 < len(value) <= 101 and not any(
        character.isspace() or character in "/?#" for character in value
    ):
        return {"forHandle": value}
    raise ValueError("Use a YouTube channel link, @handle, or channel ID.")


class Monitor:
    def __init__(self, database, session, api_key, public_url, secret):
        Path(database).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(database)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            PRAGMA journal_mode = WAL;
            PRAGMA foreign_keys = ON;
            CREATE TABLE IF NOT EXISTS channels (
                id TEXT PRIMARY KEY, name TEXT, added REAL, next_feed REAL DEFAULT 0,
                feed_ok REAL DEFAULT 0, lease REAL DEFAULT 0, next_sub REAL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS videos (
                id TEXT PRIMARY KEY, channel TEXT REFERENCES channels(id) ON DELETE CASCADE,
                found REAL, due REAL, expires REAL, source TEXT, short INTEGER DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS video_due ON videos(due);
            CREATE TABLE IF NOT EXISTS outbox (
                id INTEGER PRIMARY KEY, video TEXT REFERENCES videos(id) ON DELETE CASCADE,
                kind TEXT, title TEXT, event_time REAL, queued REAL, due REAL,
                attempts INTEGER DEFAULT 0, sent REAL, UNIQUE(video, kind)
            );
            CREATE INDEX IF NOT EXISTS outbox_due ON outbox(sent, due);
            CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT);
        """)
        self.http = session
        self.api_key = api_key
        self.public_url = public_url.rstrip("/")
        self.secret = secret
        self.callback_path = "/youtube/" + hashlib.sha256(secret.encode()).hexdigest()
        self.wake = asyncio.Event()
        self.delivery_wake = asyncio.Event()
        self.last_api_ok = 0
        self.api_error = ""

    def sql(self, query, *params):
        return self.db.execute(query, params)

    async def api(self, resource, **params):
        async with self.http.get(
            f"https://www.googleapis.com/youtube/v3/{resource}",
            params=params, headers={"x-goog-api-key": self.api_key},
        ) as response:
            data = await response.json()
            if response.status != 200:
                errors = data.get("error", {}).get("errors", [])
                reason = errors[0].get("reason", "") if errors else ""
                raise RuntimeError(f"YouTube API HTTP {response.status} {reason}".strip())
            return data.get("items", [])

    def destination(self):
        row = self.sql("SELECT value FROM settings WHERE key='destination'").fetchone()
        return int(row["value"]) if row else None

    def ingest(self, payload, source, expected_channel=None):
        root = ElementTree.fromstring(payload)
        if root.tag != f"{{{NS['atom']}}}feed":
            raise ValueError("Expected a YouTube Atom feed")
        with self.db:
            for entry in root.findall("atom:entry", NS):
                video_id = entry.findtext("yt:videoId", default="", namespaces=NS)
                channel_id = entry.findtext("yt:channelId", default="", namespaces=NS)
                if not VIDEO.fullmatch(video_id) or not CHANNEL.fullmatch(channel_id):
                    continue
                if expected_channel and channel_id != expected_channel:
                    continue
                channel = self.sql("SELECT added FROM channels WHERE id=?", channel_id).fetchone()
                if channel is None:
                    continue
                now = time.time()
                published = timestamp(entry.findtext("atom:published", namespaces=NS))
                if published and published < now - 30 * DAY:
                    continue
                link = entry.find("atom:link[@rel='alternate']", NS)
                short = bool(link is not None and urlsplit(link.get("href", "")).path.startswith("/shorts/"))
                self.sql(
                    "INSERT OR IGNORE INTO videos VALUES (?, ?, ?, ?, ?, ?, ?)",
                    video_id, channel_id, now, now, now + DAY, source, short,
                )
        self.wake.set()

    async def callback(self, request):
        if request.method == "GET":
            topic = request.query.get("hub.topic", "")
            prefix = FEED + "?channel_id="
            channel = topic.removeprefix(prefix)
            challenge = request.query.get("hub.challenge", "")
            if (
                request.query.get("hub.mode") != "subscribe" or not topic.startswith(prefix)
                or not CHANNEL.fullmatch(channel) or not challenge
                or not self.sql("SELECT 1 FROM channels WHERE id=?", channel).fetchone()
            ):
                raise web.HTTPNotFound()
            try:
                lease = int(request.query.get("hub.lease_seconds", "0"))
            except ValueError:
                raise web.HTTPBadRequest() from None
            if lease <= 0:
                raise web.HTTPBadRequest()
            now = time.time()
            with self.db:
                self.sql(
                    "UPDATE channels SET lease=?, next_sub=? WHERE id=?",
                    now + lease, now + lease * 0.75, channel,
                )
            return web.Response(text=challenge)
        body = await request.read()
        signature = request.headers.get("X-Hub-Signature", "")
        digest = hmac.new(self.secret.encode(), body, hashlib.sha1).hexdigest()
        if not hmac.compare_digest(signature, "sha1=" + digest):
            raise web.HTTPForbidden()
        try:
            self.ingest(body, "push")
        except (ValueError, SyntaxError, ElementTree.ParseError) as error:
            raise web.HTTPBadRequest() from error
        return web.Response(status=204)

    async def subscribe(self, channel):
        data = {
            "hub.mode": "subscribe",
            "hub.topic": f"{FEED}?channel_id={channel}",
            "hub.callback": self.public_url + self.callback_path,
            "hub.secret": self.secret,
        }
        try:
            async with self.http.post(HUB, data=data) as response:
                if response.status not in (202, 204):
                    LOG.warning("Push subscription returned HTTP %s for %s", response.status, channel)
        except (aiohttp.ClientError, TimeoutError):
            LOG.warning("Push subscription request failed for %s", channel)

    async def feed(self, channel):
        try:
            async with self.http.get(FEED, params={"channel_id": channel}) as response:
                if response.status != 200:
                    LOG.warning("Feed HTTP %s for %s", response.status, channel)
                    return
                self.ingest(await response.read(), "feed", channel)
            with self.db:
                self.sql("UPDATE channels SET feed_ok=? WHERE id=?", time.time(), channel)
        except (aiohttp.ClientError, TimeoutError, ValueError, SyntaxError, ElementTree.ParseError):
            LOG.warning("Feed check failed for %s", channel)

    async def discover(self):
        while True:
            now = time.time()
            feeds = self.sql(
                "SELECT id FROM channels WHERE next_feed<=? ORDER BY next_feed LIMIT 8", now
            ).fetchall()
            subs = self.sql(
                "SELECT id FROM channels WHERE next_sub<=? ORDER BY next_sub LIMIT 8", now
            ).fetchall()
            with self.db:
                for row in feeds:
                    self.sql("UPDATE channels SET next_feed=? WHERE id=?", now + 60, row["id"])
                for row in subs:
                    self.sql("UPDATE channels SET next_sub=? WHERE id=?", now + 600, row["id"])
            await asyncio.gather(
                *(self.feed(row["id"]) for row in feeds),
                *(self.subscribe(row["id"]) for row in subs),
            )
            await asyncio.sleep(1)

    def apply_details(self, requested, items, now):
        indexed = {item["id"]: item for item in items}
        with self.db:
            for video_id in requested:
                row = self.sql("""
                    SELECT v.*, c.added FROM videos v JOIN channels c ON c.id=v.channel WHERE v.id=?
                """, video_id).fetchone()
                if row is None:
                    continue
                item = indexed.get(video_id)
                if item is None:
                    self.sql("UPDATE videos SET due=? WHERE id=?", now + 60, video_id)
                    continue
                snippet = item["snippet"]
                if snippet["channelId"] != row["channel"]:
                    self.sql("DELETE FROM videos WHERE id=?", video_id)
                    continue
                live = item.get("liveStreamingDetails", {})
                published = timestamp(snippet.get("publishedAt"))
                started = timestamp(live.get("actualStartTime"))
                scheduled = timestamp(live.get("scheduledStartTime"))
                state = snippet.get("liveBroadcastContent", "none")
                if started:
                    kind, event = "live", started
                elif state in ("upcoming", "live") or scheduled:
                    # YouTube can expose the event before its actual start timestamp.
                    if state == "upcoming" and scheduled:
                        due = max(now + 20, min(now + 900, scheduled - 60))
                    else:
                        due = now + 20
                    expires = max(row["expires"], scheduled + DAY)
                    self.sql("UPDATE videos SET due=?, expires=? WHERE id=?", due, expires, video_id)
                    continue
                else:
                    kind, event = ("short" if row["short"] else "upload"), published
                # Old uploads discovered on startup are recorded quietly.
                if event >= row["added"] and event >= now - 7 * DAY:
                    self.sql("""
                        INSERT OR IGNORE INTO outbox(video, kind, title, event_time, queued, due)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, video_id, kind, snippet["title"], event, now, now)
                self.sql("UPDATE videos SET due=NULL WHERE id=?", video_id)
        self.delivery_wake.set()

    async def enrich(self):
        next_call = 0
        next_cleanup = 0
        while True:
            now = time.time()
            if now >= next_cleanup:
                with self.db:
                    self.sql("UPDATE videos SET due=NULL WHERE due IS NOT NULL AND expires<?", now)
                    self.sql("""
                        DELETE FROM videos WHERE due IS NULL AND found<?
                        AND NOT EXISTS (SELECT 1 FROM outbox WHERE video=videos.id AND sent IS NULL)
                    """, now - 30 * DAY)
                next_cleanup = now + 3600
            ids = [
                row["id"] for row in self.sql(
                    "SELECT id FROM videos WHERE due<=? AND expires>=? ORDER BY due LIMIT 50", now, now
                )
            ]
            if not ids:
                self.wake.clear()
                try:
                    await asyncio.wait_for(self.wake.wait(), timeout=5)
                except TimeoutError:
                    pass
                continue
            if now < next_call:
                await asyncio.sleep(next_call - now)
                continue
            next_call = now + 20
            try:
                items = await self.api("videos", part="snippet,liveStreamingDetails", id=",".join(ids))
                self.apply_details(ids, items, time.time())
                self.last_api_ok = time.time()
                self.api_error = ""
            except (aiohttp.ClientError, TimeoutError, RuntimeError) as error:
                self.api_error = str(error) if isinstance(error, RuntimeError) else type(error).__name__
                LOG.warning("Video lookup failed: %s", self.api_error)
                next_call = time.time() + (900 if "quota" in self.api_error.lower() else 60)

    async def deliver(self, client):
        await client.wait_until_ready()
        while True:
            destination = self.destination()
            row = self.sql("""
                SELECT o.*, v.found, v.source, c.name FROM outbox o
                JOIN videos v ON o.video=v.id JOIN channels c ON c.id=v.channel
                WHERE o.sent IS NULL AND o.due<=?
                ORDER BY (o.kind='live') DESC, o.queued LIMIT 1
            """, time.time()).fetchone()
            if destination is None or row is None:
                self.delivery_wake.clear()
                try:
                    await asyncio.wait_for(self.delivery_wake.wait(), timeout=2)
                except TimeoutError:
                    pass
                continue
            video_id = row["video"]
            label = {"upload": "New upload", "short": "New Short", "live": "Live now"}[row["kind"]]
            path = f"shorts/{video_id}" if row["kind"] == "short" else f"watch?v={video_id}"
            embed = discord.Embed(
                title=row["title"][:256], url=f"https://www.youtube.com/{path}",
                description=f"**{label}** · {discord.utils.escape_markdown(row['name'])}",
                color=0xFF0033 if row["kind"] == "live" else 0x5865F2,
                timestamp=datetime.fromtimestamp(row["event_time"], tz=discord.utils.utcnow().tzinfo),
            )
            embed.set_thumbnail(url=f"https://i.ytimg.com/vi/{video_id}/mqdefault.jpg")
            try:
                channel = client.get_channel(destination) or await client.fetch_channel(destination)
                await channel.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
                sent = time.time()
                with self.db:
                    self.sql("UPDATE outbox SET sent=? WHERE id=?", sent, row["id"])
                LOG.info(
                    "Alert %s %s source=%s event_to_discord=%.1fs detected_to_discord=%.1fs",
                    row["kind"], video_id, row["source"], sent-row["event_time"], sent-row["found"],
                )
            except (discord.HTTPException, aiohttp.ClientError, TimeoutError) as error:
                attempts = row["attempts"] + 1
                with self.db:
                    self.sql(
                        "UPDATE outbox SET attempts=?, due=? WHERE id=?",
                        attempts, time.time() + min(900, 2 ** min(attempts, 10)), row["id"],
                    )
                LOG.warning("Discord delivery failed for %s (%s)", video_id, type(error).__name__)


def make_client(monitor, guild_id):
    guild = discord.Object(id=guild_id)

    class Client(discord.Client):
        async def setup_hook(self):
            commands.copy_global_to(guild=guild)
            await commands.sync(guild=guild)

    client = Client(intents=discord.Intents(guilds=True))
    commands = app_commands.CommandTree(client)
    class AdminCommands(app_commands.Group):
        async def interaction_check(self, interaction):
            if interaction.guild_id == guild_id and interaction.permissions.manage_guild:
                return True
            await interaction.response.send_message(
                "You need Manage Server to use this command.", ephemeral=True
            )
            return False

    youtube = AdminCommands(
        name="youtube", description="Manage YouTube alerts", guild_only=True,
        default_permissions=discord.Permissions(manage_guild=True),
    )

    @youtube.command(name="set-alert-channel", description="Choose where YouTube alerts go")
    async def set_alert_channel(interaction: discord.Interaction, channel: discord.TextChannel):
        permissions = channel.permissions_for(interaction.guild.me)
        if not (permissions.view_channel and permissions.send_messages and permissions.embed_links):
            await interaction.response.send_message(
                "Give the bot View Channel, Send Messages, and Embed Links in that channel.", ephemeral=True
            )
            return
        with monitor.db:
            monitor.sql("INSERT OR REPLACE INTO settings VALUES ('destination', ?)", str(channel.id))
        monitor.delivery_wake.set()
        await interaction.response.send_message(f"Alerts will go to {channel.mention}.", ephemeral=True)

    @youtube.command(description="Follow a YouTube channel")
    @app_commands.describe(channel="YouTube channel URL, @handle, or UC channel ID")
    async def add(interaction: discord.Interaction, channel: str):
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            items = await monitor.api("channels", part="snippet", **channel_filter(channel))
        except ValueError as error:
            await interaction.followup.send(str(error), ephemeral=True)
            return
        except (aiohttp.ClientError, TimeoutError, RuntimeError):
            await interaction.followup.send(
                "YouTube lookup failed. Check the API key, quota, and Railway logs.", ephemeral=True
            )
            return
        if not items:
            await interaction.followup.send("YouTube couldn't find that channel.", ephemeral=True)
            return
        item = items[0]
        with monitor.db:
            count = monitor.sql("SELECT count(*) FROM channels").fetchone()[0]
            if monitor.sql("SELECT 1 FROM channels WHERE id=?", item["id"]).fetchone():
                message = "That channel is already tracked."
            elif count >= 40:
                message = "The watchlist is full (40 channels). Remove one first."
            else:
                monitor.sql(
                    "INSERT INTO channels(id, name, added) VALUES (?, ?, ?)",
                    item["id"], item["snippet"]["title"], time.time(),
                )
                message = f"Tracking **{discord.utils.escape_markdown(item['snippet']['title'])}** ({count + 1}/40)."
        await interaction.followup.send(message, ephemeral=True, allowed_mentions=discord.AllowedMentions.none())

    @youtube.command(description="Stop following a YouTube channel")
    @app_commands.describe(channel="Channel ID from /youtube list")
    async def remove(interaction: discord.Interaction, channel: str):
        with monitor.db:
            removed = monitor.sql("DELETE FROM channels WHERE id=?", channel.strip()).rowcount
        await interaction.response.send_message(
            "Channel removed." if removed else "Channel ID wasn't found. Copy it from /youtube list.",
            ephemeral=True,
        )

    @youtube.command(name="list", description="Show the tracked YouTube channels")
    async def list_channels(interaction: discord.Interaction):
        rows = monitor.sql("SELECT id, name FROM channels ORDER BY name COLLATE NOCASE").fetchall()
        await interaction.response.send_message(f"Tracking **{len(rows)}/40** channels.", ephemeral=True)
        for start in range(0, len(rows), 10):
            lines = [
                f"{discord.utils.escape_markdown(row['name'][:85])} · `{row['id']}`"
                for row in rows[start:start + 10]
            ]
            await interaction.followup.send(
                "\n".join(lines), ephemeral=True, allowed_mentions=discord.AllowedMentions.none()
            )

    @youtube.command(description="Show monitoring and delivery health")
    async def status(interaction: discord.Interaction):
        now = time.time()
        total, push, feeds = monitor.sql(
            "SELECT count(*), count(CASE WHEN lease>? THEN 1 END), "
            "count(CASE WHEN feed_ok>? THEN 1 END) FROM channels", now, now - 180
        ).fetchone()
        pending = monitor.sql("SELECT count(*) FROM outbox WHERE sent IS NULL").fetchone()[0]
        recent = monitor.sql("""
            SELECT sent-event_time AS delay FROM outbox WHERE sent IS NOT NULL ORDER BY sent DESC LIMIT 20
        """).fetchall()
        within_target = sum(row["delay"] <= 120 for row in recent)
        destination = monitor.destination()
        timing = f"{within_target}/{len(recent)} within 2 minutes" if recent else "No alerts sent yet"
        api_status = monitor.api_error or ("OK" if monitor.last_api_ok else "Waiting for first lookup")
        await interaction.response.send_message(
            f"Channels: **{total}/40**\nPush subscriptions: **{push}/{total}**\n"
            f"Feeds reached in the last 3 min: **{feeds}/{total}**\n"
            f"Alert channel: {f'<#{destination}>' if destination else 'Unset'}\n"
            f"Queued alerts: **{pending}**\nYouTube API: {api_status}\n"
            f"Last {len(recent)} deliveries: {timing}",
            ephemeral=True, allowed_mentions=discord.AllowedMentions.none(),
        )

    @youtube.command(description="Send a test message to the configured alert channel")
    async def test(interaction: discord.Interaction):
        destination = monitor.destination()
        if destination is None:
            await interaction.response.send_message("Run /youtube set-alert-channel first.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            channel = client.get_channel(destination) or await client.fetch_channel(destination)
            await channel.send(
                embed=discord.Embed(title="YouTube Alerts test", description="Discord delivery is working."),
                allowed_mentions=discord.AllowedMentions.none(),
            )
        except discord.HTTPException:
            await interaction.followup.send("Delivery failed. Check the bot's channel permissions.", ephemeral=True)
            return
        await interaction.followup.send("Test alert sent.", ephemeral=True)

    commands.add_command(youtube)
    return client


def required(key):
    value = os.environ.get(key, "").strip()
    if not value:
        raise SystemExit(f"Set {key} before starting the bot.")
    return value


async def main():
    public_url = os.environ.get("PUBLIC_BASE_URL") or (
        "https://" + os.environ["RAILWAY_PUBLIC_DOMAIN"] if os.environ.get("RAILWAY_PUBLIC_DOMAIN") else ""
    )
    address = urlsplit(public_url)
    if address.scheme != "https" or not address.hostname or address.path not in ("", "/") or address.query or address.fragment:
        raise SystemExit("Generate a Railway public domain or set PUBLIC_BASE_URL to an HTTPS origin.")
    secret = required("PUSH_SECRET")
    if not re.fullmatch(r"[A-Za-z0-9_-]{32,100}", secret):
        raise SystemExit("PUSH_SECRET must be 32–100 URL-safe characters.")
    token = required("DISCORD_TOKEN")
    guild_id = int(required("DISCORD_GUILD_ID"))
    api_key = required("YOUTUBE_API_KEY")
    database = os.environ.get("DATABASE_PATH", "/data/youtube-alerts.sqlite3")
    timeout = aiohttp.ClientTimeout(total=12)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        monitor = Monitor(database, session, api_key, public_url, secret)
        client = make_client(monitor, guild_id)
        app = web.Application(client_max_size=256 * 1024)
        async def health(_request):
            return web.json_response({"ok": True})

        app.router.add_get("/health", health)
        app.router.add_get(monitor.callback_path, monitor.callback, allow_head=False)
        app.router.add_post(monitor.callback_path, monitor.callback)
        runner = web.AppRunner(app, access_log=None)
        try:
            await runner.setup()
            await web.TCPSite(runner, "0.0.0.0", int(os.environ.get("PORT", "8080"))).start()
            async with client, asyncio.TaskGroup() as tasks:
                tasks.create_task(client.start(token))
                tasks.create_task(monitor.discover())
                tasks.create_task(monitor.enrich())
                tasks.create_task(monitor.deliver(client))
        finally:
            await runner.cleanup()
            monitor.db.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
