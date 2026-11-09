# YouTube Alerts for Discord

A single-server bot with an empty watchlist. Add up to 40 public YouTube channels from Discord. Uploads and Shorts go to the configured text channel. Upcoming livestreams are tracked when discovered, and an alert is sent when YouTube reports that the stream actually started.

## Deploy on Railway

1. Extract this ZIP. Put the **contents** of the `youtube-alerts` folder in a private GitHub repository, with `Dockerfile` at the repository root.
2. On Railway, create a project from that GitHub repository. Use one replica and leave Serverless sleeping disabled. If the first build starts before the service is configured, finish the next steps and redeploy.
3. Add a persistent **Volume** to the bot service and mount it at **`/data`**. Keep this volume attached across deploys. It stores the watchlist, recent video IDs and the delivery queue.
4. In the service's **Settings → Networking → Public Networking**, generate a public domain. Set target port **8080** if prompted. Railway's public HTTPS URL receives YouTube's push notifications.
5. Add these service variables in **Variables**.

   | Variable | Value |
   | --- | --- |
   | `DISCORD_TOKEN` | Bot token from the Discord Developer Portal |
   | `DISCORD_GUILD_ID` | Discord server ID. Enable Developer Mode in Discord's Advanced settings, then right-click the server icon and choose Copy Server ID |
   | `YOUTUBE_API_KEY` | Private replacement API key restricted to YouTube Data API v3 |
   | `PUSH_SECRET` | A private random URL-safe string, 32–100 characters. Generate one with `python -c "import secrets; print(secrets.token_urlsafe(32))"` |
   | `PORT` | `8080` |

   Railway supplies `RAILWAY_PUBLIC_DOMAIN` once a public domain is assigned. For another host or a custom callback domain, set `PUBLIC_BASE_URL` to the full HTTPS origin. Keep the bot token, YouTube API key, and push secret in Railway's Variables.

6. Deploy. In Discord, ensure the bot was installed with the `bot` and `applications.commands` scopes. In your alert channel, grant it **View Channel**, **Send Messages**, and **Embed Links**. Leave privileged intents off.
7. Run the commands below in the server. You need **Manage Server** permission.

   | Command | Purpose |
   | --- | --- |
   | `/youtube set-alert-channel channel:#youtube-alerts` | Set the destination |
   | `/youtube test` | Send a test alert |
   | `/youtube add channel:https://www.youtube.com/@YouTube` | Follow a channel by URL, handle, or UC channel ID |
   | `/youtube list` | See followed channels and their IDs |
   | `/youtube remove channel:UC…` | Remove a channel and its queued alerts |
   | `/youtube status` | Check push subscriptions, backup feeds, delivery backlog, and recent delivery timing |

## Alert behavior and costs

- The bot posts each new item individually with no role pings. Livestream start alerts take priority over uploads waiting in the queue. Alerts survive restarts. A crash immediately after Discord accepts a message but before SQLite records it can cause a duplicate.
- Push notifications are supplemented by roughly one-minute public feed checks. Video lookups are batched at most once every 20 seconds. An existing video is suppressed based on its publication time or actual livestream start time. Discord timing is measured from the timestamp YouTube reports.
- Shorts appear with the other uploads. A Short gets a dedicated label only when the source identifies it with a Shorts URL. Detection still works for Shorts presented as standard watch URLs.
- The two-minute target depends on when YouTube exposes each event and whether Discord is accepting messages. A livestream must first be discoverable through YouTube's push or public feed. A long outage or large burst can exceed the feed's available history. The status command measures delivery times for alerts the bot saw.
- No separate database subscription is needed. Railway Hobby currently has a $5 monthly minimum including $5 of usage. Compute, network, and volume usage count toward the total. See https://railway.com/pricing for current rates. The SQLite database stores metadata only. The bot uses the standard YouTube Data API quota and backs off when quota errors occur.

## Troubleshooting

- **No slash commands** — check the server ID, install scopes, and Railway logs. Commands are registered specifically to the configured server.
- **No push subscription** — confirm the public domain uses HTTPS, targets port 8080, and `/health` responds. Then check logs and `/youtube status`. Backup feed checks operate while push subscriptions retry.
- **Queued alerts** — run `/youtube test` and verify that the destination is set and the bot can send embeds there.
- **API lookup error** — confirm YouTube Data API v3 is enabled in the key's Google Cloud project and the restriction allows that API.
