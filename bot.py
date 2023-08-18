import asyncio
import discord
import logging
import os
import sqlite3
import urllib.request
import datetime
from dotenv import load_dotenv
from enum import IntEnum

load_dotenv()

DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
SQLITE_DB = os.getenv('SQLITE_DB', 'discord.sqlite')
CONCURRENCY = int(os.getenv('CONCURRENCY', 6))
DATE_FMT = '%Y-%m-%d %H:%M:%S'
AFTER = os.getenv('AFTER', '2020-01-01 00:00:00')
INTERVAL_SIZE = int(os.getenv('INTERVAL_SIZE', 60))

logging.basicConfig(format='%(levelname)s %(message)s',
                    datefmt=DATE_FMT, level=logging.INFO)

connection = sqlite3.connect(SQLITE_DB)
cursor = connection.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS guilds (
    id INTEGER NOT NULL,
    name TEXT,
    PRIMARY KEY (id)
)""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS members (
    id INTEGER NOT NULL,
    guild_id INTEGER NOT NULL,
    joined_at INTEGER NOT NULL,
    name TEXT,
    discriminator INTEGER,
    nick TEXT,
    PRIMARY KEY (id, guild_id),
    FOREIGN KEY (guild_id) REFERENCES guilds(id)
)""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS channels (
    id INTEGER NOT NULL,
    name TEXT,
    guild_id INTEGER,
    PRIMARY KEY (id),
    FOREIGN KEY (guild_id) REFERENCES guilds(id)
)""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER NOT NULL,
    channel_id INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    author_id INTEGER NOT NULL,
    user_name TEXT,
    content TEXT,
    PRIMARY KEY (id),
    FOREIGN KEY (channel_id) REFERENCES channels(id)
)""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS attachments (
    message_id INTEGER NOT NULL,
    filename TEXT,
    content_type TEXT,
    data BLOB,
    FOREIGN KEY (message_id) REFERENCES messages(id)
)""")

# Embed type: 0: other, 1: image (downloaded), 2: video (downloaded)
class EmbedType(IntEnum):
    Other = 0
    Image = 1
    Video = 2

cursor.execute("""
CREATE TABLE IF NOT EXISTS embeds (
    message_id INTEGER NOT NULL,
    type INTEGER NOT NULL,
    title TEXT,
    description TEXT,
    url TEXT,
    data BLOB,
    FOREIGN KEY (message_id) REFERENCES messages(id),
    CHECK (type IN (0, 1, 2))
)""")

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
client = discord.Client(intents=intents)

async def download_attachment(msg, attachment):
    logging.debug('Download attachment: {}'.format(attachment.url))
    try:
        attachment_data = await attachment.read()
    except Exception as e:
        logging.error('Failed to download attachment {} : {}'.format(attachment.url, e))
    cursor.execute('INSERT OR REPLACE INTO attachments VALUES (?, ?, ?, ?)', (msg.id, attachment.filename, attachment.content_type, attachment_data))

async def download_file(url):
    logging.info('Download file: {}'.format(url))
    request = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0 (X11; Linux i686; rv:109.0) Gecko/20100101 Firefox/114.0'})
    try:
        with urllib.request.urlopen(request) as response:
            return response.read()
    except Exception as e:
        logging.error('Failed to download file {} : {}'.format(url, e))
        return None

async def archive_channel_interval(channel, interval_start, interval_end):
    async for msg in channel.history(limit=None, after=interval_start, before=interval_end):
        logging.debug('[{}] {}: {}'.format(msg.created_at.strftime(DATE_FMT), msg.author.name, msg.content))
        cursor.execute('INSERT OR REPLACE INTO messages VALUES (?, ?, ?, ?, ?, ?)',
                       (msg.id, msg.channel.id, int(msg.created_at.timestamp()), msg.author.id, msg.author.name, msg.clean_content))
        for attachment in msg.attachments:
            await download_attachment(msg, attachment)
        for embed in msg.embeds:
            if embed.type == 'gifv':
                # the 'gifv' type is a gif as a video, the size is probably very small so we just download it
                embed_type = EmbedType.Video
                embed_data = await download_file(embed.video.url)
            elif embed.type == 'image':
                embed_type = EmbedType.Image
                embed_data = await download_file(embed.url)
            else:
                embed_type = EmbedType.Other
                embed_data = None
            cursor.execute('INSERT OR REPLACE INTO embeds VALUES (?, ?, ?, ?, ?, ?)', (msg.id, int(embed_type), embed.title, embed.description, embed.url, embed_data))
        connection.commit()

async def archive_channel(channel):
    logging.info('Start archiving channel: #{}'.format(channel.name))
    cursor.execute('INSERT OR REPLACE INTO channels VALUES (?, ?, ?)',
                   (channel.id, channel.name, channel.guild.id))
    connection.commit()

    interval_start = channel.created_at
    interval_end = interval_start + datetime.timedelta(days=INTERVAL_SIZE)
    while interval_start < datetime.datetime.now(interval_start.tzinfo):
        logging.info('Archiving interval: {} - {}'.format(interval_start.strftime(DATE_FMT), interval_end.strftime(DATE_FMT)))
        await archive_channel_interval(channel, interval_start, interval_end)
        interval_start = interval_end
        interval_end = interval_start + datetime.timedelta(days=INTERVAL_SIZE)

async def gather_with_concurrency(n, *coros):
    semaphore = asyncio.Semaphore(n)

    async def sem_coro(coro):
        async with semaphore:
            return await coro
    return await asyncio.gather(*[sem_coro(c) for c in coros])

async def archive_guild(guild):
    logging.info('Archiving guild: {}'.format(guild.name))
    cursor.execute('INSERT OR REPLACE INTO guilds VALUES (?, ?)', (guild.id, guild.name))
    logging.info('Updating {} members'.format(guild.member_count))
    async for member in guild.fetch_members(limit=None):
        logging.debug('Member: {}#{} ({})'.format(member.name, member.discriminator, member.nick))
        cursor.execute('INSERT OR REPLACE INTO members VALUES (?, ?, ?, ?, ?, ?)', (member.id, guild.id, int(member.joined_at.timestamp()), member.name, member.discriminator, member.nick))
    connection.commit()

    # Archive all channels in parallel
    logging.info('Archiving channels concurrently ({}x)'.format(CONCURRENCY))
    await gather_with_concurrency(CONCURRENCY, *[archive_channel(text_channel) for text_channel in guild.text_channels])

@client.event
async def on_ready():
    logging.info(
        'Logged in as: {}, starting operation..'.format(client.user.name))
    await client.change_presence(status=discord.Status.invisible)

    for guild in client.guilds:
        await archive_guild(guild)
    logging.info('Operation completed, closing connection..')
    await client.close()
    connection.close()

client.run(DISCORD_TOKEN)
