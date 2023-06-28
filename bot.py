import datetime
import discord
import logging
import os
import sqlite3
import urllib.request
from dotenv import load_dotenv
from enum import IntEnum

load_dotenv()

DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
SQLITE_DB = os.getenv('SQLITE_DB')
DATE_FMT = '%Y-%m-%d %H:%M:%S'

logging.basicConfig(format='%(levelname)s %(message)s',
                    datefmt=DATE_FMT, level=logging.INFO)

connection = sqlite3.connect(SQLITE_DB)
cursor = connection.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS guilds (
    id INTEGER,
    name TEXT
)""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS channels (
    id INTEGER,
    name TEXT,
    guild_id INTEGER,
    PRIMARY KEY (id),
    FOREIGN KEY (guild_id) REFERENCES guilds(id)
)""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER,
    channel_id INTEGER,
    created_at INTEGER,
    author_id INTEGER,
    user_name TEXT,
    content TEXT,
    PRIMARY KEY (id),
    FOREIGN KEY (channel_id) REFERENCES channels(id)
)""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS attachments (
    message_id INTEGER,
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
    message_id INTEGER,
    type INTEGER,
    title TEXT,
    description TEXT,
    url TEXT,
    data BLOB,
    FOREIGN KEY (message_id) REFERENCES messages(id),
    CHECK (type IN (0, 1, 2))
)""")

intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)

async def download_attachment(msg, attachment):
    logging.debug('Download attachment: {}'.format(attachment.url))
    try:
        attachment_data = await attachment.read()
    except Exception as e:
        logging.error('Failed to download attachment {} : {}'.format(attachment.url, e))
    cursor.execute('INSERT OR REPLACE INTO attachments VALUES (?, ?, ?, ?)', (msg.id, attachment.filename, attachment.content_type, attachment_data))

async def download_file(url):
    logging.debug('Download file: {}'.format(url))
    try:
        # TODO: Add fake user agent to avoid 403
        with urllib.request.urlopen(url) as response:
            return response.read()
    except Exception as e:
        logging.error('Failed to download file {} : {}'.format(url, e))
        return None

async def archive_channel(channel):
    now = datetime.datetime.now()

    logging.info('Archiving channel: {}'.format(channel.name))
    cursor.execute('INSERT OR REPLACE INTO channels VALUES (?, ?, ?)',
                   (channel.id, channel.name, channel.guild.id))
    connection.commit()
    async for msg in channel.history(limit=20, before=now, oldest_first=True):
        logging.debug('[{}] {}: {}'.format(msg.created_at.strftime(DATE_FMT), msg.author.name, msg.content))
        cursor.execute('INSERT OR REPLACE INTO messages VALUES (?, ?, ?, ?, ?, ?)',
                       (msg.id, msg.channel.id, int(msg.created_at.timestamp()), msg.author.id, msg.author.name, msg.clean_content))
        for attachment in msg.attachments:
            await download_attachment(msg, attachment)
        for embed in msg.embeds:
            if embed.type == 'gifv':
                # the 'gifv' type is a gif as a video, the size is probably very small so we just download it
                embed_type = EmbedType.Video
                embed_data = await download_file(embed.url)
            elif embed.type == 'image':
                embed_type = EmbedType.Image
                embed_data = await download_file(embed.url)
            else:
                embed_type = EmbedType.Other
                embed_data = None
            cursor.execute('INSERT OR REPLACE INTO embeds VALUES (?, ?, ?, ?, ?, ?)', (msg.id, int(embed_type), embed.title, embed.description, embed.url, embed_data))
        connection.commit()

async def archive_guild(guild):
    logging.info('Archiving guild: {}'.format(guild.name))
    cursor.execute('INSERT OR REPLACE INTO guilds VALUES (?, ?)', (guild.id, guild.name))
    connection.commit()
    for text_channel in guild.text_channels:
        await archive_channel(text_channel)


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
