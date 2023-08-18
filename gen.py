from dotenv import load_dotenv
import os
import sqlite3
import logging
from jinja2 import Environment, FileSystemLoader

load_dotenv()
SQLITE_DB = os.getenv('SQLITE_DB', 'discord.sqlite')
connection = sqlite3.connect(SQLITE_DB)
cursor = connection.cursor()

DATE_FMT = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format='%(levelname)s %(message)s',
                    datefmt=DATE_FMT, level=logging.INFO)

cursor.execute("""
CREATE TABLE IF NOT EXISTS guilds (
    id INTEGER NOT NULL,
    name TEXT
)""")

environment = Environment(loader=FileSystemLoader("templates/"))
guild_template = environment.get_template("guild.html")

guilds = cursor.execute("SELECT * FROM guilds").fetchall()
for guild in guilds:
    guild_dir = f"output/{guild[0]}"
    os.makedirs(guild_dir, exist_ok=True)

    logging.info("Generating guild '{}'".format(guild[1]))
    guild_members = cursor.execute("SELECT * FROM members WHERE guild_id = {}".format(guild[0])).fetchall()
    channels = cursor.execute("SELECT * FROM channels WHERE guild_id = {}".format(guild[0])).fetchall()
    logging.info("Got {} members and {} channels".format(len(guild_members), len(channels)))
    for channel in channels:
        messages = cursor.execute("SELECT * FROM messages WHERE channel_id = {}".format(channel[0])).fetchall()
        logging.info("Generating channel '{}' ({} messages)".format(channel[1], len(messages)))
        print(messages)
        

