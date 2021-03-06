import logging
import re
import sys
from datetime import date, datetime
from logging import handlers

import discord
import pandas as pd
from discord.ext.commands import Bot

from resources import code_map

LOGGER = logging.getLogger('ai')

CODE_PATTERN = re.compile(r'^\d{4} :: Code `(\d{3})`.*$', re.DOTALL)


def set_up_logger():
    # Logging setup
    formatter = logging.Formatter(fmt='%(asctime)s.%(msecs)03d :: %(levelname)s :: %(message)s', datefmt='%Y-%m-%d :: %H:%M:%S')

    log_file_handler = handlers.TimedRotatingFileHandler(
        filename='status_code_analyzer.log', encoding='utf-8', backupCount=6, when='D', interval=7)
    log_file_handler.setFormatter(formatter)

    logging.basicConfig(level=logging.WARNING)
    root_logger = logging.getLogger()
    root_logger.addHandler(log_file_handler)

    logger = logging.getLogger('ai')
    logger.setLevel(logging.DEBUG)

bot = Bot(command_prefix='UNUSED', case_insensitive=True, guild_subscriptions=True)

CODE_USAGE = {}
for code in code_map.keys():
    CODE_USAGE[code] = {}

async def read_channel(channel: discord.TextChannel):
    '''
    Iterate over the history of one channel to find code usage.
    '''
    reading = True
    before = datetime.now()
    LOGGER.info(f'beginning to read channel {channel.name}')
    while reading:
        reading = False
        LOGGER.info(f'reading messages before {before}')
        async for message in channel.history(before=before):
            # continue reading if there was at least one message to read
            reading = True
            before = message.created_at

            match = CODE_PATTERN.match(message.content)

            if match and match.group(1) in CODE_USAGE.keys():
                LOGGER.info(f'found usage of code {match.group(1)}')
                add_or_increase_usage(match.group(1), message.created_at.date())

def add_or_increase_usage(code: str, when: date):
    '''
    Creates a new entry in for the code at the given date in the usage map or, if it already exists, increases it by one.
    '''
    if when in CODE_USAGE[code].keys():
        CODE_USAGE[code][when] = CODE_USAGE[code][when] + 1
    else:
        CODE_USAGE[code][when] = 1

def write_file():
    '''
    Export the usage map to a spreadsheet.
    '''
    LOGGER.info('beginning to write output file')
    with pd.ExcelWriter('data/out.ods') as writer:
        frame = pd.DataFrame(data=CODE_USAGE)
        frame.fillna(0, inplace=True)
        frame.to_excel(writer)

@bot.event
async def on_ready():
    LOGGER.info('logged in')
    guild = bot.guilds[0]

    for channel in guild.text_channels:
        await read_channel(channel)

    await bot.close()

def main():
    set_up_logger()
    
    # connect to discord and collect data
    bot.run(sys.argv[1])

    # convert data to dataframes and writen them to a ODS-file
    write_file()
    LOGGER.info('process complete')


if __name__ == "__main__":
    main()
