import asyncio
import logging
import sys

from bot import BotApp


async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    logging.info("Starting...")
    bot = BotApp()
    try:
        await bot.run()
    except KeyboardInterrupt:
        logging.info("Stopping...")
    finally:
        await bot.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass