import asyncio

from scrapers.FFG import FFG


async def run():
    ffg = FFG({})
    events = []
    async for result in ffg.get_results():
        events.append(result)


def main():
    asyncio.run(run())


if __name__ == "__main__":
    main()
