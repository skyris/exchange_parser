import asyncio
from collections import namedtuple
import ccxt.async_support as ccxt
from ccxt.base import errors
import colorama


loop = asyncio.get_event_loop()
Response = namedtuple('Response', ['exchange', 'pair', 'ask', 'bid'])


async def main(exchange_names, pairs, delay):
    data = asyncio.Queue()
    while True:
        print('<Fetching data>')
        await fetch_all_at_once(exchange_names, pairs, data)
        await process_data(data)
        await asyncio.sleep(delay)


async def fetch_all_at_once(exchange_names, pairs, data):
    tasks = []
    for exchange_name in exchange_names:
        for pair in pairs:
            tasks.append(
                loop.create_task(fetch(exchange_name, pair, data)))
    final_task = asyncio.gather(*tasks)
    await final_task


async def fetch(exchange_name, pair, data):
    try:
        exchange = getattr(ccxt, exchange_name)({})
        ticker = await exchange.fetch_ticker(pair)
        response_tuple = Response(exchange_name, pair, ticker['ask'], ticker['bid'])
        await data.put(response_tuple)

    except errors.ExchangeNotAvailable:
        print('Exchange is not available: {}'.format(exchange))

    except errors.RequestTimeout:
        print('Request timeout: {} {}'.format(exchange, pair))

    except errors.BadSymbol:
        print('Bad symbol {} at {}'.format(pair, exchange))

    finally:
        await exchange.close()


async def process_data(data):
    result_list = await get_list_from_queue(data)
    print_sorted(result_list, operation='ask') 
    print_sorted(result_list, operation='bid') 


async def get_list_from_queue(data):
    queue_size = data.qsize()
    lst = []
    for _ in range(queue_size):
        item = await data.get()
        lst.append(item)

    return lst


def print_sorted(lst, operation):
    existing_pairs = { line.pair for line in lst }
    print(colorama.Fore.WHITE + 
        "{} {} {}".format("-" * 30, operation.upper(), "-" * 30), flush=True)
    for pair in existing_pairs:
        print(colorama.Fore.CYAN +
            "{}: {}".format(operation.upper(), pair), flush=True)
        sorted_lines = sorted(
            filter(lambda el: el.pair == pair, lst),
            key=lambda el: getattr(el, operation))

        for line in sorted_lines:
            exchange, value = line.exchange, getattr(line, operation)
            print(colorama.Fore.YELLOW + 
                " {:<14} {}".format(exchange, value), flush=True)

    print(colorama.Fore.WHITE, flush=True)



if __name__ == '__main__':
    DELAY = 60 * 2  # 2 minutes
    EXCHANGE_NAMES = ('exmo', 'bibox', 'poloniex', 'kraken')
    PAIRS = ('ETH/BTC', 'ETH/USDT', 'BTC/USDT', 'ADA/BTC')

    main_task = loop.create_task(main(EXCHANGE_NAMES, PAIRS, DELAY))

    try:
        loop.run_until_complete(main_task)
    except KeyboardInterrupt:
        print('Got signal: SIGINT, shutting down.')

    tasks = asyncio.all_tasks(loop=loop)
    for t in tasks:
        t.cancel()

    group = asyncio.gather(*tasks, return_exceptions=True)
    loop.run_until_complete(group)
    loop.close()
