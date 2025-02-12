from datetime import datetime
import asyncio
import logging
from math import floor, log10
from itertools import islice

import orjson
from quart import Quart, request
from flask_orjson import OrjsonProvider
from quart_cors import cors

from .dex_data import (CANDLE_STORES, GENESIS, transactions_by_pair, transactions_by_account, get_all_pairs, get_pair_stats, get_pricer, check_indexes, get_candle_stat, get_candle_stat_kda,
                       get_Xly_volume_report,get_Xly_tvl_report, get_udf_history, cache_info, init_mongo, last_swap)
from .exchange_info import DEFAULT_EXCHANGE
from .kda_utils import to_pair
from .version import VERSION
from .tokens import tokens_db

from .date_utils import * #pylint: disable=wildcard-import, unused-wildcard-import


logger = logging.getLogger(__name__)

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s:%(levelname)6s:%(name)18s => %(message)s",
            "datefmt": "[%Y-%m-%d %H:%M:%S %z]"
        },
    },
    "handlers": {
        "default": {
            "formatter": "standard",
            "class": "logging.StreamHandler",
        },
    },
    "loggers": {
        "dex_api.api": {
            "handlers": ["default"],
            "level": "INFO",
            "propagate": False
        },
        "dex_api.dex_data": {
            "handlers": ["default"],
            "level": "INFO",
            "propagate": False
        },
        "dex_api.tokens": {
            "handlers": ["default"],
            "level": "INFO",
            "propagate": False
        },
        "dex_api.kda_price": {
            "handlers": ["default"],
            "level": "INFO",
            "propagate": False
        },
    }
}

logging.config.dictConfig(LOGGING_CONFIG)


class DexApiOrjsonProvider(OrjsonProvider):
    option = orjson.OPT_SERIALIZE_NUMPY | orjson.OPT_UTC_Z

LONG_CACHE_CONTROL = {"Cache-Control":"max-age=8640000"}
ONE_DAY_CACHE_CONTROL = {"Cache-Control":"max-age=86400"}
FIVE_MIN_CACHE_CONTROL =  {"Cache-Control":"max-age=300"}
DEFAULT_LIMIT = "10"


app = Quart(__name__, static_folder=None)
app.json = DexApiOrjsonProvider(app)
app = cors(app, allow_origin="*")

status = "STARTING"

async def init_caches():
    global status #pylint: disable=global-statement

    _now = start_of_day(datetime.utcnow())

    async def __init_candles():
        total_days = (_now - start_of_day(GENESIS)).days

        for offset in range(0, total_days,20):
            for p in await get_all_pairs():
                store = CANDLE_STORES[p]
                _stop =  _now - offset*ONE_DAY
                if _stop > store.ref_date_1day:
                    _start = max(_stop - 20*ONE_DAY, store.ref_date_1day)
                    await store.get_candles_1day(_start,  _stop, in_usd=False)
                    await asyncio.sleep(0.5)
                    await store.get_candles_1day(_start, _stop , in_usd=True)
                    await asyncio.sleep(0.5)
        logger.info("Pair candles cached")

    async def __init_reports_daily():
        for c_date in day_range_reverse(_now - ONE_YEAR, _now):
            await get_Xly_volume_report(c_date, "DAILY")
            await get_Xly_tvl_report(c_date, "DAILY")
        logger.info("Daily Volume/TVL reports cached")

    async def __init_reports_monthly():
        for c_date in month_range_reverse(_now - ONE_YEAR, _now):
            await get_Xly_volume_report(c_date, "MONTHLY")
        logger.info("Monthly volume reports cached")

    async def __init_reports_weekly():
        for c_date in week_range_reverse(_now - ONE_YEAR, _now,):
            await get_Xly_volume_report(c_date, "WEEKLY")
        logger.info("Weekly volume reports cached")


    await asyncio.sleep(1.0)
    logger.info("Start to fill initial cache")
    if status == "CACHING":
        logger.warning("Previous caching not finished")
        return
    status = "CACHING"
    await asyncio.gather(__init_candles(), __init_reports_daily(), __init_reports_monthly(), __init_reports_weekly())
    status = "RUNNING"


async def maintenance_task():
    await asyncio.sleep(120.0)
    while True:
        try:
            await get_pricer().update_usd_data()
        except asyncio.CancelledError:
            logger.info("Leaving maintance task")
            return
        except: #pylint: disable=bare-except
            logger.exception("Error during price update")

        # Keep cache fresh:
        try:
            for p in await get_all_pairs():
                await asyncio.sleep(5.0)
                await get_pair_stats(p)
        except asyncio.CancelledError:
            logger.info("Leaving maintance task")
            return
        except: #pylint: disable=bare-except
            logger.exception("Error during pair cache refresh")
            await asyncio.sleep(180.0)

async def tokens_update_task():
    while True:
        try:
            await asyncio.sleep(60.0 * 30)
            updated = await tokens_db.update()
            if updated:
                for p in await get_all_pairs():
                    get_pair_stats.cache_invalidate(p)
        except asyncio.CancelledError:
            logger.info("Leaving maintance task")
            return
        except: #pylint: disable=bare-except
            logger.exception("Error during token DB update")

async def daily_clean_task():
    while True:
        try:
            await asyncio.sleep(60.0 * 60 * 24)
            # We invalidate daily one full week of candles.. To be sure we recover in case something bad happened
            for store in CANDLE_STORES.values():
                store.invalidate(from_now(-SEVEN_DAYS), from_now(ONE_HOUR))
        except asyncio.CancelledError:
            logger.info("Leaving daily cleaning task")
            return
        except: #pylint: disable=bare-except
            logger.exception("Error during daily cleaning")

async def monthly_clean_task():
    while True:
        try:
            await asyncio.sleep(60.0 * 60 * 24 * 61)
            # We reinit the store each two months to be sure that
            for store in CANDLE_STORES.values():
                await store.init()
            await init_caches()
        except asyncio.CancelledError:
            logger.info("Leaving monthly cleaning task")
            return
        except: #pylint: disable=bare-except
            logger.exception("Error during monthly cleaning")



@app.before_serving
async def init_api():
    await init_mongo()
    await tokens_db.update()
    await get_pricer().init()
    await check_indexes()
    await get_all_pairs()
    app.add_background_task(init_caches)
    app.add_background_task(maintenance_task)
    app.add_background_task(tokens_update_task)
    app.add_background_task(daily_clean_task)
    app.add_background_task(monthly_clean_task)

def not_found_error(x):
    return {"error":"NotFound", "msg":x}, 404

def invalid_parameter_error(x):
    return {"error":"InvalidParameter", "msg":x}, 400

@app.route("/mempool/get-gas-data")
async def gas_data():

    return {"timestamp":datetime.utcnow().isoformat(),
            "lowestGasPrice":1e-8,
            "highestGasPrice":1.3e-8,
            "suggestedGasPrice":1.2e-8,
            "networkCongested":False}, LONG_CACHE_CONTROL


@app.route("/api/pairs")
async def get_pairs():
    pair_id = request.args.get("id")
    if pair_id:
        _, symbol = pair_id.split(":")
        token_mod = tokens_db.token_mod_from_symbol(symbol)

        if not token_mod:
            logger.info("Unknown symbol requested : {}".format(symbol))
            return not_found_error("Unknown symbol")

        stats = await get_pair_stats(to_pair("coin", token_mod)), FIVE_MIN_CACHE_CONTROL
        if not stats:
            logger.info("Pair stats returns None: {}".format(symbol))
            return not_found_error("Pair not indexed")
        return stats

    return [await get_pair_stats(p) for p in await get_all_pairs()], FIVE_MIN_CACHE_CONTROL

@app.route("/api/transactions")
async def get_transactions():
    pair_id = request.args.get("id")
    account = request.args.get("account")
    limit = int(request.args.get("limit", DEFAULT_LIMIT))
    from_time = request.args.get("fromTime")
    to_time = request.args.get("toTime")
    ts_start = datetime.utcfromtimestamp(float(from_time)) if from_time else None
    ts_stop = datetime.utcfromtimestamp(float(to_time)) if to_time else None

    if ts_stop:
        ts_stop = min(ts_stop, datetime.utcnow() + ONE_HOUR)

    if ts_start:
        ts_start = min(ts_start, datetime.utcnow() + ONE_HOUR)

    if ts_start and ts_stop and not ts_start<=ts_stop:
        logger.warning("Invalid time")
        return invalid_parameter_error("Invalid time")

    if limit > 100 or limit < 1:
        logger.warning("Someone is requesting a huge amount of transactions : {:d}".format(limit))
        return invalid_parameter_error("Limit too high")

    if account and pair_id:
        logger.warning("Both account and pair_id in transaction request")
        return invalid_parameter_error("Invalid query")

    if not account and not pair_id:
        logger.warning("Neither account and pair_id in transaction request")
        return invalid_parameter_error("Invalid query")

    if pair_id:
        _, symbol = pair_id.split(":")

        token_mod = tokens_db.token_mod_from_symbol(symbol)

        if token_mod is None:
            logger.info("Unknown symbol requested : {}".format(symbol))
            return not_found_error("Token not supported")

        return await transactions_by_pair(to_pair("coin", token_mod), ts_start,ts_stop, limit)

    if account:
        if len(account) > 256 or len(account) < 4:
            logger.warning("Account length insane")
            return invalid_parameter_error("Invalid query")

        return await transactions_by_account(account, ts_start, ts_stop, limit)

    return []

def args_dates():
    dateStart = datetime.fromisoformat(request.args.get("dateStart").strip())
    dateEnd = datetime.fromisoformat(request.args.get("dateEnd").strip())
    return max(dateStart, GENESIS), min(dateEnd, datetime.utcnow() + ONE_DAY)


@app.route("/api/candles")
@app.route("/candles")
async def get_candles():
    dateStart, dateEnd = args_dates()
    asset = request.args.get("asset")

    dateStart = max(dateStart, dateEnd-ONE_YEAR)

    if asset == "KDA":
        return await get_candle_stat_kda(dateStart, dateEnd)
    candles = await get_candle_stat(asset, dateStart, dateEnd)
    if not candles:
        logger.info("Candle returns returns None: {}".format(asset))
        return not_found_error("Pair not indexed")
    return candles


@app.route("/volume/daily")
async def get_daily():
    dateStart, dateEnd = args_dates()
    return [await get_Xly_volume_report(c_date, "DAILY") for c_date in day_range_limit(dateStart, dateEnd)], FIVE_MIN_CACHE_CONTROL

@app.route("/volume/weekly")
async def get_weekly():
    dateStart, dateEnd = args_dates()
    return [await get_Xly_volume_report(c_date, "WEEKLY") for c_date in week_range_limit(dateStart, dateEnd)], FIVE_MIN_CACHE_CONTROL

@app.route("/volume/monthly")
async def get_monthly():
    dateStart, dateEnd = args_dates()
    return [await get_Xly_volume_report(c_date, "MONTHLY") for c_date in month_range_limit(dateStart, dateEnd)], FIVE_MIN_CACHE_CONTROL

@app.route("/tvl/daily")
async def get_tvl_daily():
    dateStart, dateEnd = args_dates()
    return [await get_Xly_tvl_report(c_date, "DAILY") for c_date in day_range_limit(dateStart, dateEnd)], FIVE_MIN_CACHE_CONTROL

@app.route("/tvl/weekly")
async def get_tvl_weekly():
    dateStart, dateEnd = args_dates()
    return [await get_Xly_tvl_report(c_date, "WEEKLY") for c_date in week_range_limit(dateStart, dateEnd)], FIVE_MIN_CACHE_CONTROL

@app.route("/tvl/monthly")
async def get_tvl_monthly():
    dateStart, dateEnd = args_dates()
    return [await get_Xly_tvl_report(c_date, "MONTHLY") for c_date in month_range_limit(dateStart, dateEnd)], FIVE_MIN_CACHE_CONTROL


@app.route("/analytics/get-data")
async def get_analytics_data():
    return [{}], LONG_CACHE_CONTROL



### UDF API
@app.route("/config")
@app.route("/udf/config")
async def udf_get_config():
    return {"supported_resolutions":["5","15","30","60","120","240","1D","1W"],
            "supports_group_request":False,
            "supports_marks":False,
            "supports_search":True,
            "supports_timescale_marks":False,
            "symbol_types":[{"name":"Crypto","value":"crypto"}],
             "exchanges":[{"value":DEFAULT_EXCHANGE.symbol,"name":DEFAULT_EXCHANGE.name,"description":DEFAULT_EXCHANGE.description}]}, ONE_DAY_CACHE_CONTROL




async def get_udf_symbol_info(token_mod, currency):
    stats = await get_pair_stats(to_pair("coin", token_mod))
    price = stats["price"] if currency == "USD" else stats["priceKda"]
    price_log = floor(log10(price))
    scale = max(1000, 10**(3 - price_log))
    udf_symbol = ":".join((tokens_db.token_symbol(token_mod), currency, DEFAULT_EXCHANGE.symbol))

    return {"symbol": udf_symbol,
            "description":tokens_db.token_info(token_mod)["name"],
            "ticker":  udf_symbol,
            "pricescale": scale,
            "type":"crypto",
            "has-no-volume":False,
            "exchange-listed":DEFAULT_EXCHANGE.name,
            "exchange-traded":DEFAULT_EXCHANGE.name,
            "minmovement":1,
            "has-dwm":True,
            "has-intraday":True,
            "timezone":"Etc/UTC",
            "supported_resolutions":["5","15","30","60","120","240","1D","1W"],
            "has_intraday":True,
            "intraday_multipliers":["5", "60"],
            "session-regular":"24x7"}


@app.route("/symbols")
@app.route("/udf/symbols")
async def udf_get_symbols():
    symbol = request.args.get("symbol").strip()
    sym1, sym2, exchange = symbol.split(":")

    if exchange not in ("KADDEX", DEFAULT_EXCHANGE.symbol):
        logger.info("Unknown exchange : {}".format(exchange))
        return not_found_error("Exchange not supported")

    if sym2 not in ("USD", "KDA"):
        logger.info("Unknown currency : {}".format(sym2))
        return not_found_error("Unsupported pair")

    token_mod = tokens_db.token_mod_from_symbol(sym1)
    if token_mod is None:
        logger.info("Unknown symbol requested : {}".format(symbol))
        return not_found_error("Token not supported")

    udf_symbol_info = await get_udf_symbol_info(token_mod, sym2)

    if udf_symbol_info is None:
        logger.info("Unknown symbol requested : {}".format(symbol))
        return not_found_error("Token not supported")

    return udf_symbol_info, ONE_DAY_CACHE_CONTROL


@app.route("/search")
@app.route("/udf/search")
async def udf_search():
    query = request.args.get("query").strip()
    exchange = request.args.get("exchange").strip()
    _limit = int(request.args.get("limit", DEFAULT_LIMIT).strip())
    data_query = query.split(":", 2)[0]

    if exchange not in ("KADDEX", DEFAULT_EXCHANGE.symbol):
        logger.info("Unknown exchange : {}".format(exchange))
        return not_found_error("Exchange not supported")

    def __gen_search_result(token_mod, currency):
        return {"symbol":":".join((tokens_db.token_symbol(token_mod), currency)),
                "description": tokens_db.token_info(token_mod)["description"],
                "exchange": DEFAULT_EXCHANGE.symbol,
                "ticker": ":".join((tokens_db.token_symbol(token_mod), currency, DEFAULT_EXCHANGE.symbol)),
                "type":"crypto"}

    results = []
    for token_mod in islice( filter(lambda x: x!="coin", tokens_db.token_mod_search(data_query)), _limit//2):
        results.append(__gen_search_result(token_mod,"USD"))
        results.append(__gen_search_result(token_mod,"KDA"))
    return results


@app.route("/history")
@app.route("/udf/history")
async def udf_get_history():
    symbol = request.args.get("symbol").strip()
    _from = datetime.utcfromtimestamp(int(request.args.get("from").strip()))
    _to = datetime.utcfromtimestamp(int(request.args.get("to").strip()))
    _countback = request.args.get("countback")
    countback = int(_countback) if _countback else None
    resolution = request.args.get("resolution").strip()

    sym1, sym2, exchange = symbol.split(":")

    if exchange not in ("KADDEX", DEFAULT_EXCHANGE.symbol):
        return not_found_error("Exchange not supported")

    token_mod = tokens_db.token_mod_from_symbol(sym1)

    if token_mod is None:
        return not_found_error("Token not supported")

    if sym2 not in ("USD", "KDA"):
        return not_found_error("Unsupported pair")

    if not token_mod:
        return not_found_error("Pair not found")

    if resolution not in ("5", "60", "1D", "1W"):
        return invalid_parameter_error("Resolution not supported")

    return await get_udf_history(to_pair("coin", token_mod), _from, _to, countback, resolution, in_usd=sym2=="USD")

@app.route("/kda_price")
async def get_kda_price():
    _pricer = get_pricer()
    return {"price": float(_pricer.at(datetime.utcnow())), "source":_pricer.SOURCE}

@app.route("/info")
async def get_info():
    return {"name": "dex_api", "status":status,
            "version":VERSION, "cache":cache_info(), "exchange":DEFAULT_EXCHANGE._asdict(), "tokens_db":tokens_db.get_info(),
            "pricer":get_pricer().get_info(), "last_swap":last_swap()}, {"Refresh": 30}

@app.route("/")
async def root():
    edp = [str(x) for x in app.url_map.iter_rules()]
    return {"name": "dex_api", "status":status, "version":VERSION, "endpoints":edp}

@app.errorhandler(404)
async def page_not_found(error):
    return not_found_error(str(error))

@app.errorhandler(Exception)
async def special_exception_handler(error):
    return {"error":"Unexpected error", "msg":str(error)}, 500
