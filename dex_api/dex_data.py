import asyncio
import logging
import os
from datetime import datetime, UTC
from decimal import Decimal
from functools import reduce
import numpy as np

from motor.motor_asyncio import AsyncIOMotorClient
from bson.decimal128 import Decimal128

from async_lru import alru_cache

from .kda_price import KdaPriceFiveMin
from .exchange_info import DEFAULT_EXCHANGE
from .kda_utils import modref, module_fqn, pair_to_asset, to_pair
from .date_utils import * #pylint: disable=wildcard-import, unused-wildcard-import
from .tokens import tokens_db

GENESIS = datetime.fromisoformat("2022-01-01T00:00:00.0000")
DIRECT_SORT = [("ts",1), ("rank",1)]
REVERSE_SORT = [("ts",-1), ("rank",-1)]
COIN = "coin"
MOD_COIN = modref(COIN)

# KDA/USD Database handle
USD_DATA_DB = None

# Events Datatabse handle
EVENTS_DB = None

# UPDATE collection (from events DB) handle
UPDATE = None

# SWAP collection (from events DB) handle
SWAP = None

logger = logging.getLogger(__name__)
pricer = None


async def init_mongo():
    global pricer, USD_DATA_DB, EVENTS_DB, UPDATE, SWAP #pylint: disable=global-statement

    uri = os.environ.get('MONGO_URI') or "mongodb://localhost:27017"
    usd_data_name = os.environ.get('USD_DATA_DB') or "usd_data"
    kadena_events_name = os.environ.get('EVENTS_DB') or "kadena_events"
    retries = 0
    max_retries = 5

    while retries < max_retries:
        try:
            logger.info("Connecting to MongoDB using URI : {}".format(uri))
            mongo_client = AsyncIOMotorClient(uri)
            await mongo_client.admin.command("ping")
            server_info  = await mongo_client.server_info()
            logger.info("Connected to MongoDB v{!s}".format(server_info["version"]))

            logger.info("USD_Data Database: {}".format(usd_data_name))
            logger.info("Events Database: {}".format(kadena_events_name))

            USD_DATA_DB = mongo_client[usd_data_name]
            EVENTS_DB = mongo_client[kadena_events_name]
            UPDATE = EVENTS_DB["kaddex.exchange.UPDATE"]
            SWAP = EVENTS_DB["kaddex.exchange.SWAP"]
            pricer = KdaPriceFiveMin(USD_DATA_DB)

            return
        except Exception as e: #pylint: disable=broad-exception-caught
            logger.warning("Attempt {:d}/{:d} failed to connect to MongoDB: {!s}".format(retries, max_retries,e))
            retries += 1
            if retries == max_retries:
                logger.error("Failed to connect to MongoDB after multiple attempts")
                raise
            await asyncio.sleep(5.0)

def get_pricer():
    return pricer


async def check_indexes():
    logger.info("Checking and updating indexes")
    idx = await UPDATE.index_information()
    if "dex_api_pair" not in idx:
        logger.info("UPDATE Pair index missing => Create")
        await UPDATE.create_index("params.0", name="dex_api_pair")

    if "dex_api_pair_date" not in idx:
        logger.info("UPDATE Pair_date index missing => Create")
        await UPDATE.create_index({"params.0":1, "ts":1, "rank":1}, name="dex_api_pair_date")

    idx = await SWAP.index_information()
    if "dex_api_src_token" not in idx:
        logger.info("SWAP src_token index missing => Create")
        await SWAP.create_index("params.3.refName", name="dex_api_src_token")

    if "dex_api_dst_token" not in idx:
        logger.info("SWAP dst_token index missing => Create")
        await SWAP.create_index("params.5.refName", name="dex_api_dst_token")

    if "dex_api_pair" not in idx:
        logger.info("SWAP pair index missing => Create")
        await SWAP.create_index({"params.3.refName":1, "params.5.refName":1}, name="dex_api_pair")

    if "dex_api_user_date" not in idx:
        logger.info("SWAP pair_user_date index missing => Create")
        await SWAP.create_index({"params.1":1, "ts":1, "rank":1}, name="dex_api_user_date")

    if "dex_api_pair_date" not in idx:
        logger.info("SWAP pair_date index missing => Create")
        await SWAP.create_index({"params.3.refName":1, "params.5.refName":1, "ts":1, "rank":1}, name="dex_api_pair_date")

def to_kda_price(doc):
    if doc is None:
        return None
    pair, *amounts = doc["params"]
    A, B = map(_dec, amounts) #pylint: disable=invalid-name
    return float(A/B) if pair.startswith("coin:") else float(B/A)

def extract_amounts(doc):
    _, *amounts = doc["params"]
    return map(_float, amounts)

def _dec(x):
    return x.to_decimal() if isinstance(x, Decimal128) else Decimal(x)

def _float(x):
    return float(x.to_decimal()) if isinstance(x, Decimal128) else float(x)

CANDLE_STORES = {}

@alru_cache(ttl=1440.0, maxsize=1)
async def get_all_pairs():
    pairs = UPDATE.aggregate([{"$group": {"_id":"$params.0"}}])
    result =  [x["_id"] async for x in pairs]
    for p in result:
        if p not in CANDLE_STORES:
            listed =  tokens_db.is_token_listed(pair_to_asset(p))
            new_store = CandleStore(p, time_limited=not listed)
            await new_store.init()
            CANDLE_STORES[p] = new_store
    return result


def _aggregate_candles(candles, step, output):
    _len = candles.shape[-1]
    # We always assume the first candle is the incomplete
    start_idx =  _len%step if _len%step else step
    for idx, data in enumerate(np.hsplit(candles, range(start_idx, _len, step))):
        # Open = Take the open of the first candle
        output[0,idx] = data[0,0]
        # Close = Take the close of the last candle
        output[1, idx] = data[1,-1]
        # High => Take the max of all candles
        output[2, idx] = data[2,:].max()
        # Low => Take the min of all candles
        output[3, idx] = data[3,:].min()
        # Volume => Take the sum of all candles
        output[4, idx] = data[4,:].sum()

### Some design notes:
# Each pair is represented by a CandleStore. The store is the main source of cache.
#
# The candles are using Numpy masked matrix called candles_5min, candeles_1hour, and candles_1day
#  https://numpy.org/doc/stable/reference/maskedarray.html
# A masked value means => Not already computed value (because never requested or invalidated and uncached), or undefined (because in the future)
#
# candles_5_min is a 5 x L matrix. Each column represents a 5 min candle.
#   - row 0 = Open value
#   - row 1 = Close value
#   - row 2 = High value
#   - row 3 = Low value
#   - row 4 = Volume in KDA
#
# candles_1hour, and canles_1day are 10 x L matrix. Each colum represents a 1hour/1 day candle.
#   - row 0 = Open value in KDA
#   - row 1 = Close value in KDA
#   - row 2 = High value in KDA
#   - row 3 = Low value in KDA
#   - row 4 = Volume in KDA
#   - row 5 = Open value in USD
#   - row 6 = Close value in USD
#   - row 7 = High value in USD
#   - row 8 = Low value in USD
#   - row 9 = Volume in USD
#
# Each matrix has an associated ref_date variable, which represents the date of the first candle.
#
# Only the candles_5_min is really computed fromt he events database (functions and get_candles_5min _gen_candles_batch).
# The two other matrix are derived from the "5 minutes data".
#
# Note that to balance memory usage and performances, for the USD data only the "1 hour" and "1 day" candles are cached.
# For the 5 minutes data, the LDA -> USD conversion is applied in realtime.

# Calling tree
#
# get_candles_1day()
#     |
#     \-----> get_candles_1hour()
#                  |
#                  \-----> get_candles_5min()
#                              |
#                              |-----> _gen_candles_batch()  -> gen from events DB
#                              \-----> to_usd_price()        -> covert to USD if needed


class CandleStore:
    """ Class representing a storage for all candles related to a pair """
    __slots__ = ("_pair", "ref_date_5min", "ref_date_1hour", "ref_date_1day", "candles_5min", "candles_1hour", "candles_1day", "atl", "ath", "atl_kda", "ath_kda","last_swap", "_time_limited")

    def __init__(self, pair, time_limited=False):
        self._pair = pair
        self.ref_date_5min = None
        self.ref_date_1hour = None
        self.ref_date_1day = None
        self.candles_5min = None
        self.candles_1hour = None
        self.candles_1day = None
        self.last_swap = datetime.min
        self.atl = np.inf
        self.ath = 0.0
        self.atl_kda = np.inf
        self.ath_kda = 0.0

        self._time_limited = time_limited

    def __hash__(self):
        return hash(self._pair)

    def cache_info(self):
        def __matrix_stat(mat):
            return "{:d}/{:d}".format(mat[0,:].count(), len(mat[0,:]))

        return {"5min_candles": __matrix_stat(self.candles_5min),
                "1hour_candles": __matrix_stat(self.candles_1hour),
                "1hour_candles_usd": __matrix_stat(self.candles_1hour[5:,:]),
                "1day_candles": __matrix_stat(self.candles_1day),
                "1day_candles_usd": __matrix_stat(self.candles_1day[5:,:])
               }

    async def init(self):
        first_update = (await UPDATE.find_one({"params.0":self._pair}, sort=DIRECT_SORT)).get("ts")
        # We took a 25 Hours margin to be sure the get_pairs_stats won't try to retrieve unexistant data
        first_update -= 25 * ONE_HOUR
        if self._time_limited:
            first_update = max(first_update, from_now(-SIX_MONTHS))
        self.ref_date_5min = start_of_5_minutes(first_update)
        self.ref_date_1hour = start_of_hour(first_update)
        self.ref_date_1day  = start_of_day(first_update)

        end = from_now(THREE_MONTHS)
        logger.info("===============> INIT PAIR {} {} {} => {}".format(self._pair, self.ref_date_5min, "(TL)" if self._time_limited else "", self.idx_5min(end)))

        self.candles_5min  = np.ma.masked_all( (5, self.idx_5min(end)))
        self.candles_1hour = np.ma.masked_all( (10, self.idx_1hour(end)))
        self.candles_1day  = np.ma.masked_all( (10, self.idx_1day(end)))

    def idx_5min(self, ts):
        return int((ts - self.ref_date_5min).total_seconds())//300

    def idx_1hour(self, ts):
        return int((ts - self.ref_date_1hour).total_seconds())//3600

    def idx_1day(self, ts):
        return int((ts - self.ref_date_1day).total_seconds())//86400

    def invalidate(self, ts_start, ts_stop):
        logger.debug("===============> INVLIDATE {} {} => {}".format(self._pair, ts_start, ts_stop))
        idx_start, idx_stop =  map(self.idx_5min, (ts_start, ts_stop))
        self.candles_5min[:, idx_start:idx_stop].mask = True

        idx_start, idx_stop =  map(self.idx_1hour, (ts_start, ts_stop))
        self.candles_1hour[:, idx_start:idx_stop].mask = True

        idx_start, idx_stop =  map(self.idx_1day, (ts_start, ts_stop))
        self.candles_1day[:, idx_start:idx_stop].mask = True


    async def _gen_candles_batch(self, ts_start, ts_stop, output):
        logger.debug("===============> COMPUTE_CANDLES_BATCH {} {} => {}".format(self._pair, ts_start, ts_stop))
        asset = pair_to_asset(self._pair)
        idx = 0
        current_price = to_kda_price(await UPDATE.find_one({"params.0":self._pair,"ts":{"$lt":ts_start}}, sort=REVERSE_SORT))
        if not current_price:
            current_price = to_kda_price(await UPDATE.find_one({"params.0":self._pair,"ts":{"$gte":ts_start,}}, sort=DIRECT_SORT))
            if not current_price:
                raise ValueError("Unable to retrieve first price of the candle => Should never happen {} {} => {}".format(self._pair, ts_start, ts_stop))

        _open = _min = _max = current_price
        candle_end = ts_start + FIVE_MIN
        idx = 0
        ts = datetime.min
        async for doc in UPDATE.find({"params.0":self._pair, "ts":{"$gte":ts_start, "$lt":ts_stop}}, sort=DIRECT_SORT, batch_size=200):
            ts = doc["ts"]
            while ts >= candle_end:
                output[:, idx] = (_open, current_price, _max, _min, 0.0)
                _open = _min = _max = current_price
                candle_end += FIVE_MIN
                idx += 1
            current_price = to_kda_price(doc)
            _min = min(current_price, _min)
            _max = max(current_price, _max)

        self.last_swap = max(ts, self.last_swap)

        while candle_end <= ts_stop:
            output[:, idx] = (_open, current_price, _max, _min, 0.0)
            _open = _min = _max = current_price
            candle_end += FIVE_MIN
            idx += 1

        ## Volume
        candle_end = ts_start + FIVE_MIN
        idx = 0
        _vol = 0.0
        async for doc in SWAP.find({"$or": [{"params.3.refName":MOD_COIN, "params.5.refName":modref(asset)},
                                            {"params.5.refName":MOD_COIN, "params.3.refName":modref(asset)}],
                                    "ts":{"$gte":ts_start, "$lt":ts_stop}},  sort=DIRECT_SORT, batch_size=200):
            ts = doc["ts"]
            amount = doc["params"][2] if doc["params"][3]["refName"] == MOD_COIN else doc["params"][4]
            while ts >= candle_end:
                output[4, idx] = _vol
                candle_end += FIVE_MIN
                idx += 1
                _vol = 0.0
            _vol += _float(amount)
        output[4, idx] = _vol

        return output

    async def get_candles_5min(self, ts_start, ts_stop, in_usd=False):
        logger.debug("===============> GET_CANDLES_5MIN {} {} => {} {}".format(self._pair, ts_start, ts_stop, "USD" if in_usd else ""))
        idx_start, idx_stop =  map(self.idx_5min, (ts_start, ts_stop))
        data = self.candles_5min[:, idx_start:idx_stop]

        if np.ma.is_masked(data[0,:]):
            dirty_idx  = int(data[0,:].mask.argmax())
            dirty_ts = ts_start + timedelta(minutes=5*dirty_idx)
            logger.debug("===============> CANDLES_5_MIN_DIRTY {} {} => {}".format(self._pair, dirty_ts, ts_stop))
            await self._gen_candles_batch(dirty_ts, ts_stop, data[:,dirty_idx:])
            # We maintain here the ATL / ATH
            self.ath_kda = max(self.ath_kda, data[2,:].max())
            self.atl_kda = min(self.atl_kda, data[3,:].min())
            self.ath = max(self.ath, pricer.to_usd_price(data[2,:], ts_start).max())
            self.atl = min(self.atl, pricer.to_usd_price(data[3,:], ts_start).min())

            # If some data are more recent than 1 hour, we will invalidate them in 30.0s. We have to keep fresh data !!
            invalid_limit = max(from_now_5_minutes(-ONE_HOUR), self.ref_date_5min)
            if ts_stop >= invalid_limit:
                asyncio.get_running_loop().call_later(30.0, self.invalidate, invalid_limit, ts_stop)

        return pricer.to_usd_price(data, ts_start) if in_usd else data


    async def get_candles_1hour(self, ts_start, ts_stop, in_usd=False):
        logger.debug("===============> GET_CANDLES_1HOUR {} {} => {} {}".format(self._pair, ts_start, ts_stop, "USD" if in_usd else ""))
        idx_start, idx_stop =  map(self.idx_1hour, (ts_start, ts_stop))
        logger.debug("===============> GET_CANDLES_1HOUR {} {} => {}".format(self._pair, idx_start, idx_stop))
        data = self.candles_1hour[5:10, idx_start:idx_stop] if in_usd else self.candles_1hour[0:5, idx_start:idx_stop]

        if np.ma.is_masked(data[0,:]):
            dirty_idx  = int(data[0,:].mask.argmax())
            dirty_ts = max(ts_start + timedelta(hours=dirty_idx), self.ref_date_5min)
            logger.debug("===============> CANDLES_1HOUR_DIRTY {} {} => {}".format(self._pair, dirty_ts, ts_stop))
            c = await self.get_candles_5min(dirty_ts, ts_stop, in_usd=in_usd)
            _aggregate_candles(c, 12, data[:,dirty_idx:])
        return data

    async def get_candles_1day(self, ts_start, ts_stop, in_usd=False):
        logger.debug("===============> GET_CANDLES_1DAY {} {} => {} {}".format(self._pair, ts_start, ts_stop, "USD" if in_usd else ""))
        idx_start, idx_stop =  map(self.idx_1day, (ts_start, ts_stop))
        logger.debug("===============> GET_CANDLES_1DAY {} {} => {}".format(self._pair, idx_start, idx_stop))
        data = self.candles_1day[5:10, idx_start:idx_stop] if in_usd else self.candles_1day[0:5, idx_start:idx_stop]

        if np.ma.is_masked(data[0,:]):
            dirty_idx  = int(data[0,:].mask.argmax())
            dirty_ts = max(ts_start + timedelta(days=dirty_idx), self.ref_date_1hour)
            logger.debug("===============> CANDLES_1DAY_DIRTY {} {} => {}".format(self._pair, dirty_ts, ts_stop))
            c = await self.get_candles_1hour(dirty_ts, ts_stop, in_usd=in_usd)
            _aggregate_candles(c, 24, data[:,dirty_idx:])

        return data

    async def history_price(self, delay):
        target_ts = datetime.utcnow() - delay
        doc = await UPDATE.find_one({"params.0":self._pair, "ts":{"$lte":target_ts}}, sort=REVERSE_SORT)
        if doc:
            price = to_kda_price(doc)
            return (price, pricer.at(doc["ts"])*price)

        # In case we found no matching doc for the given delay, return the most recent price
        return await self.current_price()

    async def current_price(self):
        doc = await UPDATE.find_one({"params.0":self._pair}, sort=REVERSE_SORT)
        price = to_kda_price(doc)
        return (price, pricer.at(doc["ts"])*price)



def format_transaction_record(doc):
    token_a_am = _float(doc["params"][2])
    token_a_mod = module_fqn(doc["params"][3]["refName"])
    token_b_am = _float(doc["params"][4])
    token_b_mod = module_fqn(doc["params"][5]["refName"])
    token_a_info = tokens_db.token_info(token_a_mod)
    token_b_info = tokens_db.token_info(token_b_mod)

    kda_price = pricer.at(doc["ts"])
    token_a =  {"ticker": token_a_info["symbol"], "address":token_a_mod, "img":token_a_info["img"], "amount":token_a_am}
    token_b =  {"ticker": token_b_info["symbol"], "address":token_b_mod, "img":token_b_info["img"], "amount":token_b_am}

    return {"requestkey":doc["reqKey"],
            "eventId":doc["rank"],
            "timestamp":doc["ts"].replace(tzinfo=UTC),
            "type": "BUY" if token_a_mod == COIN else "SELL",
            "token0": token_b if token_a_mod == COIN else token_a,
            "token1": token_a if token_a_mod == COIN else token_b,
            "amount": kda_price * (token_a_am if token_a_mod == COIN else token_b_am),
            "address": doc["params"][1],
            "price": kda_price * ((token_a_am /token_b_am) if token_a_mod == COIN else (token_b_am /token_a_am)),
            }


@alru_cache(maxsize=256, ttl=60)
async def transactions_by_pair(pair, ts_start, ts_stop, limit):
    asset = pair_to_asset(pair)
    if ts_start is None:
        ts_start = GENESIS

    if ts_stop is None:
        ts_stop = datetime.utcnow()

    req = SWAP.find({"$or": [{"params.3.refName":MOD_COIN, "params.5.refName":modref(asset)}, {"params.5.refName":MOD_COIN, "params.3.refName":modref(asset)}],
                              "ts":{"$gt":ts_start, "$lt":ts_stop}}).sort({"ts":-1, "rank":-1}).limit(limit)

    return [format_transaction_record(x) async for x in req]


@alru_cache(maxsize=256, ttl=60)
async def transactions_by_account(account, ts_start, ts_stop, limit):
    if ts_start is None:
        ts_start = GENESIS

    if ts_stop is None:
        ts_stop = datetime.utcnow()

    req = SWAP.find({"params.1":account, "ts":{"$gt":ts_start, "$lt":ts_stop}}).sort({"ts":-1, "rank":-1}).limit(limit)

    return [format_transaction_record(x) async for x in req]

@alru_cache(maxsize=50000)
async def tvl(pair, ts_start, ts_stop):
    def _ts_delta(x, y):
        return (x - y).total_seconds()

    logger.debug("===============> GET_TVL {} /  {} => {}".format(pair,ts_start, ts_stop))
    # Compute a TW average
    last_ts = ts_start
    last_from = last_to = 0.0
    acc_from = acc_to = 0.0

    prev_update = await UPDATE.find_one({"params.0":pair,"ts":{"$lt":ts_start}}, sort=[("ts",-1)])
    if prev_update:
        last_from, last_to = extract_amounts(prev_update)

    async for doc in UPDATE.find({"params.0":pair, "ts":{"$gte":ts_start, "$lt":ts_stop}}).sort({"ts":1, "rank":1}):
        delta = _ts_delta(doc["ts"], last_ts)
        acc_from += last_from * delta
        acc_to += last_to * delta
        last_from, last_to = extract_amounts(doc)
        last_ts = doc["ts"]

    final_delta = _ts_delta(ts_stop, last_ts)
    acc_from += last_from * final_delta
    acc_to += last_to * final_delta

    total_delta = _ts_delta(ts_stop, ts_start)

    #Invalidate cache in 1 hour for recent data
    if ts_stop  >= from_now(-ONE_HOUR):
        asyncio.get_running_loop().call_later(3600, tvl.cache_invalidate, pair, ts_start, ts_stop)

    return (acc_from / total_delta, acc_to /total_delta)


@alru_cache(ttl=120.0)
async def get_pair_stats(pair):
    if pair not in CANDLE_STORES:
        return None
    store = CANDLE_STORES[pair]
    token_a, token_b = pair.split(":")
    asset, currency = (token_b, token_a) if token_a == COIN else (token_a, token_b)
    token0_info = tokens_db.token_info(asset)
    token1_info = tokens_db.token_info(currency)
    asset_info =  tokens_db.token_info(asset)

    # For volume
    logger.info("===============> GET_PAIR_STATS {}".format(pair))
    candle_data = await store.get_candles_5min(from_now_5_minutes(-ONE_DAY), from_now_5_minutes(FIVE_MIN), in_usd=True)
    candle_data_kda = await store.get_candles_5min(from_now_5_minutes(-ONE_DAY), from_now_5_minutes(FIVE_MIN), in_usd=False)
    cur_price = candle_data[1,-1] # We take the last close price
    cur_price_kda = candle_data_kda[1,-1] # We take the last close price

    one_hour_price, one_day_price, seven_days_price,  = await asyncio.gather(store.history_price(ONE_HOUR),
                                                                             store.history_price(ONE_DAY),
                                                                             store.history_price(SEVEN_DAYS))



    return {"id": "KDA:{}".format(asset_info["symbol"]),
            "symbol": ":".join((asset_info["symbol"],"USD", DEFAULT_EXCHANGE.symbol)),
            "symbolKda": ":".join((asset_info["symbol"],"KDA", DEFAULT_EXCHANGE.symbol)),
            "token0":{"name":token0_info["name"], "address":asset, "img":token0_info["img"]},
            "token1":{"name":token1_info["name"], "address":currency, "img":token1_info["img"]},
            "exchange":DEFAULT_EXCHANGE._asdict(),
            "pair": "KDA/{}".format(asset),
            "price": cur_price,
            "priceKda": cur_price_kda,
            "pricePercChange1h": (cur_price-one_hour_price[1])/one_hour_price[1],
            "pricePercChange24h": (cur_price-one_day_price[1])/one_day_price[1],
            "pricePercChange7d": (cur_price-seven_days_price[1])/seven_days_price[1],
            "pricePercChange1hKda": (cur_price_kda-one_hour_price[0])/one_hour_price[0],
            "pricePercChange24hKda": (cur_price_kda-one_day_price[0])/one_day_price[0],
            "pricePercChange7dKda": (cur_price_kda-seven_days_price[0])/seven_days_price[0],
            "volume24h": candle_data[4,:].sum(),
            "volume24hKda": candle_data_kda[4:].sum(),
            "totalSupply": asset_info.get("totalSupply",0.0),
            "circulatingSupply": asset_info.get("circulatingSupply", asset_info.get("totalSupply",0.0)),
            "socials": asset_info["socials"],
            "allTimeHigh": store.ath,
            "allTimeLow": store.atl,
            "allTimeHighKda": store.ath_kda,
            "allTimeLowKda": store.atl_kda
            }


@alru_cache(maxsize=50000)
async def get_volume_stats(asset_from, asset_to, ts_start, ts_stop):
    logger.debug("===============> GET_VOLUME_STATS {} => {} / {} => {}".format(asset_from, asset_to, ts_start, ts_stop))
    token_from, token_to = map(modref, (asset_from, asset_to))
    volume_from = 0.0
    volume_to = 0.0
    async for doc in SWAP.find({"params.3.refName":token_from, "params.5.refName":token_to,"ts":{"$gte":ts_start, "$lt":ts_stop}}, batch_size=200):
        volume_from += _float(doc["params"][2])
        volume_to += _float(doc["params"][4])

    #Invalidate cache in 30s for recent candles
    if ts_stop  >= from_now(-ONE_HOUR):
        asyncio.get_running_loop().call_later(3600, get_volume_stats.cache_invalidate, asset_from, asset_to, ts_start, ts_stop)


    return {"tokenFromNamespace":token_from["namespace"],
            "tokenFromName":token_from["name"],
            "tokenToNamespace":token_to["namespace"],
            "tokenToName":token_to["name"],
            "tokenFromVolume": volume_from,
            "tokenToVolume": volume_to}

@alru_cache(maxsize=1000, ttl=120)
async def get_Xly_volume_report(start_day, report_type="DAILY"): #pylint: disable=invalid-name
    logger.debug("===============> GET_VOLUME_REPORT {} ({})".format(start_day, report_type))
    if report_type == "DAILY":
        ts_start = start_day
        ts_stop = start_day + ONE_DAY
        meta = {"day":start_day, "dayString":start_day.date().isoformat()}
        _id = ts_start.date().isoformat()
    elif report_type == "WEEKLY":
        ts_start = start_of_week(start_day)
        ts_stop = ts_start + SEVEN_DAYS
        calendar = ts_start.date().isocalendar()
        meta = {"startDay":ts_start.isoformat(), "year":str(calendar.year), "week":str(calendar.week)}
        _id = "{:d}-W{:d}".format(calendar.year, calendar.week)
    elif report_type == "MONTHLY":
        ts_start = start_of_month(start_day)
        ts_stop = next_month(ts_start)
        meta = {"startDay":ts_start.isoformat(), "year":str(ts_start.year), "month":str(ts_start.month)}
        _id = "{:d}-{:d}".format(ts_start.year,  ts_start.month)
    else:
        raise ValueError("Bad report type")

    volumes = []
    for asset in map(pair_to_asset, await get_all_pairs()):
        if not asset:
            continue
        vol_from, vol_to = await asyncio.gather(get_volume_stats(asset, COIN, ts_start, ts_stop),
                                                get_volume_stats(COIN, asset, ts_start, ts_stop))

        if vol_from["tokenFromVolume"] > 0.0:
            volumes.append(vol_from | meta)

        if vol_to["tokenFromVolume"] > 0.0:
            volumes.append(vol_to | meta)

    return {"_id":_id , "volumes":volumes}


@alru_cache(maxsize=1000, ttl=120)
async def get_Xly_tvl_report(start_day, report_type="DAILY"): #pylint: disable=invalid-name
    logger.debug("===============> GET_TVL_REPORT {} ({})".format(start_day, report_type))
    if report_type == "DAILY":
        ts_start = start_day
        ts_stop = start_day + ONE_DAY
        meta = {"day":start_day, "dayString":start_day.date().isoformat()}
        _id = ts_start.date().isoformat()
    elif report_type == "WEEKLY":
        ts_start = start_of_week(start_day)
        ts_stop = ts_start + SEVEN_DAYS
        calendar = ts_start.date().isocalendar()
        meta = {"startDay":ts_start.isoformat(), "year":str(calendar.year), "week":str(calendar.week)}
        _id = "{:d}-W{:d}".format(calendar.year, calendar.week)
    elif report_type == "MONTHLY":
        ts_start = start_of_month(start_day)
        ts_stop = next_month(ts_start)
        meta = {"startDay":ts_start.isoformat(), "year":str(ts_start.year), "month":str(ts_start.month)}
        _id = "{:d}-{:d}".format(ts_start.year,  ts_start.month)
    else:
        raise ValueError("Bad report type")
    tvls = []
    for pair in await get_all_pairs():
        token_from, token_to = pair.split(":")
        (tvl_from, tvl_to) = await tvl(pair, ts_start, ts_stop)
        if tvl_from > 0.0 and tvl_to > 0.0:
            data = {"tokenFrom": token_from,
                    "tokenTo":token_to,
                    "tokenFromTVL": tvl_from,
                    "tokenToTVL": tvl_to
                   }
            tvls.append(data | meta)

    return {"_id":_id , "tvl":tvls}

@alru_cache(maxsize=500, ttl=60)
async def get_candle_stat(asset, day_start, day_end):
    logger.debug("===============> GET_CANDLE_STAT {} {} => {}".format(asset, day_start, day_end))
    pair = to_pair(asset, COIN)
    if pair not in CANDLE_STORES:
        return None
    store = CANDLE_STORES[pair]
    day_start = max(day_start, store.ref_date_1day)
    day_end = max(start_of_day(datetime.utcnow()), day_end)
    data = await store.get_candles_1day(day_start, day_end+ONE_DAY, in_usd=False)
    data_usd = await store.get_candles_1day(day_start, day_end+ONE_DAY, in_usd=True)

    def __format_candle(_candle):
        return {"open":_candle[0], "close":_candle[1], "high":_candle[2], "low":_candle[3]}

    def __format_data(day, candle_kda, candle_usd):
        return { "day":day,
                "dayString":day.date().isoformat(),
                "pairName":to_pair(asset, COIN),
                "price":__format_candle(candle_kda),
                "usdPrice":__format_candle(candle_usd),
                "volume":candle_kda[4]}

    return [__format_data(d,c,c_usd) for d,c, c_usd in zip(day_range(day_start, day_end), data.T, data_usd.T)]


NEUTRAL_CANDLE = {"open":1.0, "close":1.0, "high":1.0, "low":1.0}

@alru_cache(maxsize=500, ttl=300)
async def get_candle_stat_kda(day_start, day_end):
    day_start = start_of_day(day_start)

    def __fetch_format_data(day):
        candle_data = pricer.get_prices(day, day + ONE_DAY)
        return {"day":day,
                "dayString":day.date().isoformat(),
                "pairName":"KDA/USDT",
                "usdPrice":{"open":candle_data[0], "close":candle_data[-1], "high":candle_data.max(), "low":candle_data.min()},
                "price":NEUTRAL_CANDLE,
                "volume":0.0}

    return [__fetch_format_data(d) for d in day_range(day_start, day_end)]

DURATION = {"5":FIVE_MIN, "60":ONE_HOUR, "1D":ONE_DAY}

@alru_cache(maxsize=500, ttl=60)
async def get_udf_history(pair, ts_start, ts_stop, countback, resolution, in_usd=True):
    logger.debug("===============> UDF_HISTORY {} {} => {}".format(pair, ts_start, ts_stop))
    ts_stop = min(ts_stop, datetime.utcnow())
    store = CANDLE_STORES[pair]
    if resolution == "5":
        ts_stop = max(start_of_5_minutes(ts_stop) + FIVE_MIN, store.ref_date_5min)
        ts_start = max(ts_stop - countback*FIVE_MIN, store.ref_date_5min)
        duration = FIVE_MIN
        data = await store.get_candles_5min(ts_start, ts_stop, in_usd)
    elif resolution == "60":
        ts_stop = max(start_of_hour(ts_stop) + ONE_HOUR, store.ref_date_1hour)
        ts_start = max(ts_stop - countback*ONE_HOUR, store.ref_date_1hour)
        duration = ONE_HOUR
        data = await store.get_candles_1hour(ts_start, ts_stop, in_usd)
    elif resolution == "1D":
        ts_stop = max(start_of_day(ts_stop) +ONE_DAY, store.ref_date_1day)
        ts_start = max(ts_stop - countback*ONE_DAY, store.ref_date_1day)
        duration = ONE_DAY
        data = await store.get_candles_1day(ts_start, ts_stop, in_usd)
    else:
        raise ValueError("UnsupportedResolution")

    t = list(ts_range(ts_start, ts_stop, duration))
    result = {"t":t,
              "o":data[0,:].data,
              "c":data[1,:].data,
              "h":data[2,:].data,
              "l":data[3,:].data,
              "v":data[4,:].data,
              "s": "ok" if t else "no_data"}

    return result

CACHED_FUNCTIONS =  [get_all_pairs, transactions_by_pair, transactions_by_account, tvl, get_pair_stats, get_candle_stat, get_volume_stats, get_udf_history]

def clear_caches():
    logger.info("Clearing all caches")
    for fct in CACHED_FUNCTIONS:
        fct.cache_clear()

def cache_info():
    return {fct.__name__:fct.cache_info()._asdict() for fct in CACHED_FUNCTIONS} | {k:v.cache_info() for k,v in CANDLE_STORES.items()}

def last_swaps():
    return  map(lambda x:x.last_swap, CANDLE_STORES.values())

def last_swap():
    return reduce(max, last_swaps(), datetime.min)
