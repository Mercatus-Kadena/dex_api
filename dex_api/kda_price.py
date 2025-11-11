import logging

from pymongo import ReplaceOne
from decimal import Decimal
from datetime import datetime, timedelta, UTC

import aiohttp
from bson.decimal128 import Decimal128
import numpy as np

from .date_utils import FIVE_MIN, ONE_YEAR, ONE_HOUR


logger = logging.getLogger(__name__)

START_DATE = datetime.fromisoformat("2022-01-01T00:00:00.0000")

def _from_kucoin(doc):
    return (datetime.utcfromtimestamp(int(doc[0])), [Decimal(x) for x in  doc[1:5]])

def _to_ts(x):
    return int(x.replace(tzinfo=UTC).timestamp())

def _to_index(dt):
    return int((dt-START_DATE).total_seconds())//300

def _from_index(idx):
    return START_DATE + timedelta(minutes=5*int(idx))

### Some design notes:
#
# The pricer uses Kucoin 5 minutes candles as a source.
#
# Raw data is stored in the MongoDB database as a long term cache:
#    - Decimal type
#    - to improve startup time (no need to download again the full candles set  from Kucoin)
#    - to avoid being mitigated by Kucoin (looks ok though byt who know ?)
#
# Short term cache (the one really used for computations) is stored in the Numpy array (self.cache)
#   - as a Masked matrix (ie all missing data are masked)
#   - float64 type
#   - Each candles (i) has 5 components:
#      - self.cache[0, i] => Min
#      - self.cache[1, i] => Max
#      - self.cache[2, i] => Open
#      - self.cache[3, i] => Close
#      - self.cache[4, i] => Mean of those 4 values
#     As such, this array can be multiplied directly elementswise with a 5 elements candles computed in dex_data.
#
#  At startup:
#     - Existing data (if any) in MongoDB is preloaded into the short term cache.
#     - then missing data is downloaded from Kucoin
#
#  Each 2 minutes, most recent candles are downloaded from Kucoin, and caches are updated.
#
# ** External API
# def at(ts):
#   Return the value as float at a given timestamp
#
# def to_usd_price(mat, ts)
#   Convert data in Price/KDA in Price/USD, starting at ts
#   If mat is an array, it's supposed to be a simple array of price (respect to time), in 5 minutes increment.
#   If mat is a 5 cols matrix, it's supposed to be a list of full candled (Min Min, Open, Close, Volume).
#
# def get_prices(ts_start, ts_end)
#   Return an array of KDA/USD prices between two timestamps.


class KdaPriceFiveMin:
    DELTA = FIVE_MIN
    SYMBOL = "KDA-USDT"
    SOURCE = "Kucoin 5 minute candles"
    BATCH_SIZE = 1500

    def __init__(self, db):
        self.db = db
        self.col = db["kucoin_price_5min_details"]
        self.cache = np.ma.masked_all((5,_to_index(datetime.utcnow()+ONE_YEAR)))
        self.default_value = 0.0
        self.last_update = None

    def get_info(self):
        return {"cache_size":np.ma.count(self.cache[1,:]),
                "last_update":self.last_update,
                "start_date":START_DATE,
                "symbol":self.SYMBOL,
                "source":self.SOURCE,
                "delta": self.DELTA.seconds}

    async def init(self):
        idx = await self.col.index_information()
        if "ts_1" not in idx:
            logger.info("Kucoin index missing => Create")
            await self.col.create_index("ts")

        logger.info("Loading prices in cache")
        logger.debug("Starting date: {!s}".format(START_DATE))

        async for doc in self.col.find():
            if doc["ts"] >= START_DATE:
                self.cache[0:4, _to_index(doc["ts"])] = [x.to_decimal() for x in doc["prices"]]

        self.cache[-1,:] = self.cache[0:4,:].mean(axis=0)

        # Set up the default value from the last element in DB
        first_missing = np.argmax(self.cache.mask[0:,])
        if first_missing:
            self.default_value = self.cache[-1:first_missing-1]

        logger.info("Price cache ready")
        logger.info("Downloading KDA/USD missing data")
        await self.update_usd_data()
        logger.debug("Total cache size: {:d}".format(len(self.cache)))


    def at(self, ts):
        value = self.cache[:,_to_index(ts)]
        return self.default_value if np.ma.is_masked(value) else value.mean()

    def to_usd_price(self, mat, ts):
        idx = _to_index(ts)
        if mat.ndim==1:
            return mat * self.cache[-1,idx:idx + len(mat)].filled(self.default_value)
        if mat.ndim==2 and mat.shape[0] == 5:
            return mat * self.cache[:,idx:idx + mat.shape[-1]].filled(self.default_value)
        raise ValueError("Invalid matrix {!s}".format(mat.shape))


    def get_prices(self, ts_start, ts_end):
        idx_start, idx_end = map(_to_index, (ts_start, ts_end))
        return self.cache[4, idx_start:idx_end].filled(self.default_value)


    async def _load_from_kucoin(self, start_dt, end_dt):
        logger.info("Updating prices from Kucoin")
        logger.debug("Update period: {!s} => {!s}".format(start_dt, end_dt))

        def _candle_idx(x): return (x-_to_ts(start_dt))//self.DELTA.seconds


        async with aiohttp.ClientSession() as session:
            for st in range(_to_ts(start_dt),  _to_ts(end_dt), self.BATCH_SIZE*300):

                logger.info("Downloading {:d}/{:d} candles".format(_candle_idx(st), _candle_idx(_to_ts(end_dt))))

                params = {"type":"5min", "symbol":self.SYMBOL, "startAt":st, "endAt":st+self.BATCH_SIZE*300}

                async with session.get("https://api.kucoin.com/api/v1/market/candles", params=params) as resp:
                    raw_data = await resp.json()
                    data = sorted(map(_from_kucoin, raw_data["data"]), key=lambda x:x[0])
                    db_ops = [ReplaceOne({"ts":_ts}, {"ts":_ts, "prices":list(map(Decimal128, prices))}, upsert=True)
                                for _ts, prices in data]
                    if db_ops:
                       await self.col.bulk_write(db_ops)

                    for _ts, prices in data:
                        np_prices = np.array(prices, dtype=np.float64)
                        self.cache[0:4,_to_index(_ts)] = np_prices
                        self.cache[-1, _to_index(_ts)] = np_prices.mean()
                        # We use the last close value as a default value.
                        self.default_value = np_prices[1]
                        self.last_update = _ts

                    logger.debug("{:d} prices updated from Kucoin".format(len(data)))

    async def update_usd_data(self):
        first_missing = np.argmax(self.cache.mask[0:,])
        logger.debug("First missing data: {!s} ( {!s} )".format(first_missing, _from_index(first_missing)))
        first_missing = max(0, first_missing-5)

        await self._load_from_kucoin(_from_index(first_missing), datetime.utcnow())
