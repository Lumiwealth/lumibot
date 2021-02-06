import logging
import os

from redis import Redis


def set_redis_db(host="localhost", port=6379, db_number=0):
    os.environ["LUMIBOT_USE_REDIS"] = "TRUE"
    os.environ["LUMIBOT_REDIS_HOST"] = str(host)
    os.environ["LUMIBOT_REDIS_PORT"] = str(port)
    os.environ["LUMIBOT_REDIS_DB"] = str(db_number)
    logging.info(f"setting redis db at redis://{host}:{port}/{db_number}")


def get_redis_db():
    return RedisCacheData.check_redis_db()


class RedisCacheData:
    @classmethod
    def check_redis_db(cls):
        LUMIBOT_USE_REDIS = os.environ.get("LUMIBOT_USE_REDIS")
        if LUMIBOT_USE_REDIS and LUMIBOT_USE_REDIS.upper() == "TRUE":
            LUMIBOT_REDIS_HOST = os.environ.get("LUMIBOT_REDIS_HOST")
            if not LUMIBOT_REDIS_HOST:
                LUMIBOT_REDIS_HOST = "localhost"

            LUMIBOT_REDIS_PORT = os.environ.get("LUMIBOT_REDIS_PORT")
            if not LUMIBOT_REDIS_PORT:
                LUMIBOT_REDIS_PORT = 6379

            LUMIBOT_REDIS_DB = os.environ.get("LUMIBOT_REDIS_DB")
            if not LUMIBOT_REDIS_DB:
                LUMIBOT_REDIS_DB = 0

            try:
                return cls(LUMIBOT_REDIS_HOST, LUMIBOT_REDIS_PORT, LUMIBOT_REDIS_DB)
            except:
                logging.error("Could not find the redis db")
                return None

        return None

    @staticmethod
    def decode(value):
        if isinstance(value, bytes):
            value = value.decode(encoding="utf8")
        return value

    @staticmethod
    def parse_redis_value(value):
        if isinstance(value, dict):
            result = {}
            for key, val_raw in value.items():
                val = RedisCacheData.parse_redis_value(val_raw)
                result[key] = val
            return result

        try:
            return float(value)
        except:
            return value

    @staticmethod
    def build_key(source, entity, identifier, subidentifier=""):
        key = f"LUMIBOT_{source}_{entity}_{identifier}"
        if subidentifier:
            key = f"{key}_{subidentifier}"
        return key.upper()

    @staticmethod
    def build_pattern(source="*", entity="*", identifier="*", subidentifier=""):
        pattern = f"LUMIBOT_{source}_{entity}_{identifier}"
        if source != "*" and entity != "*" and identifier != "*":
            return f"{pattern}_*".upper()

        if subidentifier and subidentifier != "*":
            return f"{pattern}_{subidentifier}".upper()

        return patter.upper()

    def __init__(self, host="localhost", port=6379, db_number=0):
        self.host = host
        self.port = port
        self.db_number = db_number
        self.url = f"redis://{host}:{port}/{db_number}"
        self._redis = Redis(host=host, port=port, db=db_number, decode_responses=True)

    def store_item(self, item, source, entity, identifier, subidentifier=""):
        key = self.build_key(source, entity, identifier, subidentifier)
        if isinstance(item, dict):
            self._redis.hset(key, mapping=item)
        else:
            self._redis.set(key, item)

    def store_bars(self, bars):
        for index, row_raw in bars.df.iterrows():
            timestamp = int(index.timestamp())
            row = row_raw.to_dict()
            row["timestamp"] = timestamp
            self.store_item(row, bars.source, "bar", bars.symbol, timestamp)

    def retrieve_by_key(self, key):
        type_ = self._redis.type(key)
        val = None
        if type_ == "hash":
            raw_val = self._redis.hgetall(key)
            val = self.parse_redis_value(raw_val)
        elif type == "string":
            raw_val = self._redis.get(key)
            val = self.parse_redis_value(raw_val)

        return val

    def retrieve_item(self, source, entity, identifier, subidentifier=""):
        key = self.build_key(source, entity, identifier, subidentifier)
        return self.retrieve_by_key(key)

    def retrieve_store(self, source, symbols, parser=None):
        if parser is not None and not callable(parser):
            raise Exception("parser parameter is not callable")

        result = {}
        for symbol in symbols:
            result[symbol] = []
            pattern = self.build_pattern(source, "BAR", symbol)
            for key in self._redis.scan_iter(pattern, count=1000):
                val = self.retrieve_by_key(key)
                if parser:
                    val = parser(val)

                result[symbol].append(val)

        return result

    def bgsave(self):
        self._redis.bgsave()
