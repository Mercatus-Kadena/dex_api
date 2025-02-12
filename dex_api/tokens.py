import os
from urllib.parse import urljoin
from datetime import datetime, UTC
import logging

import aiohttp
import yaml

logger = logging.getLogger(__name__)

NETWORK = "mainnet"

BASE_URL = os.environ.get('TOKENS_DB') or "https://raw.githubusercontent.com/CryptoPascal31/kadena_tokens/main/"

def default_token_symbol(token_mod):
    ns, mod = token_mod.split(".")
    return mod.upper() if ns.startswith("n_") else "{}-{}".format(ns, mod).upper()

class TokensDB:
    def __init__(self):
        self._last_db_hash = None
        self._checked = None
        self._updated = None
        self.tokens = None
        self.blacklist = None

    @property
    def url(self):
        return urljoin(BASE_URL, "tokens.yaml")

    @property
    def network(self):
        return NETWORK

    def get_info(self):
        return {"url":self.url, "network":self.network, "checked": self._checked,
                "updated":self._updated, "hash":self._last_db_hash, "data":self.tokens}

    async def update(self):
        try:
            logger.info("Downloading token database")
            self._checked = datetime.now(tz=UTC)
            async with aiohttp.ClientSession() as session:
                async with session.get(urljoin(BASE_URL, "tokens.yaml")) as resp:
                    yaml_data = await resp.read()
                    new_hash = hash(yaml_data)
                    if new_hash == self._last_db_hash:
                        return False

                    logger.info("Database updated")
                    self._last_db_hash = new_hash
                    data = yaml.safe_load(yaml_data)

                    self.tokens = data[NETWORK]
                    self.blacklist = data["blacklist"]
                    self._updated = datetime.now(tz=UTC)
                    # Adjust IMGS URL and missing descriptions
                    for tok in self.tokens.values():
                        tok["img"] = urljoin(BASE_URL, tok["img"])
                        tok["name"] = "" if tok.get("name", None) is None else tok["name"]
                        tok["description"] = "" if tok.get("description", None) is None else tok["description"]

                    return True

        except:  #pylint: disable=bare-except
            logger.error("Cannot retrieve Tokens Database")
            return False

    def token_mod_from_symbol(self, token_symb):
        for k,v in self.tokens.items():
            if v["symbol"] == token_symb:
                return k
        return None

    def token_mod_search(self, req_data):
        _req_data = req_data.lower()
        for k,v in self.tokens.items():
            for field in ["symbol", "name", "description"]:
                if _req_data in v[field].lower():
                    yield k
                    break

    def is_token_listed(self, tok):
        return tok in self.tokens and tok not in self.blacklist

    def token_info(self, token_mod):
        if token_mod in self.tokens:
            return self.tokens[token_mod]
        return {"symbol":default_token_symbol(token_mod),
                "name":default_token_symbol(token_mod),
                "description": "",
                "img":"",
                "img_data": None,
                "socials": [],
                "totalSupply":0.0,
                "circulatingSupply":0.0
               }

    def token_symbol(self, token_mod):
        if token_mod in self.tokens:
            return self.tokens[token_mod]["symbol"]
        return None

tokens_db = TokensDB()
