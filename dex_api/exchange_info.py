from collections import namedtuple

ExchangeInfoData = namedtuple("ExchangeInfoData", "symbol name description img")

MERCATUS = ExchangeInfoData(symbol="MERCATUS",
                            name="MERCATUS",
                            description="Universal Kadena DEX",
                            img="https://raw.githubusercontent.com/Mercatus-Kadena/ecko-dex-ui/refs/heads/main/public/favicon.svg")


DEFAULT_EXCHANGE = MERCATUS
