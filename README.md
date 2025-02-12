# Mercatus DEX API Implementation

## Architecture overview

![image](doc/dex_api.svg)

## Requirements
  - A MongoDB local database fed by the kadana_indexer tool.
  - The events `kaddex.exchange.SWAP` and `kaddex.exchange.UPDATE` must be indexed.

## Databases
  - Events MongoDB: Read-only, fed by kadena_indexer.
  - kda_usd MongoDB:  Read-Write, used to cache kda_usd price history. Avoiding doing the same requests again and again against Kucoin API.

## Theory

The daemon is a full async program:
  - Python AsyncIO
  - Hypercorn and ASGI API
  - MongoDB Motor driver
  - Numpy for efficient vectorized computations

The daemon pulls its data from:
  - SWAP and UPDATE events
  - KDA/USDT Kucoin's pair
  - Kadena Tokens Github database

### Time increment

The daemon uses a "5 minute" time increment as a basis. Every piece of data in computed and stored through "5 minute candles". Other and larger time increments are derived from the "5 minutes" candles.


### Caching

The daemon uses extensive caching and data pre-loading / pre-computing:
  - Data older than 7 days, cached  indefinitely.
  - Data between 7 days and 1 hour old: cache refresh happens every 24 hours.
  - Fresh data (less than 1 hour old): cache policy between 30s and 3 minutes depending on the data.

As such, in case of an interruption (eg node or indexer crash):
  - If the interruption lasts more than 7 days, a restart is required.
  - If the interruption lasts more than 1 hour, a restart is recommended (but not required, as it will automatically recover valid data after 24 hours).

Due to the caching, the daemon requires 300/400 Mb of RAM.

### REST API

#### Ecko Dex API

The daemon exposes an API compatible with the old and venerable Ecko-DEX API:
- ``/api/pairs``
- ``/api/transactions``
- ``/api/candles``
- ``/volume/daily``
- ``/volume/weekly``
- ``/volume/monthly``
- ``/tvl/daily``
- ``/tvl/weekly``
- ``/tvl/monthly``

#### UDF Compatible API (Tradingview)

- ``/udf/symbols``
- ``/udf/config``
- ``/udf/search``
- ``/udf/history``

#### Extra endpoints

- ``/info`` : Gives info about the daemon runtime: caching status , database status, health status, managed tokens, ...
- ``/kda_price`` : Gives the current KDA/USD price


### Status

The status can have the following values:
  - *pre-starting*: The HTTP API is not available. This can last up to 30 minutes in case the KDA/USD database is empty.
  * ```STARTING```: The API is not fully initialized. This transient state doesn't last more than a couple of seconds.
  * ``CACHING``: The API is working, but some pre-caching operations are in progress. Can last up to 2 hours. Performances are degraded.
  * ``RUNNING``: Normal status. Performances are optimal.


## Configuration

### Hypercorn configuration:
The hypercorn server can be configured using a config a TOML file, as described here:
    - https://hypercorn.readthedocs.io/en/latest/how_to_guides/configuring.html


### Databases configuration
To simplify the dockerization, the configuration is done using environment variables
 - `MONGO_URI` (default: *mongodb://localhost:27017*): URI of the MongoDB database
 - `USD_DATA_DB` (default: *usd_data*): DB name of the KDA/USD database
 - `EVENTS_DB` (default: *kadena_events*): DB name of the Events database (ie Indexer's output)
 - `TOKENS_DB` (default *https://raw.githubusercontent.com/CryptoPascal31/kadena_tokens/main/*): GH Tokens Database


## How to run it

## Directly on the host.

Install through pip (eventually in a virtual env) using the wheel:
```sh
pip3 install dex_api-0.11.3-py3-none-any.whl
```

Create a hypercorn_config.toml config file.

Then:
```sh
hypercorn --config hypercorn_config.toml dex_api.api:app
```

## Using Docker images

### Flavor 1: Unix socket
This image listens on a Unix socket `/run/dexapi/dexapi.socket`.

Example:
```sh
docker run -e MONGO_URI=mongodb://172.17.0.1:27017/?directConnection=true \
           -e USD_DATA_DB=usd_data_bis \
           -v /var/run/my-sockets/:/var/run/dexapi dex_api:0.11.3-unix
```


### Flavor 2: TCP socket
This image listen on TCP port 8080.

Example:
```sh
docker run -e MONGO_URI=mongodb://172.17.0.1:27017/?directConnection=true \
           -e USD_DATA_DB=usd_data_bis \
           -p 8080:8080 dex_api:0.11.3-tcp
```

## Proxying
In every case, it's not recommended to expose directly an Hypercorn socket directly on the web.

Using a reverse proxy is always preferable, especially for managing stuffs such as HTTPs stuffs, rate limiting, ...
