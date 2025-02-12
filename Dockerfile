# Use the official Python image from the Docker Hub
FROM python:3.12-slim

ARG CONFIG=unix
ENV SOCKET_TYPE ${CONFIG}

# Install the dependencies
RUN apt-get update; \
    apt-get install -y curl jq; \
    rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /build

# Copy the source
COPY pyproject.toml pyproject.toml
COPY README.md README.md
COPY dex_api dex_api
RUN pip install --root-user-action ignore build
RUN python -m build

# Install the wheel
RUN pip install --root-user-action ignore --no-cache-dir dist/*.whl

# And clean
RUN rm -rf build dex_api dist pyproject.toml


# Copy the default config file into the container
WORKDIR /app
COPY configs/hypercorn_config_${CONFIG}.toml hypercorn_config.toml

# Create a ready socket directory if needed
RUN if [ ${CONFIG} = "unix" ] ; then mkdir -p /run/dexapi ; fi

# Set the entrypoint to run the Python module
ENTRYPOINT ["hypercorn", "--config", "hypercorn_config.toml", "dex_api.api:app"]

# Create fetch_info script
COPY docker_utils/fetch_info.sh fetch_info.sh
RUN chmod a+x fetch_info.sh

HEALTHCHECK --start-period=10m \
  CMD /app/fetch_info.sh | jq -n "input.status"

# Building command:
#    docker build . --build-arg CONFIG=unix -t dex_api:x.x.x-unix
#    docker build . --build-arg CONFIG=tcp -t dex_api:x.x.x-tcp
# -where x.x.x is the version
