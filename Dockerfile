FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV LD_LIBRARY_PATH=/app/lib

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libstdc++6 \
    zlib1g \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN mkdir -p /app/bin /app/config /app/logs /app/data /app/lib

COPY bin/KVServerNode /app/bin/KVServerNode
COPY deploy/runtime/lib/ /app/lib/
COPY deploy/config/ /app/config/

EXPOSE 50001

ENTRYPOINT ["/app/bin/KVServerNode"]
CMD ["-config=/app/config/config-node1.yml"]
