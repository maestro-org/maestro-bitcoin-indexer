#!/bin/sh
# Cluster-of-one Redis bootstrap.
#
# The stack's services connect with Redis *Cluster* clients, which require cluster
# mode even for a single node: the node must own all 16384 hash slots before the
# cluster reports `cluster_state:ok` and clients will talk to it.
#
# REDIS_ANNOUNCE_IP (optional): address this node advertises to cluster clients.
# Inside docker compose the container IP is reachable and no announce is needed;
# set it to 127.0.0.1 when exposing the node to clients on the host (playground).
set -eu

redis-server \
    --cluster-enabled yes \
    --appendonly no \
    --save '' \
    ${REDIS_ANNOUNCE_IP:+--cluster-announce-ip "$REDIS_ANNOUNCE_IP"} &
PID=$!

until redis-cli ping >/dev/null 2>&1; do sleep 0.2; done

if ! redis-cli cluster info | grep -q 'cluster_state:ok'; then
    redis-cli cluster addslotsrange 0 16383
fi

wait $PID
