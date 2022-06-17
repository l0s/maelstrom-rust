# Maelstrom Rust

Rust implementations of the distributed systems in the
[Maelstrom](https://github.com/jepsen-io/maelstrom/) workbench.

## Running Workloads

The workloads require a StatsD server to be running. For local testing, 
Graphite can be used:

    docker run \
      --name graphite \
      --restart=always \
      -p 80:80 \
      -p 2003-2004:2003-2004 \
      -p 2023-2024:2023-2024 \
      -p 8125:8125/udp \
      -p 8126:8126 \
      --rm \                          
      graphiteapp/graphite-statsd
