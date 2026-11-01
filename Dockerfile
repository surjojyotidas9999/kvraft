FROM ubuntu:24.04 AS build
RUN apt-get update && apt-get install -y g++ && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY src ./src
RUN g++ -std=c++17 -O2 -pthread src/raft_main.cpp src/storage/wal.cpp src/storage/sstable.cpp -o raft_node

FROM ubuntu:24.04
WORKDIR /app
COPY --from=build /src/raft_node /app/raft_node
ENTRYPOINT ["/app/raft_node"]