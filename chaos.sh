#!/bin/bash
# Fault-injection test for the kvraft Raft cluster.
# A background writer keeps writing keys while nodes are killed with kill -9
# (a hard crash) and restarted. At the end, every write that was acknowledged
# with "OK" must still be there, on every node.
# Usage: ./chaos.sh [rounds]     (default 8)

cd "$(dirname "$0")" || exit 1
ROUNDS=${1:-8}

if [ ! -x ./raft_node ]; then echo "build raft_node first"; exit 1; fi

pkill -x raft_node 2>/dev/null
sleep 1
rm -rf node*_data logs acked.txt stop.flag
mkdir -p logs
: > acked.txt

alive=(0 0 0 0)
pids=(0 0 0 0)
FAILS=0

cleanup() { touch stop.flag; pkill -x raft_node 2>/dev/null; }
trap cleanup EXIT

# Send one line to a node's port and print its one-line reply.
rpc() {
  ( exec 3<>/dev/tcp/127.0.0.1/$1 || exit 1
    printf '%s\n' "$2" >&3
    read -t 5 -r line <&3
    echo "$line" ) 2>/dev/null
}

start_node() {
  ./raft_node "$1" 8001 8002 8003 >> "logs/node$1.log" 2>&1 &
  pids[$1]=$!
  disown $!
  alive[$1]=1
}

kill_node() {
  kill -9 "${pids[$1]}" 2>/dev/null
  alive[$1]=0
  sleep 0.3
}

find_leader() {
  for i in 1 2 3; do
    [ "${alive[$i]}" = 1 ] || continue
    s=$(rpc 800$i "STATUS")
    if [[ "$s" == *"role=LEADER"* ]]; then echo $i; return; fi
  done
}

wait_for_leader() {
  for t in $(seq 1 40); do
    [ -n "$(find_leader)" ] && return 0
    sleep 0.5
  done
  return 1
}

# Wait until all 3 nodes have applied the same number of log entries.
wait_converge() {
  for t in $(seq 1 60); do
    vals=""
    for i in 1 2 3; do
      s=$(rpc 800$i "STATUS")
      vals="$vals $(echo "$s" | sed -n 's/.*applied=\([0-9]*\).*/\1/p')"
    done
    set -- $vals
    if [ $# -eq 3 ] && [ "$1" = "$2" ] && [ "$2" = "$3" ] && [ "$1" != "0" ]; then return 0; fi
    sleep 0.5
  done
  echo "   (nodes did not fully converge in 30 seconds)"
  FAILS=$((FAILS + 1))
  return 1
}

# Try a write up to 10 times. Returns success only if the cluster answered "OK".
write_key() {
  for attempt in 1 2 3 4 5 6 7 8 9 10; do
    reply=$(./client.sh "PUT $1 $2")
    if [ "$reply" = "OK" ]; then return 0; fi
    sleep 0.5
  done
  return 1
}

get_retry() {
  for attempt in 1 2 3 4 5 6 7 8 9 10; do
    reply=$(./client.sh "GET $1")
    if [[ "$reply" == VALUE* || "$reply" == NOT_FOUND* ]]; then echo "$reply"; return; fi
    sleep 0.5
  done
  echo "$reply"
}

writer() {
  i=0
  while [ ! -f stop.flag ]; do
    i=$((i + 1))
    if write_key "k$i" "v$i"; then echo "k$i v$i" >> acked.txt; fi
    sleep 0.1
  done
}

# Check every acknowledged write: through the leader, then on each node's own copy.
verify() {
  local bad=0 total=0 k v reply i nbad
  while read -u 4 -r k v; do
    total=$((total + 1))
    reply=$(get_retry "$k")
    if [ "$reply" != "VALUE $v" ]; then
      echo "   LOST OR WRONG: $k expected 'VALUE $v', got '$reply'"
      bad=$((bad + 1))
    fi
  done 4< acked.txt
  echo "   $total acknowledged writes read back through the leader: $bad problems"
  FAILS=$((FAILS + bad))

  for i in 1 2 3; do
    nbad=0
    while read -u 4 -r k v; do
      reply=$(rpc 800$i "LOCALGET $k")
      [ "$reply" = "VALUE $v" ] || nbad=$((nbad + 1))
    done 4< acked.txt
    echo "   node $i's own copy: $nbad missing or wrong"
    FAILS=$((FAILS + nbad))
  done
}

# ---------------- the test ----------------

echo "starting 3 nodes..."
for i in 1 2 3; do start_node $i; done
if ! wait_for_leader; then echo "no leader was elected"; exit 1; fi
echo "leader is node $(find_leader)"

writer &
WRITER=$!

echo
echo "phase 1: killing and restarting nodes while writes keep coming in"
for round in $(seq 1 $ROUNDS); do
  sleep 2
  dead=""
  for i in 1 2 3; do [ "${alive[$i]}" = 0 ] && dead=$i; done

  if [ -n "$dead" ]; then
    echo "  round $round: restarting node $dead   (writes so far: $(wc -l < acked.txt))"
    start_node $dead
    sleep 2
  else
    L=$(find_leader)
    if [ $((RANDOM % 2)) -eq 0 ] && [ -n "$L" ]; then victim=$L; else victim=$((RANDOM % 3 + 1)); fi
    tag=""
    [ "$victim" = "$L" ] && tag=" (the leader)"
    echo "  round $round: killing node $victim$tag   (writes so far: $(wc -l < acked.txt))"
    kill_node $victim
    sleep 3
  fi
done

touch stop.flag
wait $WRITER
for i in 1 2 3; do [ "${alive[$i]}" = 1 ] || start_node $i; done
wait_for_leader
wait_converge
echo
echo "checking after phase 1:"
verify

echo
echo "phase 2: hard-killing ALL nodes at once, then restarting them"
for i in 1 2 3; do kill_node $i; done
sleep 1
for i in 1 2 3; do start_node $i; done
wait_for_leader
wait_converge
echo "checking after full-cluster crash:"
verify

echo
if [ "$FAILS" -eq 0 ]; then
  echo "PASS: all $(wc -l < acked.txt) acknowledged writes survived"
else
  echo "FAIL: $FAILS problems (see the lines above and logs/node*.log)"
  exit 1
fi