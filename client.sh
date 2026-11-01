#!/bin/bash
# Usage: ./client.sh "PUT name hello"
# Tries each node until it finds the leader.
for port in 8001 8002 8003; do
  reply=$( ( exec 3<>/dev/tcp/127.0.0.1/$port || exit 1
             printf '%s\n' "$1" >&3
             read -t 5 -r line <&3
             echo "$line" ) 2>/dev/null )
  if [ -n "$reply" ] && [[ "$reply" != NOT_LEADER* ]]; then
    echo "$reply"
    exit 0
  fi
done
echo "no leader available"