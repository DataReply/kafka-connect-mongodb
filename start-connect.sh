#!/usr/bin/env bash

tag="[start-connect.sh]"

function info {
  echo "$tag (INFO) : $1"
}
function warn {
  echo "$tag (WARN) : $1"
}
function error {
  echo "$tag (ERROR): $1"
}

echo ""
info "Starting..."

CONNECT_PID=0

handleSignal() {
  info 'Stopping... '
  if [ $CONNECT_PID -ne 0 ]; then
    kill -s TERM "$CONNECT_PID"
    wait "$CONNECT_PID"
  fi
  info 'Stopped'
  exit
}

trap "handleSignal" SIGHUP SIGINT SIGTERM
$CONNECT_BIN $CONNECT_CFG &
CONNECT_PID=$!

wait
