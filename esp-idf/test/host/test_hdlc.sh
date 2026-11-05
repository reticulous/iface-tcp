#!/bin/sh
# Build and run test_hdlc.cpp (the HDLC codec and receive pump in
# src/tcp_hdlc.h) against vectors from Python RNS's TCPInterface.
#   [PY=<python with RNS>] sh test_hdlc.sh
set -e
HERE=$(cd "$(dirname "$0")" && pwd)
PY=${PY:-/home/spangap/partyline-venv/bin/python}
OUT=${OUT:-/tmp/test_hdlc}
mkdir -p "$OUT"
"$PY" -I "$HERE/hdlc_vectors.py" "$OUT/vectors"
g++ -std=c++17 -O1 -g -Wall -Wextra -fsanitize=address,undefined \
    -I"$HERE/../../src" "$HERE/test_hdlc.cpp" -o "$OUT/test_hdlc"
"$OUT/test_hdlc" "$OUT/vectors"
