#/usr/bin/env bash

diff <(sort -nk1 "$1") <(sort -nk1 "$2")
