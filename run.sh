#!/bin/sh -ex

IN=/var/log
[ $(uname -n) = "wallaby" ] && IN=../var-log-from-pi

OUT=../converted

TAIL_NUMBER=${1:-DEABC}

for f in ${IN}/sensors_*.csv
do
    i=$(basename $f)

    o=${OUT}/foreflight/$(echo $i | sed -e 's|sensors_|stratux_converted_ff_|')
    # echo $f $i $o
    mkdir -p $(dirname $o)
    ./stratux_sensors_to_foreflight.py $f -o $o --tail-number=${TAIL_NUMBER} --airports airports.csv --debug
done
