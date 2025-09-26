#!/bin/sh -ex

IN=/var/log
[ $(uname -n) = "wallaby" ] && IN=../var-log-from-pi

OUT=../converted
mkdir -p ${OUT}

for f in ${IN}/sensors_*.csv
do
    i=$(basename $f)
    # o=$(echo $i | sed -e 's|.csv$|.gpx|')
    o=$(echo $i | sed -e 's|sensors_|stratux_converted_ff_|')
    echo $f $i $o

    ./stratux_sensors_to_foreflight.py $f -o ${OUT}/$o --airports airports.csv --debug
done


