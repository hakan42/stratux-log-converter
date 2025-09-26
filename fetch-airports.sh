#!/bin/sh -ex

wget \
    --quiet \
    --output-document=airports.csv \
    https://github.com/davidmegginson/ourairports-data/raw/refs/heads/main/airports.csv
