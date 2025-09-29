#!/bin/bash

source .env
if [ -z "$REMOTE" ]; then
    echo "REMOTE environment variable is not set, set REMOTE = root@<your_ip_address>."
    exit 1
fi

OUT_DIR=/Volumes/Archive/data

mkdir -p ${OUT_DIR}/binance_data
rsync -e 'ssh -o "ProxyCommand=nc -X 5 -x 127.0.0.1:15235 %h %p"' \
    -r -v --progress \
    --include="*.tar.gz" --exclude="*" \
    ${REMOTE}:/root/data/binance_data/ \
    ${OUT_DIR}/binance_data
