#!/bin/bash

DATA_DIR=/Volumes/Archive/data/binance_data
OUT_DIR=/Volumes/Archive/data_export/binance_data

export_data() {
    local date_arg=$1
    echo "Exporting data for date: ${date_arg}."
    python binance_exporter.py \
        --tar_file_path=${DATA_DIR}/${date_arg}.tar.gz \
        --out_dir=${OUT_DIR} 2>&1
    if [ $? -eq 0 ]; then
        echo "Exporting data for date: ${date_arg} completed."
    else
        echo "Exporting data for date: ${date_arg} failed."
    fi
}

main() {
    export_data 20250921
    export_data 20250922
    export_data 20250923
    export_data 20250924
    export_data 20250925
    export_data 20250926
    export_data 20250927
    export_data 20250928
}

mkdir -p log
main > log/binance_export.log 2> log/binance_export.err
