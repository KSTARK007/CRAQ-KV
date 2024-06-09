#!/bin/bash

usage() {
    echo "Usage: $0 --workload-dirs=dirs --dist-changes=number --output-dir=output --threads=number"
    exit 1
}

for i in "$@"; do
    case $i in
        --workload-dirs=*)
            WORKLOAD_DIRS="${i#*=}"
            shift
            ;;
        --dist-changes=*)
            DIST_CHANGES="${i#*=}"
            shift
            ;;
        --output-dir=*)
            OUTPUT_DIR="${i#*=}"
            shift
            ;;
        --threads=*)
            THREADS="${i#*=}"
            shift
            ;;
        *)
            usage
            ;;
    esac
done

if [ -z "$WORKLOAD_DIRS" ] || [ -z "$DIST_CHANGES" ] || [ -z "$OUTPUT_DIR" ]; then
    usage
fi

IFS=',' read -r -a DIR_ARRAY <<< "$WORKLOAD_DIRS"

mkdir -p "$OUTPUT_DIR"

for dir in "${DIR_ARRAY[@]}"; do
    for file in "$dir"/*; do
        file_name=$(basename "$file")
        if [[ $file_name == client*.txt ]]; then
            > "$OUTPUT_DIR/$file_name"
        fi
    done
done

for (( i=0; i<DIST_CHANGES; i++ )); do
    selected_dir="${DIR_ARRAY[RANDOM % ${#DIR_ARRAY[@]}]}"
    for file in "$selected_dir"/*; do
        file_name=$(basename "$file")
        if [[ $file_name == client*.txt ]]; then
            head -n 100000000 "$file" >> "$OUTPUT_DIR/$file_name" &
        fi
        if [[ $(jobs -r | wc -l) -ge $THREADS ]]; then
            wait -n
        fi
    done
    wait
done

echo "Mixed distribution files created in: $OUTPUT_DIR"