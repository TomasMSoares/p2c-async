#!/usr/bin/env bash

# Parameters
sf=${1:-"100"}
rows_per_file=${2:-"50000"}
s3_mount_dir=${3:-"s3-data"}/parquet_sf_${sf}_new
compression_type=${4:-"zstd"}

local_dir="local_tmp_sf_${sf}"
dbgen_chunks=$sf 

mkdir -p "$local_dir" "$s3_mount_dir"

( cd tpch-dbgen || exit 1; test -f dbgen || ( git submodule update --init .; make ) ) || exit $?
make parquet-generator

TABLES=("nation" "region" "supplier" "part" "partsupp" "customer" "orders" "lineitem")

declare -A table_idx

for table in "${TABLES[@]}"; do
    mkdir -p "$s3_mount_dir/$table"
    table_idx[$table]=0
done

echo "Starting Dynamic Row Generation | SF: $sf | Rows/File: $rows_per_file"

for (( i=1; i<=dbgen_chunks; i++ )); do
    echo "=== Processing Memory Chunk $i of $dbgen_chunks ==="
    
    ( cd tpch-dbgen || exit 1; ./dbgen -f -s "$sf" -C "$dbgen_chunks" -S "$i" > /dev/null 2>&1
      mv -f ./*.tbl* "../$local_dir/" 2>/dev/null || true )

    for table in "${TABLES[@]}"; do
        [ "$dbgen_chunks" -eq 1 ] && tbl_file="${local_dir}/${table}.tbl" || tbl_file="${local_dir}/${table}.tbl.${i}"
        
        if [[ "$table" == "nation" || "$table" == "region" ]]; then
            [ "$i" -eq 1 ] && tbl_file="${local_dir}/${table}.tbl" || continue
        fi

        [ ! -f "$tbl_file" ] && continue

        echo "--- Slicing Table: $table (Current global index: ${table_idx[$table]}) ---"
        
        ./parquet-generator "$tbl_file" "$local_dir" "$table" "${table_idx[$table]}" "$rows_per_file" "$compression_type"
        
        if [ $? -ne 0 ]; then
            echo "Error processing $table. Exiting."
            exit 1
        fi
        
        # Find generated parquet files
        generated_files=($(ls "${local_dir}/${table}/${table}_"*.parquet 2>/dev/null))
        num_generated=${#generated_files[@]}
        
        if [ "$num_generated" -gt 0 ]; then
            table_idx[$table]=$((table_idx[$table] + num_generated))
            
            cp "${local_dir}/${table}/${table}_"*.parquet "$s3_mount_dir/${table}/"
            
            rm -f "${local_dir}/${table}/${table}_"*.parquet "$tbl_file"
        fi
    done
done

echo "=== Uploading Metadata to S3 ==="
for table in "${TABLES[@]}"; do
    if [ -f "${local_dir}/${table}/${table}_metadata.json" ]; then
        cp "${local_dir}/${table}/${table}_metadata.json" "$s3_mount_dir/${table}/"
    fi
done

echo "=== Generating Global Schema JSON ==="
./parquet-generator "schema" "$local_dir" "schema" "0" "0" "$compression_type"
cp "${local_dir}/tpch_schema.json" "$s3_mount_dir/"

rm -rf "$local_dir"
echo "✓ Data generation complete!"