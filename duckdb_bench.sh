#!/bin/bash

S3_PATH="s3://tomas-tpch-data/duckdb-tpch-100/lineitem.parquet"
PROFILE_LOG="duckdb_sf100_q01_profile.txt"
RUNS=5 
TOTAL_ACCUMULATED_TIME=0

echo "Starting TPC-H Q1 Benchmark against $S3_PATH ($RUNS runs)..."

for (( i=1; i<=$RUNS; i++ ))
do
    echo "Run $i/$RUNS..."
    
    duckdb <<EOF
INSTALL httpfs;
LOAD httpfs;

PRAGMA threads=64;
PRAGMA memory_limit='110GB';

CREATE SECRET s3_creds (
    TYPE S3,
    PROVIDER CREDENTIAL_CHAIN,
    REGION 'eu-north-1'
);

.timer on
PRAGMA enable_profiling;
PRAGMA profiling_output = '$PROFILE_LOG';

-- Run TPC-H Query 1
SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) AS sum_qty,
    sum(l_extendedprice) AS sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    avg(l_quantity) AS avg_qty,
    avg(l_extendedprice) AS avg_price,
    avg(l_discount) AS avg_disc,
    count(*) AS count_order
FROM
    read_parquet('$S3_PATH')
WHERE
    l_shipdate <= CAST('1998-09-02' AS DATE)
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus;
EOF

    CURRENT_TIME=$(grep "Total Time:" "$PROFILE_LOG" | sed -E 's/.*Total Time: ([0-9.]+)s.*/\1/')
    
    echo "Run $i Duration: ${CURRENT_TIME}s"
    
    TOTAL_ACCUMULATED_TIME=$(echo "$TOTAL_ACCUMULATED_TIME + $CURRENT_TIME" | bc)
done

AVERAGE_TIME=$(echo "scale=2; $TOTAL_ACCUMULATED_TIME / $RUNS" | bc)

echo "---------------------------------------"
echo "Benchmark complete."
echo "Total Accumulated Time: ${TOTAL_ACCUMULATED_TIME}s"
echo "Average Runtime over $RUNS runs: ${AVERAGE_TIME}s"