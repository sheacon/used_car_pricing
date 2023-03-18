#!/bin/bash

for ((i=1; i<=100; i++))
do
    mkdir -p subdirectory_$i
done

i=1
for file in *.parquet
do
    subdir=subdirectory_$((i % 100 + 1))
    mv "$file" "$subdir/"
    i=$((i + 1))
done
