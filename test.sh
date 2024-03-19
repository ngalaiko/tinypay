#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

function run_test {
    TMP_FILE=$(mktemp)
    trap "rm -f $TMP_FILE" EXIT

    ./target/debug/tinypay "./tests/$1_transactions.csv" > $TMP_FILE

    diff -u "./tests/$1_accounts.csv" $TMP_FILE

    echo "[$1] OK"
}

cargo build

for file in $(ls ./tests/); do 
    FILE_PATH="./tests/$file"
    if [[ $file == *_transactions.csv ]]; then
        TEST_N="${file%"_transactions.csv"}"
        run_test $TEST_N
    fi
done
