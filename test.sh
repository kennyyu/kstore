#!/bin/bash

if [ -z "$1" ]
then
    echo "Usage: ./test.sh testdir    [server must be running]"
    exit 1
fi

make
TMP="tmp.$RANDOM"
mkdir -p $TMP
echo "storing output in $TMP/"

testdir=$1
tests=`ls "$testdir"/*.txt | sort`
for t in $tests
do
    base=`basename $t .txt`
    expected="$testdir/$base.expected"
    mine="$TMP/$base.me"
    ./client < $t > $mine
    results=`diff $expected $mine`
    if [ -z "$results" ]
    then
        echo "[PASS] $t"
    else
        echo "[FAIL] $t"
        echo "       $results"
    fi
done
