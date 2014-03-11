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
    difffile="$TMP/$base.diff"
    ./client < $t > $mine
    results=`diff $expected $mine`
    if [ -z "$results" ]
    then
        echo "[PASS] $t"
    else
        # try to compare the lines in different order
        cat $mine | sort > $mine.sorted
        cat $expected | sort > $TMP/$base.expected.sorted
        resultssorted=`diff $TMP/$base.expected.sorted $mine.sorted`
        if [ -z "$resultssorted" ]
        then
            echo "[PASS--Same lines in different order] $t"
        else
            echo "[FAIL] $t"
            echo "$results"
            echo "$results" > $difffile
        fi
    fi
done
