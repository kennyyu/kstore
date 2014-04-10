#!/bin/bash

if [ -z "$1" ]
then
    echo "Usage: ./test_client.sh {hash | tree | sort | loop}"
    exit 1
fi

case "$1" in
    hash | tree | sort | loop)
        time ../../client --loaddir=. < perftest_"$1".txt
        ;;
    *)
        echo "Usage: ./test_client.sh {hash | tree | sort | loop}"
        exit 1
esac
