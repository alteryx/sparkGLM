#!/bin/bash

FWDIR="$(cd `dirname $0`; pwd)"

FAILED=0
rm -f unit-tests.log

cd $FWDIR/R/pkg/tests
Rscript run-all.R | tee -a $FWDIR/unit-tests.log
FAILED=$((PIPESTATUS[0]||$FAILED))

if [[ $FAILED != 0 ]]; then
    echo -en "\033[31m"  # Red
    echo "Had test failures; see logs."
    echo -en "\033[0m"  # No color
    exit -1
else
    echo -en "\033[32m"  # Green
    echo "Tests passed."
    echo -en "\033[0m"  # No color
fi
