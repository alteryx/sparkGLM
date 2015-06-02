#!/bin/bash

FWDIR="$(cd `dirname $0`; pwd)"
LIB_DIR="$FWDIR/lib"

mkdir -p $LIB_DIR

R CMD INSTALL --library=$LIB_DIR pkg/
