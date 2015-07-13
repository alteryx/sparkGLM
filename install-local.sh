#!/bin/bash
SCALA_VERSION="2.10"
SPARKGLM_VERSION="0.0.1"
JAR_NAME="sparkglm-assembly-$SPARKGLM_VERSION.jar"
SBT_TARGET_NAME="target/scala-$SCALA_VERSION/$JAR_NAME"

FWDIR="$(cd `dirname $0`; pwd)"
LIB_DIR="$FWDIR/R/lib"

mkdir -p $LIB_DIR

INSTALLED=$(Rscript ./checkInstall.R)

if [ $INSTALLED == TRUE ]; then
  echo "sparkGLM is already installed"
else
  echo "Installing sparkGLM"
  ./build/sbt assembly
  cp -f $SBT_TARGET_NAME $FWDIR/R/pkg/inst/
  R CMD INSTALL --library=$LIB_DIR R/pkg/
fi
