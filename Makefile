SHELL = /bin/bash

MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
FWDIR := $(patsubst %/,%,$(dir $(MAKEFILE_PATH)))
LIB_DIR := $(FWDIR)/R/lib
SCALA_VERSION := 2.10
SPARKGLM_VERSION := 0.0.1
JAR_NAME := sparkglm-assembly-$(SPARKGLM_VERSION).jar
SBT_TARGET_NAME := target/scala-$(SCALA_VERSION)/$(JAR_NAME)

SCALA_SOURCE_DIR := src/main/scala/com/Alteryx/sparkGLM
RESOURCE_DIR := src/main/resources

SCALA_FILES := $(wildcard $(SCALA_SOURCE_DIR)/*.scala)
RESOURCE_FILES := $(wildcard $(RESOURCE_DIR)/*)

install:	install-scala install-R

install-scala: build.sbt $(SCALA_FILES) $(RESOURCE_FILES)
	./build/sbt assembly

install-R:
	cp -f $(SBT_TARGET_NAME) R/pkg/inst/
	mkdir -p $(LIB_DIR)
	R CMD INSTALL --library=$(LIB_DIR) R/pkg/

clean:
	./build/sbt clean
	rm -rf target
	rm -rf project/target
	rm -rf project/project
	rm -rf R/lib
	-rm build/sbt-launch-*.jar
	rm -f R/pkg/inst/$(JAR_NAME)
	rm -f *.o
	rm -f *.so

.PHONY: clean install install-scala install-R
