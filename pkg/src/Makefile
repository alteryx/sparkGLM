SCALA_VERSON := 2.10
SPARKGLM_VERSION := 0.1
JAR_NAME := sparkGLM-assembly-$(SPARKGLM_VERSION).jar
SBT_TARGET_NAME := target/scala-$(SCALA_VERSON)/$(JAR_NAME)

SCALA_SOURCE_DIR := src/main/scala/com/Alteryx/sparkGLM
RESOURCE_DIR := src/main/resources

SCALA_FILES := $(wildcard $(SCALA_SOURCE_DIR)/*.scala)
RESOURCE_FILES := $(wildcard $(RESOURCE_DIR)/*)

$(SBT_TARGET_NAME): build.sbt $(SCALA_FILES) 
	./sbt/sbt assembly
	cp -f $(SBT_TARGET_NAME) ../inst/

clean:
	sbt clean
	rm -rf target
	rm -rf project/target
	rm -rf project/project
	rm sbt/sbt-launch-*.jar
	rm -f ../inst/$(JAR_NAME)
	rm -f *.o
	rm -f *.so

.PHONY: all clean
