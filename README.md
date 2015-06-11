# sparkGLM
An R-like GLM package for Apache Spark.

## Requires
Spark 1.4.0
(note the use of `--driver-class-path` due to [SPARK_5185](https://issues.apache.org/jira/browse/SPARK-5185))

## Building
After cloning the project, go to the sparkGLM directory and enter
```
./build/sbt compile
```

## Running in the SBT REPL
In the sparkGLM directory enter the command
```
./build/sbt console
```
