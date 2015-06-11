# sparkGLM
An R-like GLM package for Apache Spark.

##Building
Use `install.sh` to build a development version of sparkGLM. This will compile all of the Scala dependencies into one `JAR` and then build the R package on top of it.

##Running in Spark Shell
You'll need to use the `--jars` flag to include this package when you initialize a Spark shell. For example:
```bash
<SPARK HOME>/bin/sparkR \
--jars=/path/to/sparkGLM/pkg/src/target/scala-2.10/sparkGLM-assembly-<version>.jar \
--driver-class-path=/path/to/sparkGLM/pkg/src/target/scala-2.10/sparkGLM-assembly-0.1.jar
```
(note the use of `--driver-class-path` due to [SPARK_5185](https://issues.apache.org/jira/browse/SPARK-5185))

Once you're in R, import the package:
```R
library(sparkGLM, lib.loc="/path/to/sparkGLM/lib/")
```
