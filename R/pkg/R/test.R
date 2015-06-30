
helloWorldTest <- function(name) {
  SparkR:::callJStatic("com.Alteryx.sparkGLM.hello",
                       "helloWorld",
                       name)
}
