
helloWorldTest <- function(name) {
  SparkR:::callJStatic("com.Alteryx.sparkGLM.test",
                       "hello",
                       name)
}
