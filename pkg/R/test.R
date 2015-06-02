
helloWorldTest <- function(name) {
  var <- SparkR:::callJStatic("com.Alteryx.sparkGLM.test",
                       "hello",
                       name)
  print(var)
}
