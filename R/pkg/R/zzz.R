.onLoad <- function(libname, pkgname) {
  libDir <- find.package("sparkGLM")
  jarName <- list.files(libDir, "*.jar")
  sparkGLMJar <<- file.path(libDir, jarName)
  print("sparkGLM JAR path stored in `sparkGLMJar`")
}
