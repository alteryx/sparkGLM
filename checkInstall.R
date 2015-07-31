#!/usr/bin/env Rscript

checkInstall <- function() {
  if ("sparkGLM" %in% installed.packages()) {
    res <- TRUE
  } else {
    res <- FALSE
  }
  cat(res); invisible(res)
}
checkInstall()
