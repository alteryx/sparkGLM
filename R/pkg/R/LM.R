
sparkLM <- function(x, ...) {
  UseMethod("sparkLM", x)
}

sparkLM.formula <- function(formula, df, omitNAs = TRUE) {
  if (class(formula) != "formula") {
    stop("The provided formula is not a formula object.")
  }
  if (class(df) != "DataFrame") {
    stop("The data must be a Spark DataFrame.")
  }
  pf <- parseFormula(formula)
  args <- as.list(match.call())
  theCall <- paste("sparkLM(formula = ", formulaToString(formula), ", df = ", args$df, ")", sep = "")
  if (omitNAs) {
    df <- omitNA(df)
  }
  targetDF <- modelMatrix(select(df, pf$target))
  predDF <- modelMatrix(select(df, pf$predictors))
  theModelObj <- SparkR:::callJStatic("com.Alteryx.sparkGLM.LM",
                                   "fit",
                                   predDF@sdf,
                                   targetDF@sdf)
  sparkLM(theModelObj, theCall)
}

sparkLM.jobj <- function(jobj, call = NULL) {
  theModel <- structure(list(), class = "sparkLM")
  theModel$jobj <- jobj
  if (!is.null(call)) {
    theModel$call <- call
  }
  theModel$xnames <- as.character(SparkR:::callJMethod(jobj, "xnames"))
  theModel$coefficients <- {
    coefs <- as.numeric(SparkR:::callJMethod(
      SparkR:::callJMethod(jobj, "coefs"),
      "toArray"))
    names(coefs) <- theModel$xnames
    coefs
  }
  theModel$residuals <- {
    resids <- as.numeric(SparkR:::callJMethod(jobj, "stdErr"))
    names(resids) <- theModel$xnames
    resids
  }
  theModel
}

predict.sparkLM <- function(model, newData) {
  if (class(newData) != "DataFrame") {
    stop("newData must be a Spark DataFrame.")
  }
  if (class(model) != "sparkLM") {
    stop("The model must be a sparkLM model object.")
  }
  scoreDF <- matchCols(model, modelMatrix(newData))
  sdf <- SparkR:::callJStatic("com.Alteryx.sparkGLM.LM",
                              "predict",
                              model$jobj,
                              scoreDF@sdf)
  SparkR:::dataFrame(sdf)
}

summary.sparkLM <- function(model) {
  rawSummary <- SparkR:::callJStatic("com.Alteryx.sparkGLM.LM",
                                     "summaryArray",
                                     model$jobj)
  cat("\nModel:\n")
  cat(rawSummary[[1]])
  cat("\n")
  cat("\nCoefficients:\n")
  cat(rawSummary[[2]], "\n")
  cat("\n")
  for(i in 3:5) {
    cat(rawSummary[[i]], "\n")
  }
}
