##
## Helper functions
##

# Parse an R formula so it can be incorporated into a SparkSQL query to get the
# target and relevant predictors and to determine if the model should include an
# intercept. This is not yet provide all features of an R formula
parseFormula <- function(formula) {
  if (class(formula) != "formula") {
    stop("The provided argument is not a formula.")
  }
  formula.parts <- as.character(formula)
  preds <- unlist(strsplit(unlist(strsplit(formula.parts[3], " \\+ ")), " \\- "))
  intercept <- if (any(preds %in% c("1", "-1"))) {
    0L
  } else {
    1L
  }
  preds <- preds[!(preds %in% c("1", "-1"))]
  vars <- c(formula.parts[2], preds)
  list(target = vars[[1]], predictors = as.list(vars[2:length(vars)]), intercept = intercept)
}

omitNA <- function(df) {
  sdf <- SparkR:::callJMethod(df@sdf, "drop", "any")
  SparkR:::dataFrame(sdf)
}

modelMatrix <- function(df) {
  sdf <- SparkR:::callJStatic("com.Alteryx.sparkGLM.modelMatrix",
                              "apply",
                              df@sdf)
  SparkR:::dataFrame(sdf)
}

matchCols <- function(modelObj, df) {
  if (!is.null(modelObj$xnames)) {
    sdf <- SparkR:::callJStatic("com.Alteryx.sparkGLM.utils",
                                "matchCols",
                                as.list(modelObj$xnames),
                                df@sdf)
    SparkR:::dataFrame(sdf)
  } else {
    stop("The model object must contain the `xnames` attribute.")
  }
}

formulaToString <- function(formula) {
  stringForm <- as.character(formula)
  paste(stringForm[2], stringForm[1], stringForm[3])
}
