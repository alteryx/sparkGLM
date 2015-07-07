# LM.R - Linear Model class and methods for SparkR

#' @include generics.R utils.R
NULL

#' @title sparkLM
#' @description sparkLM is used to fit linear models in SparkR and is influenced heavily by R's `lm'
#' function. It works seamlessly with SparkR DataFrames and returns an S3 object of class "sparkLM".
#' @rdname sparkLM
#' 
#' @param formula An R formula (e.g. y ~ x1 + x2) specifying the parameters of your model.
#' @param df A SparkR DataFrame containing your target field as well as any predictors you plan to
#' include in the model.
#' @param omitNAs A logical vector indicating whether you want to omit rows which contain null values
#' from the DataFrame prior to model estimation.
#' @export
#' @example
#' \dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' irisDF <- createDataFrame(iris)
#' theModel <- sparkLM(Sepal.Length ~ Petal.Length + Petal.Width + Species)
#' }
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

#' @rdname sparkLM
#' @export
#' 
#' @param jobj A SparkR Java object
#' @call The call used when constructing the model. If a call is included, it will be added to the
#' resulting model object.
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

#' @title Predict
#' @description Compute predicted values based on a model estimated with the `sparkLM` function.
#' @rdname sparkLM
#' 
#' @param model A `sparkLM` model object
#' @param newData A SparkR DataFrame containing the observations you want to compute predictions for.
#' @export
#' @example
#' \dontrun{
#' class(theModel)
#' > "sparkLM"
#' predictedValues <- predict(theModel, newData)
#' }
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

#' @title SummaryObj
#' @description Obtain the summary output from a `sparkLM` model and return it as an object.
#' @rdname sparkLM
#' 
#' @param model A `sparkLM` model object
#' @export
#' @example
#' \dontrun{
#' modelSummary <- summaryObj(sparkLMModel)
#' modelSummary$formula
#' > Model:
#' > y ~ x1 + x2 + x3
#' }
summaryObj.sparkLM <- function(model) {
  if (class(model) != "sparkLM") {
    stop("model must be a sparkLM model object.")
  }
  rawSummary <- SparkR:::callJStatic("com.Alteryx.sparkGLM.LM",
                                     "summaryArray",
                                     model$jobj)
  summaryOut <- list()
  summaryOut$model <- rawSummary[[1]]
  summaryOut$coefficients <- rawSummary[[2]]
  summaryOut$RSE <- rawSummary[[3]]
  summaryOut$R2 <- rawSummary[[4]]
  summaryOut$Fstat <- rawSummary[[5]]
  summaryOut$printSummary <- function() {
    cat("\nModel:\n")
    cat(summaryOut$model)
    cat("\n")
    cat("\nCoefficients:\n")
    cat(summaryOut$coefficients, "\n")
    cat("\n")
    for(i in c("RSE", "R2", "Fstat")) {
      cat(summaryOut[[i]], "\n")
    }
  }
  summaryOut
}

#' @title Summary
#' @description Print a summary of a `sparkLM` model object
#' @rdname sparkLM
#' 
#' @param model A `sparkLM` model object
#' @export
summary.sparkLM <- function(model) {
  summary <- summaryObj(model)
  summary$printSummary()
}
