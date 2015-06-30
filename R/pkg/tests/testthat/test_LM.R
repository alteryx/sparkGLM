library(testthat)
library(sparkGLM)

context("LM class and functions")

sc <- sparkR.init(sparkJars = "../../inst/sparkglm-assembly-0.0.1.jar")
sqlCtx <- sparkRSQL.init(sc)
df <- read.df(sqlCtx, "../test_support/linear_reg_mixed.json", "json")

test_that("lm with all numeric fields", {
  numDF <- select(df, "x1", "x2", "x3", "y")
  testModel <- sparkLM(y ~ x1 + x2 + x3, numDF)
  expect_is(testModel, "sparkLM")
  expect_equal(testModel$xnames, c("x1", "x2", "x3"))
  # Create a new sparkLM obj from the jobj
  testModel2 <- sparkLM(testModel$jobj)
  expect_is(testModel2, "sparkLM")

  predicted <- predict(testModel, select(numDF, "x1", "x2", "x3"))
  expect_is(predicted, "DataFrame")
  expect_equal(count(predicted), 1000)
})

test_that("lm with numeric and categorical fields", {
  mixedDF <- select(df,"x1", "x2", "x7", "y")
  testModel <- sparkLM(y ~ x1 + x2 + x7, mixedDF)
  expect_is(testModel, "sparkLM")
  expect_equal(length(testModel$xnames), 4)
  expect_equal(testModel$xnames, c("x1", "x2", "x7_b", "x7_c"))

  predicted <- predict(testModel, select(mixedDF, "x1", "x2", "x7"))
  expect_is(predicted, "DataFrame")
  expect_equal(count(predicted), 1000)
})
