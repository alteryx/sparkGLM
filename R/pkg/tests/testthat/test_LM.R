library(testthat)
library(sparkGLM)

context("LM class and functions")

sc <- sparkR.init(sparkJars = "../../inst/sparkglm-assembly-0.0.1.jar")
sqlCtx <- sparkRSQL.init(sc)
irisDF <- createDataFrame(sqlCtx, iris)

model <- sparkLM(Sepal_Width ~ Petal_Length + Petal_Width + Species, irisDF)

test_that("lm with all numeric fields", {
  testModel <- sparkLM(Sepal_Width ~ Petal_Length + Petal_Width, irisDF)
  expect_is(testModel, "sparkLM")
  expect_equal(testModel$xnames, c("Petal_Length", "Petal_Width"))
  # Create a new sparkLM obj from the jobj
  testModel2 <- sparkLM(testModel$jobj)
  expect_is(testModel2, "sparkLM")
  expect_equal(testModel2$xnames, c("Petal_Length", "Petal_Width"))

  predicted <- predict(testModel, select(irisDF, "Petal_Length", "Petal_Width"))
  expect_is(predicted, "DataFrame")
  expect_equal(count(predicted), 150)
})

test_that("lm with numeric and categorical fields", {
  testModel <- sparkLM(Sepal_Width ~ Petal_Length + Petal_Width + Species, irisDF)
  expect_is(testModel, "sparkLM")
  expect_equal(length(testModel$xnames), 4)
  expect_equal(testModel$xnames, c("Petal_Length", "Petal_Width", "Species_versicolor", "Species_virginica"))

  predicted <- predict(testModel, select(irisDF, "Petal_Length", "Petal_Width", "Species"))
  expect_is(predicted, "DataFrame")
  expect_equal(count(predicted), 150)
})
