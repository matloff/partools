library(partools)

context("Cluster setup")

test_that("Defaults", {
    cls <- makeCluster(2)
    setclsinfo(cls)

    # Verifying testthat runs this as expected.
    expect_true(TRUE)
})
