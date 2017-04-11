context("Utilities in SnowUtils")

cls <- makeCluster(2)
setclsinfo(cls)

az <- data.frame(letter = letters, number = seq_along(letters))

test_that("formrowchunks will export", {

    formrowchunks(cls, az, "az_i")

    variables <- clusterEvalQ(cls, ls())
    azin <- sapply(variables, function(x) "az_i" %in% x)
    expect_true(all(azin))

    chunks <- clusterEvalQ(cls, az_i)

})
