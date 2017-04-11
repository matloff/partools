context("Utilities in SnowUtils")

cls <- makeCluster(2)
setclsinfo(cls)

az <- data.frame(letter = letters, number = seq_along(letters))

test_that("formrowchunks exports data", {

    formrowchunks(cls, az, "az_i", scramble = TRUE)
    chunks <- clusterEvalQ(cls, az_i)
    az2 <- do.call(rbind, chunks)
    az2 <- az2[order(az2$number), ]
    expect_equal(az, az2)

})
