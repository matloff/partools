context("Utilities in SnowUtils")

cls <- makeCluster(2)
setclsinfo(cls)

az <- data.frame(letter = letters, number = seq_along(letters))
azaz <- rbind(az, az)

test_that("formrowchunks exports data", {

    formrowchunks(cls, az, "az_i", scramble = TRUE)
    chunks <- clusterEvalQ(cls, az_i)
    az2 <- do.call(rbind, chunks)
    az2 <- az2[order(az2$number), ]
    expect_equal(az, az2)

})


test_that("distrib* functions", {

    distribsplit(cls, "azaz")
    azback <- distribcat(cls, "azaz")
    expect_equal(azaz, azback)

    az2 <- distribagg(cls, ynames = "number", xnames = "letter"
                      , dataname = "azaz", FUN = "mean")

    expect_equal(az, az2)


})
