context("Utilities in SnowUtils")

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

    # Not sure how to set the variable name on the aggregated version,
    # which is currently x
    expect_equivalent(az, az2)

})


test_that("Creating a new column in a data frame", {

    distribsplit(cls, "az")

    az_expected <- az
    az_expected$LETTER <- toupper(az_expected$letter)

    clusterEvalQ(cls, {
        az$LETTER <- toupper(az$letter)
        NULL
    })

    az_actual <- distribcat(cls, "az")

    expect_equal(az_expected, az_actual)

})
