context("disksort")

unsorted_iris_file = "iris.txt"
sorted_iris_file = "sorted_iris.txt"
write.table(iris, unsorted_iris_file, col.names = FALSE, row.names = FALSE)
iris_sorted = iris[order(iris[, 1]), ]


test_that("all defaults", {

    disksort(unsorted_iris_file)
    actual = read.table(sorted_iris_file)
    expect_equivalent(iris_sorted, actual)
    unlink(sorted_iris_file)

})


test_that("passing in options", {

    disksort(unsorted_iris_file, breaks = c(5, 6), cleanup = TRUE)
    actual = read.table(sorted_iris_file)
    expect_equivalent(iris_sorted, actual)
    unlink(sorted_iris_file)

})


unlink(unsorted_iris_file)
