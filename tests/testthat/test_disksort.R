library(partools)

context("disksort")

fname_unsorted = "4132.txt"
lines_unsorted = "4 four
1 one
3 three
2 two"

lines_sorted = "1 one
2 two
3 three
4 four"

writeChar(lines_unsorted, fname_unsorted)

unsorted_iris_file = "iris.txt"
write.table(iris, unsorted_iris_file, col.names = FALSE, row.names = FALSE)
iris_sorted = iris[order(iris[, 1]), ]


test_that("all defaults", {

    disksort(fname_unsorted)
    fname_sorted = paste0("sorted_", fname_unsorted)
    actual = readChar(fname_sorted, nchars = 1000L)

    expect_equal(lines_sorted, actual)
    unlink(fname_sorted)

})


test_that("iris data", {

    sorted_iris_file = "sorted_iris.txt"
    #options(stringsAsFactors = FALSE)
    #options("stringsAsFactors") 
    disksort(unsorted_iris_file, breaks = c(5, 6), cleanup = TRUE)
    actual = read.table(sorted_iris_file)
    expect_equal(iris_sorted, actual)
    unlink(sorted_iris_file)

})


unlink(fname_unsorted)
unlink(unsorted_iris_file)
