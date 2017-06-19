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


test_that("all defaults", {

    disksort(fname_unsorted)
    fname_sorted = paste0("sorted_", fname_unsorted)
    actual = readChar(fname_sorted, nchars = 1000L)

    expect_equal(lines_sorted, actual)
    unlink(fname_sorted)

})


unlink(fname_unsorted)


# Interactive testing

library(partools)
fname = "iris.txt"

options(stringsAsFactors = FALSE)
options("stringsAsFactors") 

write.table(iris, fname, col.names = FALSE, row.names = FALSE)

disksort(fname, breaks = c(5, 6), cleanup = TRUE)
