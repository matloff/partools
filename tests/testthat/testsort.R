context("File sort")

fname = tempfile()

unsorted = data.frame(c(3, 1, 2), c("three", "one", "two"))
sorted = unsorted[order(unsorted[, 1]), ]

write.table(unsorted, fname, row.names = FALSE, col.names = FALSE)


test_that("Basic file sort", {

    filesort(cls
             , infilenm = fname
             , colnum = 1
             , outdfnm = "sorted"
             )

    sorted_partools <- distribcat(cls, "sorted")

    expect_equivalent(sorted, sorted_partools)

    #sorted_from_file = read.table("sorted.txt")

})


unlink(fname)
