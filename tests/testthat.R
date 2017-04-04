# Workaround:
# https://github.com/hadley/testthat/issues/86
Sys.setenv("R_TESTS" = "")

library(testthat)
library(partools)

test_check("partools")
