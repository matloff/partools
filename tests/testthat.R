# Workaround:
# https://github.com/hadley/testthat/issues/86
Sys.setenv("R_TESTS" = "")

library(testthat)
library(partools)

cls <- makeCluster(2)
setclsinfo(cls)

test_check("partools")

parallel::stopCluster(cls)
