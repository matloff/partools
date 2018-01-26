
# routines to load and prep common datasets

# prgeng data

getPE <- function() 
{
   data(prgeng)
   pe <- prgeng[,c(1,3,7:9)]
   # dummies for MS, PhD
   pe$ms <- as.integer(pe$educ == 14)
   pe$phd <- as.integer(pe$educ == 16)
   pe$educ <- NULL
   pe <<- pe
}

