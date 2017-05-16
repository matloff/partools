library(partools)
cls <- makeCluster(2)
setclsinfo(cls)


unsorted <- read.fwf("unsorted.txt", widths = c(10, -2, 32, -2, 52), comment.char = "")

# Will this make any difference?
#                     , stringsAsFactors = FALSE)

unsorted[, 2] <- as.integer(paste0("0x", unsorted[, 2]))

# Regular R takes about 115 ms to do 1 million records
sorted <- unsorted[order(unsorted[, 1]), ]


distribsplit(cls, "unsorted")
