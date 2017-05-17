library(partools)
cls <- makeCluster(2)
setclsinfo(cls)


unsorted <- read.fwf("unsorted.txt", widths = c(10, -2, 32, -2, 52)
                     , comment.char = "", n = 100)
unsorted[, 2] <- as.integer(paste0("0x", unsorted[, 2]))

# Regular R takes about 115 ms to do 1 million records
local_sorted <- unsorted[order(unsorted[, 1]), ]

distribsplit(cls, "unsorted")

filesave(cls, "unsorted", newbasename = "unsorted", ndigs = 2, sep = "\t", row.names = FALSE)

filesort(cls, infilenm = "unsorted", colnum = 1, outdfnm = "sorted"
         , infiledst = TRUE, ndigs = 2, sep = "\t", comment.char = "")

dist_sorted <- distribcat(cls, "sorted")

write.table(unsorted[1:10, ]
            , "little.txt"
            , sep = "\t"
            , row.names = FALSE
            , qmethod = "double"
            )


t2 = read.table("little.txt"
                , header = TRUE
                , sep = "\t"
                #, quote = "\""
                , comment.char = ""
                #, fill = TRUE
                #, nrows = 10
                #, colClasses = rep("character", 3)
                )

# TODO: Clark- come back and debug this, and then put this case into a
# test.
# Tue May 16 17:01:18 PDT 2017
t1 = read.table("unsorted.01"
                , header = TRUE
                , sep = "\t"
                #, quote = "\""
                , comment.char = ""
                #, fill = TRUE
                #, nrows = 10
                #, colClasses = rep("character", 3)
                )
