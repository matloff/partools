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

filesave(cls, "unsorted", newbasename = "unsorted", ndigs = 2, sep = "\t")

filesort(cls, infilenm = "unsorted", colnum = 1, outdfnm = "unsorted"
         , infiledst = TRUE, ndigs = 2, sep = ",", comment.char = "")


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


t1 = read.table("unsorted.01"
                , header = TRUE
                , sep = "\t"
                , quote = "\""
                , comment.char = ""
                #, fill = TRUE
                #, nrows = 10
                #, colClasses = rep("character", 3)
                )
