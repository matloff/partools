### R code from vignette source 'Snowdoop.Rnw'

###################################################
### code chunk number 1: Snowdoop.Rnw:63-96
###################################################
# setclsinfo(), filechunkname(), addlists() from partools
library(partools)

# each node executes this function
wordcensus <- function(basename,ndigs) {
   # find the file chunk to be handled by this worker
   fname <- filechunkname(basename,ndigs)
   words <- scan(fname,what="")
   # determine which words occur how frequently in this chunk
   tapply(words,words,length, simplify=FALSE)
}

# manager
fullwordcount <- function(cls,basename,ndigs) {
   setclsinfo(cls)  # give workers ID numbers, etc.
   clusterEvalQ(cls,library(partools))
   # have each worker execute wordcensus()
   counts <- clusterCall(cls,wordcensus,basename,ndigs)
   # coalesce the output for the overall counts
   addlistssum <- function(lst1,lst2) addlists(lst1,lst2,sum)
   Reduce(addlistssum,counts)
}

test <- function() {
   cls <- makeCluster(2)
   # make a test file, 2 chunks
   cat("How much wood","Could a woodchuck chuck",file="test.1",sep="\n")
   cat("If a woodchuck could chuck wood?",file="test.2")
   # set up cluster
   cls <- makeCluster(2)
   # find the counts
   fullwordcount(cls,"test",1)
}


