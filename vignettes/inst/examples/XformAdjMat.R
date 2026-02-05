
# transforms a graph adjacency matrix from "wide" to "narrow" format, e.g.
# transforming
# 
# 0 1 0 0 
# 1 0 0 1 
# 0 1 0 1 
# 1 1 1 0 
# 
# to 
# 
# 1 2
# 2 1
# 2 4
# 3 2
# 3 4
# 4 1
# 4 2
# 4 3
# 
# for example, the 2,4 row in the output means that vertex 2 has an edge
# going to vertex 4, shown in the input matrix by a 1 in row 2, column 4

# assumptions:

#    the input matrix basename is assumed stored in distributed fashion,
#    in as many file chunks as there are nodes in the cluster cls, e.g.
#    basename.0001, basename.0002, etc. 

#    input files do not have a header

#    the input files have sequence numbers, e.g. they were created under
#    filesplit() with seqnums=TRUE

#    blanks separate entries within a row

#    setclsinfo() has already been called

xformadj <- function(cls,inbasename,outbasename) {
   ndigs <- ceiling(log10(length(cls)))
   clusterCall(cls,processchunk,inbasename,outbasename,ndigs)
}

# node reads in its chunk, outputs its chunk
processchunk <- function(inbasename,outbasename,ndigs) {
   fname <- filechunkname(inbasename,ndigs)
   indata <- read.table(fname,header=FALSE)
   outname <- filechunkname(outbasename,ndigs)
   conout <- file(outname,open="w")
   for (i in 1:nrow(indata)) {
      indatarow <- indata[i,]
      vertexnum <- indatarow[1]
      # locate the 1s, skipping the seq number, and shifting 
      # indices left accordingly
      ones <- which(indatarow != 0)[-1] - 1
      # form the output lines and write them
      outlines <- paste(vertexnum,ones)
      writeLines(outlines,conout)
   }
   close(conout)
}

