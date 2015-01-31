
# suppose we have a file basename, stored in chunks, say basename.001,
# basename.002 etc.; this function determine the file name for the chunk
# to be handled by node nodenum; the latter is the ID for the executing
# node, partoolsenv$myid, set by setclsinfo()
filechunkname <- function (basename, ndigs,nodenum=NULL) 
{
    tmp <- basename
    if (is.null(nodenum)) {
       pte <- getpte()
       nodenum <- pte$myid
    }
    n0s <- ndigs - nchar(as.character(nodenum))
    zerostring <- paste(rep("0", n0s),sep="",collapse="")
    paste(basename, ".", zerostring, nodenum, sep = "") 
}


# distributed file sort on cls, based on column number colnum of input;
# file name from basename, ndigs; bucket sort, with categories
# determined by first sampling nsamp from each chunk; each node's output
# chunk written to file outname (plus suffix based on node number) in
# the node's global space

filesort <- function(cls,basename,ndigs,colnum,
      outname,nsamp=1000,header=FALSE,sep="") 
{
   clusterEvalQ(cls,library(partools))
   setclsinfo(cls)
   samps <- clusterCall(cls,getsample,basename,ndigs,colnum,
      header=header,sep=sep,nsamp) 
   samp <- Reduce(c,samps)
   bds <- getbounds(samp,length(cls))
   clusterApply(cls,bds,mysortedchunk,
      basename,ndigs,colnum,outname,header,sep)
   0
}

getsample <- function(basename,ndigs,colnum,
      header=FALSE,sep="",nsamp) 
{
   fname <- filechunkname(basename,ndigs)
   read.table(fname,nrows=nsamp,header=header,sep=sep)[,colnum]
}

getbounds <- function(samp,numnodes) {
   bds <- list()
   q <- quantile(samp,((2:numnodes) - 1) / numnodes)
   samp <- sort(samp)
   for (i in 1:numnodes) {
      mylo <- if (i > 1) q[i-1] else NA
      myhi <- if (i < numnodes) q[i] else NA
      bds[[i]] <- c(mylo,myhi)
   }
   bds
}

mysortedchunk <- function(mybds,basename,ndigs,colnum,outname,header,sep) {
   pte <- getpte()
   me <- pte$myid
   ncls <- pte$ncls
   mylo <- mybds[1]
   myhi <- mybds[2]
   for (i in 1:ncls) {
      tmp <-
         read.table(filechunkname(basename,ndigs,i),header=header,sep)
      tmpcol <- tmp[,colnum]
      if (me == 1) {
         tmp <- tmp[tmpcol <= myhi,] 
      } else if (me == ncls) {
          tmp <- tmp[tmpcol > mylo,]
      } else {
         tmp <- tmp[tmpcol > mylo & tmpcol <= myhi,] 
      }
      mychunk <- if (i == 1) tmp else rbind(mychunk,tmp)
   }
   sortedmchunk <- mychunk[order(mychunk[,colnum]),]
   assign(outname,sortedmchunk,envir=.GlobalEnv)
}

# split a file into chunks, one per cluster node

filesplit <- function(cls,basename,header=FALSE) {
   cmdout <- system(paste("wc -l",basename),intern=TRUE)
   tmp <- strsplit(cmdout[[1]][1], " ")[[1]]
   nlines <- as.integer(tmp[length(tmp) - 1])
   con <- file(basename,open="r")
   if (header) {
      hdr <- readLines(con,1)
      nlines <- nlines - 1
   }
   lcls <- length(cls)
   ndigs <- ceiling(log10(lcls))
   chunks <- clusterSplit(cls,1:nlines)
   chunksizes <- sapply(chunks,length)
   for (i in 1:lcls) {
      chunk <- readLines(con,chunksizes[i])
      fn <- filechunkname(basename,ndigs,i)
      conout <- file(fn,open="w")
      if (header) writeLines(hdr,conout)
      writeLines(chunk,conout)
      close(conout)
   }
}
