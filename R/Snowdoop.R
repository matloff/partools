
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
      outname,nsamp=1000,header=FALSE,sep=" ") 
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

# useful if need data in random order but the distributed file is not in
# random order
#
# read distributed file, producing scrambled, distributed chunks in
# memory at the cluster nodes, i.e. line i of the original distributed
# file has probability 1/nch of ending up at any of the in-memory
# chunks;  chunk sizes may not exactly equal across nodes, even if nch
# evenly divides the total number of lines in the distributed file; sep
# is the file field delineator, e.g. space of comma
readnscramble <- function(cls,basename,header=FALSE,sep= " ") {
   nch <- length(cls)
   ndigs <- getnumdigs(nch)
   linecounts <- vector(length=nch)
   # get file names
   fns <- sapply(1:nch,function(i) 
      filechunkname(basename,ndigs,nodenum=i))
   linecounts <- sapply(1:nch,function(i)
      linecount(fns[i]))
   cums <- cumsum(linecounts)
   clusterExport(cls,c("linecounts","cums","fns","nch","header","sep"),
      envir=environment())
   totrecs <- cums[nch]
   # give the nodes their index assignments
   idxs <- sample(1:totrecs,totrecs,replace=FALSE)
   clusterApply(cls,idxs,readmypart)
}

readmypart <- function(myidxs) {
   mydf <- NULL
   for (i in 1:nch) {
      filechunk <- read.table(fns[i],header=header,sep=sep)
      # which ones are mine?
      tmp <- myidxs
      if (i > 1) tmp <- myidxs - cums[i-1]
      tmp <- tmp[tmp <= linecounts[i]]
      mydf <- rbind(mydf,filechunk[tmp,])
   }
   assign(basename,mydf,envir=.GlobalEnv)
}

# split a file basename into nch approximately equal-sized chunks, with
# suffix being chunk number; e.g. if nch = 16, then basename.01,
# basename.02,..., basename.16; header, if any, is retained in the
# chunks; optionally, each output line can be preceded by a sequence
# number, in order to preserve the original ordering; if scramble is
# set, 

filesplit <- function(nch,basename,
      header=FALSE,seqnums=FALSE,scramble=FALSE) {
   nlines <- linecount(basename)
   con <- file(basename,open="r")
   if (header) {
      hdr <- readLines(con,1)
      nlines <- nlines - 1
   }
   # ndigs <- ceiling(log10(nch))
   ndigs <- getnumdigs(nch)
   chunks <- splitIndices(nlines,nch)
   chunksizes <- sapply(chunks,length)
   if (seqnums) cumulsizes <- cumsum(chunksizes)
   for (i in 1:nch) {
      chunk <- readLines(con,chunksizes[i])
      fn <- filechunkname(basename,ndigs,i)
      conout <- file(fn,open="w")
      if (header) writeLines(hdr,conout)
      if (seqnums) {
         if (i == 1) {
            seqrange <- 1:chunksizes[1]
         } else 
            seqrange <- (cumulsizes[i-1]+1):cumulsizes[i]
         chunk <- paste(seqrange,chunk)
      }
      writeLines(chunk,conout)
      close(conout)
   }
}

# get the number of lines in the file 
#
# arguments:
#
# infile: quoted file namee
# chunksize: number of lines to read in one chunk; if -1, then read the
#            file in one fell swoop
#
# the file is read in one chunk at a time, in order to avoid
# overwhelming memory
linecount <- function(infile,chunksize=100000) {
   # could check for Unix first (Mac, Linux, cygwin), running the more
   # efficient 'wc -l' in that case
   con <- file(infile,"r")
   nlines <- 0
   repeat {
      tmp <- readLines(con,chunksize)
      nread <- length(tmp)
      if (nread == 0) return(nlines)
      nlines <- nlines + nread
   }
}

filecat <- function (cls, basename, header = FALSE)  {
    lcls <- length(cls)
    # ndigs <- ceiling(log10(lcls))
    ndigs <- getnumdigs(lcls)
    if (file.exists(basename)) file.remove(basename)
    conout <- file(basename, open = "w")
    for (i in 1:lcls) {
       # read in the entire file chunk
       fn <- filechunkname(basename, ndigs, i)
       conin <- file(fn, open = "r")
       lines <- readLines(conin,-1)
       if (header && i > 1) {
          lines <- lines[-1]
       }
       writeLines(lines, conout)
    }
    close(conout)
}

# find the number of digits needed for suffixes for nch chunks
getnumdigs <- function(nch) {
   # floor(log10(nch)) + 1
   nchar(as.character(nch))
}
