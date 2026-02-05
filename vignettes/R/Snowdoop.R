
# dealing with Snowdoop distributed files

# suppose we have a file basenm, stored in chunks, say basenm.001,
# basenm.002 etc.; this function determine the file name for the chunk
# to be handled by node nodenum; the latter is the ID for the executing
# node, partoolsenv$myid, set by setclsinfo()
filechunkname <- function (basenm, ndigs,nodenum=NULL) 
{
    tmp <- basenm
    if (is.null(nodenum)) {
       pte <- getpte()
       nodenum <- pte$myid
    }
    n0s <- ndigs - nchar(as.character(nodenum))
    zerostring <- paste(rep("0", n0s),sep="",collapse="")
    paste(basenm, ".", zerostring, nodenum, sep = "") 
}

# filesort():
#    distributed file sort; sorts an input file, whether distributed or
#    ordinary, writing output to a distributed data frame 

# approach: bucket sort, with bins determined by a preliminary pass of
# the first nsamp records in the input file

# arguments:

#    cls: a 'parallel' cluster
#    infilenm: name of input file (without suffix, if distributed)
#    colnum: index of the column to be sorted
#    outdfnm: name of output distributed data frame 
#    infiledst: if TRUE, infilenm is distributed
#    ndigs: number of digits in suffix of distributed files
#    nsamp: bins formed by sampling the first nsamp records of infilenm
#    header: infilenm has a header
#    sep: sep character between fields in infilenm
#    usefread: use fread()

filesort <- function(cls,infilenm,colnum,outdfnm,
   infiledst=FALSE,ndigs=0,nsamp=1000,header=FALSE,sep=" ",usefread=FALSE) 
{  clusterExport(cls,
      c('getbounds','getsample','makemysortedchunk','getmypart'),
      envir=environment())
   # find the bins
   bds <- getbounds(cls,infilenm,infiledst,colnum,ndigs,header,sep,nsamp) 
   # at each node, cull out the records in infilenm for that node's bin,
   # and sort them at the node
   invisible(
   clusterApply(cls,bds,makemysortedchunk,
      infilenm,ndigs,colnum,outdfnm,infiledst,header,sep,usefread)
   )
}

# does most of the work for filesort(), for the given node; reads in
# data from disk, flagging records for this node's bin; finally writes
# the assembled data frame to global space of this node
makemysortedchunk <- function(mybds,infilenm,ndigs,colnum,outdfnm,
                    infiledst,header,sep,usefread) {
   pte <- getpte()
   me <- pte$myid
   ncls <- pte$ncls
   mylo <- mybds[1]
   myhi <- mybds[2]
   if (usefread) {
      requireNamespace('data.table')
      myfread <- data.table::fread
   } else myfread <- read.table
   if (!infiledst) {
      # this node reads the ordinary file, and grabs the records 
      # belonging to its bin
      tmp <- myfread(infilenm,header=header,sep=sep) 
      mychunk <- getmypart(tmp,colnum,myhi,mylo)
   } else {
      # this node reads all chunks of`the distributed file, 
      # and grabs the records belonging to its bin
      for (i in 1:ncls) {
         tmp <- myfread(filechunkname(infilenm,ndigs,i),header=header,sep=sep) 
         tmp <- getmypart(tmp,colnum,myhi,mylo)
         mychunk <- if (i == 1) tmp else rbind(mychunk,tmp)
      }
   }
   sortedmchunk <- mychunk[order(mychunk[,colnum]),]
   eval(parse(text = paste(outdfnm,' <<- sortedmchunk')))
}

# find the bins
getbounds <- function(cls,infilenm,infiledst,colnum,ndigs,header,sep,nsamp) {
   numnodes <- length(cls)
   if (infiledst) {  
       # sample from each node's chunk of the distributed file
      nsamp <- ceiling(nsamp/numnodes)
      samps <- clusterCall(cls,getsample,infilenm,ndigs,colnum,
         nsamp,header=header,sep=sep) 
      samp <- Reduce(c,samps)
   } else  # sample from the ordinary file
      samp <- 
        read.table(infilenm,nrows=nsamp,header=header,sep=sep)[,colnum]
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

# read the distributed file
getsample <- function(basenm,ndigs,colnum,nsamp,
      header=FALSE,sep="") 
{
   fname <- filechunkname(basenm,ndigs)
   read.table(fname,nrows=nsamp,header=header,sep=sep)[,colnum]
}

# grab for this node's bin from this chunk of the file (the whole file,
# in in the non-distributed case)
getmypart <- function(chunk,colnum,myhi,mylo) {
   pte <- getpte()
   me <- pte$myid
   ncls <- pte$ncls
   tmpcol <- chunk[,colnum,drop=FALSE]
   if (me == 1) {
      tmp <- chunk[tmpcol <= myhi,] 
   } else if (me == ncls) {
       tmp <- chunk[tmpcol > mylo,]
   } else {
      tmp <- chunk[tmpcol > mylo & tmpcol <= myhi,] 
   }
   tmp
}

# useful if need data in random order but the distributed file is not in
# random order
#
# read distributed file, producing scrambled, distributed chunks in
# memory at the cluster nodes, i.e. line i of the original distributed
# file bhas probability 1/nch of ending up at any of the in-memory
# chunks;  chunk sizes may not exactly equal across nodes, even if nch
# evenly divides the total number of lines in the distributed file; sep
# is the file field delineator, e.g. space of comma
readnscramble <- function(cls,basenm,header=FALSE,sep= " ") {
   nch <- length(cls)
   ndigs <- getnumdigs(nch)
   linecounts <- vector(length=nch)
   # get file names
   fns <- sapply(1:nch,function(i) 
      filechunkname(basenm,ndigs,nodenum=i))
   linecounts <- sapply(1:nch,
      function(i) linecount(fns[i],header=header))
   cums <- cumsum(linecounts)
#    clusterExport(cls,
#       c("basenm","linecounts","cums","fns","nch","header","sep"),
#       envir=environment())
   totrecs <- cums[nch]
   # give the nodes their index assignments
   tmp <- sample(1:totrecs,totrecs,replace=FALSE)
   idxs <- clusterSplit(cls,tmp)
   # invisible(clusterApply(cls,idxs,readmypart))
   invisible(clusterApply(cls,idxs,readmypart,
      basenm,linecounts,cums,fns,nch,header,sep))
}

# readmypart <- function(myidxs) {
readmypart <- function(myidxs,
      basenm,linecounts,cums,fns,nch,header,sep) {
   mydf <- NULL
   for (i in 1:nch) {
      filechunk <- read.table(fns[i],header=header,sep=sep)
      # filechunk <- freadfns[i],header=header,sep=sep)
      # which ones are mine?
      tmp <- myidxs
      if (i > 1) tmp <- myidxs - cums[i-1]
      tmp <- tmp[tmp >= 1]
      tmp <- tmp[tmp <= linecounts[i]]
      mydf <- rbind(mydf,filechunk[tmp,])
   }
   ### assign(basenm,mydf,envir=.GlobalEnv)  # write to global at worker
   eval(parse(text = 
      paste('assign("',basenm,'",mydf,envir=.GlobalEnv)',sep='')))
}

# split a file basenm into nch approximately equal-sized chunks, with
# suffix being chunk number; e.g. if nch = 16, then basenm.01,
# basenm.02,..., basenm.16; header, if any, is retained in the
# chunks; optionally, each output line can be preceded by a sequence
# number, in order to preserve the original ordering

filesplit <- function(nch,basenm,header=FALSE,seqnums=FALSE) {
   nlines <- linecount(basenm,header=header)  # not incl. header line
   con <- file(basenm,open="r")
   if (header) hdr <- readLines(con,1)
   ndigs <- getnumdigs(nch)
   chunks <- splitIndices(nlines,nch)
   chunksizes <- sapply(chunks,length)
   if (seqnums) cumulsizes <- cumsum(chunksizes)
   for (i in 1:nch) {
      chunk <- readLines(con,chunksizes[i])
      fn <- filechunkname(basenm,ndigs,i)
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

# like filesplit(), but randomizing the records
filesplitrand <- function(cls,fname,newbasename,ndigs,header=FALSE,sep) {
   tmpdf <- read.table(fname,header=header,sep=sep)
   # tmpdf <- freadfname,header=header,sep=sep)
   distribsplit(cls,'tmpdf',scramble=TRUE)
   filesave(cls,'tmpdf',newbasename,ndigs,sep=sep)
}

# same aim as filesplitrand(), but without ever reading more than one
# record at a time into memory 
#
# assumes that filesplit() has been run first
#
# if one has a file f and wishes to divide it into chunks with random
# order of records, one might call filesplit() first, then fileshuffle()
# several times in succession
#
# the number of output files need not be the same as the number inputs
# thus enabling adaptation to expansion or contraction of the
# computation cluster

# arguments:

#    inbasename: basename of the input files, e.g. x for x.1, x.2, ...
#    nout: number of output files
#    outbasename: basename of the output files
#    header: if TRUE, it is assumed (but not checked) that 
#            all input files have the same header

fileshuffle <- function(inbasename,nout,outbasename,header=FALSE) {
   infiles <- getinfiles(inbasename)
   nin <- length(infiles)
   incons <- list(length=nin)
   # set up connections for the input files
   for (i in 1:nin) {
      con <- file(infiles[i],open='r')
      incons[[i]] <- con
   } 
   # get header, if any
   if (header) for (i in 1:nin) hdr <- readLines(incons[[i]],1)
   # set up connections for the output files
   ndigs <- getnumdigs(nout)
   outcons <- list(length=nout)
   for (j in 1:nout) {
      fn <- filechunkname(outbasename,ndigs,j)
      conout <- file(fn,open='w')
      if (header) writeLines(hdr,conout)
      outcons[[j]] <- conout
   }
   nrecs <- rep(0,nout)  # number of records in each file
   # start shuffle
   repeat {
      # read a record from a random input file
      if (length(incons) > 0) {
         i <- sample(1:length(incons),1)
         rec <- readLines(incons[[i]],1)
         if (length(rec) == 0)  {  # end of file
            close(incons[[i]])
            incons[[i]] <- NULL
         } else {
            # write to a random output file
            j <- sample(1:nout,1)
            writeLines(rec,outcons[[j]])
            nrecs[j] <- nrecs[j] + 1
         }
      } else {
         for (j in 1:nout) close(outcons[[j]])
         return()
      }
   }
}

# gather the names of all the files with names starting with bn + '.'
getinfiles <- function(bn) {
   d <- dir()
   bnpoint <- paste(bn,'.',sep='')
   lbn1 <- nchar(bnpoint)
   startswith <- function(di) 
      substr(di,1,lbn1) == bnpoint
   tmp <- startswith(d)
   d[tmp]
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
linecount <- function(infile,header=FALSE,chunksize=100000) {
   # could check for Unix first (Mac, Linux, cygwin), running the more
   # efficient 'wc -l' in that case
   con <- file(infile,"r")
   nlines <- 0 - as.integer(header)
   repeat {
      tmp <- readLines(con,chunksize)
      nread <- length(tmp)
      if (nread == 0) return(nlines)
      nlines <- nlines + nread
   }
}

filecat <- function (cls, basenm, header = FALSE)  {
    lcls <- length(cls)
    # ndigs <- ceiling(log10(lcls))
    ndigs <- getnumdigs(lcls)
    if (file.exists(basenm)) file.remove(basenm)
    conout <- file(basenm, open = "w")
    for (i in 1:lcls) {
       # read in the entire file chunk
       fn <- filechunkname(basenm, ndigs, i)
       conin <- file(fn, open = "r")
       lines <- readLines(conin,-1)
       if (header && i > 1) {
          lines <- lines[-1]
       }
       writeLines(lines, conout)
    }
    close(conout)
}

# saves the distributed data frame/matrix d to a distributed file of the
# specified basename; the suffix has ndigs digits, and the field 
# separator will be sep; d must have column names
filesave <- function(cls,dname,newbasename,ndigs,sep) {
   # what will the new file be called at each node?
   tmp <- paste('"',newbasename,'",',ndigs,sep='')
   cmd <- paste('myfilename <- filechunkname(',tmp,')',sep='')
   clusterExport(cls,"cmd",envir=environment())
   clusterEvalQ(cls,eval(parse(text=cmd)))
   # start building the write.table() call
   tmp <- paste(dname,'myfilename',sep=',')
   # what will the column names be for the new files?
   clusterEvalQ(cls,eval(parse(text=cmd)))
   cncmd <- paste('colnames(',dname,')',sep='')
   clusterExport(cls,"cncmd",envir=environment())
   clusterEvalQ(cls,cnames <- eval(parse(text=cncmd)))[[1]]
   # now finish pasting the write.table() command, and run it
   writecmd <- paste('write.table(',tmp,
      ',row.names=FALSE,col.names=cnames,sep="',sep,'")',sep='')
   clusterExport(cls,"writecmd",envir=environment())
   clusterEvalQ(cls,eval(parse(text=writecmd)))
}

# reads in a distributed file with prefix fname, producing a distributed
# data frame dname
fileread <- function(cls,fname,dname,ndigs,
               header=FALSE,sep=" ",usefread=FALSE) {
   if (usefread) {
     clusterEvalQ(cls,requireNamespace('data.table'))
     clusterEvalQ(cls,myfread <- data.table::fread)
   } else {clusterEvalQ(cls,myfread <- read.table)}
   fnameq <- paste("'",fname,"'",sep="")
   tmp <- paste(fnameq,ndigs,sep=',')
   cmd <- paste("mychunk <- filechunkname(",tmp,")")
   clusterExport(cls,"cmd",envir=environment())
   clusterEvalQ(cls,eval(parse(text=cmd)))
   tmp <- paste(dname,"<- myfread(mychunk,header=",header,",sep='")
   cmd <- paste(tmp,sep,"')",sep="")
   clusterExport(cls,"cmd",envir=environment())
   invisible(clusterEvalQ(cls,eval(parse(text=cmd))))
}

# find the number of digits needed for suffixes for nch chunks
getnumdigs <- function(nch) {
   # floor(log10(nch)) + 1
   nchar(as.character(nch))
}

# like aggregate(), but a single R process reading in and processing
# one file at a time; it is presumed that each file can be fit in memory
#
# arguments: 
#
#    ynames: a character vector stating the variables to be tabulated
#    xnames: a character vector stating the variables to be used to form
#            cells
#    fnames: a character vector stating the files to be tabulated
#    FUN, FUN2: functions to be applied at the first and second levels
#               of aggregation
fileagg <- function(fnames,ynames,xnames,
      header=FALSE,sep=" ",FUN,FUN1=FUN) {
   nby <- length(xnames) # number in the "by" arg to aggregate()
   # set up aggregate() command to be run on the cluster nodes
   ypart <- paste("cbind(",paste(ynames,collapse=","),")",sep="")
   xpart <- paste(xnames,collapse="+")
   forla <- paste(ypart,"~",paste(xnames,collapse="+"))
   forla <- as.formula(forla)
   agg <- NULL
   for (fn in fnames) {
       tmpdata <- read.table(fn,header=header,sep=sep)
       tmpagg <- aggregate(forla,data=tmpdata,FUN=FUN)
       agg <- rbind(agg,tmpagg)
   }
   # run the command, and combine the returned data frames into one big
   # data frame
   # typically a given cell will found at more than one cluster node;
   # they must be combined, using FUN1
   FUN1 <- get(FUN1)
   aggregate(x=agg[,-(1:nby)],by=agg[,1:nby,drop=FALSE],FUN1)
}

# distributed wrapper for fileagg(); assigns each cluster node to handle
# a set of files, call fileagg() on them; then combines the results
dfileagg <- function(cls,fnames,ynames,xnames,
      header=FALSE,sep=" ",FUN,FUN1=FUN) {
   idxs <- splitIndices(length(fnames),length(cls))
   fnchunks <- Map(function(idxchunk) fnames[idxchunk],idxs)
   aggs <- clusterApply(cls,fnchunks,fileagg,ynames,xnames,
      header=header,sep=sep,FUN=FUN,FUN1=FUN1) 
   nby <- length(xnames)
   agg <- Reduce(rbind, aggs)
      FUN1 <- get(FUN1)
      aggregate(x = agg[, -(1:nby)], by = agg[, 1:nby, drop = FALSE], 
         FUN1)
}

# reads in the files in fnames, one at a time, naming the data tmpdata,
# and applying
#
#    tmprows <- tmpdataexpr
#
# where tmpdataexpr is an expression involving tmpdata; rbind() combines
# all the results for the final output
filegetrows <- function(fnames,tmpdataexpr,header=FALSE,sep=" ") {
   rows <- NULL
   for (fn in fnames) {
      tmpdata <- read.table(fn,header=header,sep=sep)
      cmd <- paste('tmprows <- ',tmpdataexpr,sep='')
      tmprows <- eval(parse(text=cmd))
      rows <- rbind(rows,tmprows)
   }
   rows
}

# distributed wrapper for fileagg(); assigns each cluster node to handle
# a set of files, call fileagg() on them; then combines the results
dfilegetrows <- function(cls,fnames,tmpdataexpr,
      header=FALSE,sep=" ") {
   idxs <- splitIndices(length(fnames),length(cls))
   fnchunks <- Map(function(idxchunk) fnames[idxchunk],idxs)
   rowslist <- clusterApply(cls,fnchunks,filegetrows,tmpdataexpr,
      header=header,sep=sep) 
   Reduce(rbind,rowslist)
}
