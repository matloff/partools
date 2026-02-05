\name{snowdoop,filechunkname, etc...}
\alias{snowdoop}
\alias{filechunkname}
\alias{filesort}
\alias{filesplit}
\alias{filesplitrand}
\alias{fileshuffle}
\alias{filecat}
\alias{readnscramble}
\alias{linecount}
\alias{getnumdigs}
\alias{filesave}
\alias{fileread}
\alias{fileagg}
\alias{dfileagg}
\alias{filegetrows}
\alias{dfilegetrows}

\title{
Snowdoop.
}

\description{
``Snowdoop'': Utilities for distributed file storage, access and 
related operations.
}

\usage{
filechunkname(basenm,ndigs,nodenum=NULL)
filesort(cls,infilenm,colnum,outdfnm,infiledst=FALSE,
   ndigs=0,nsamp=1000,header=FALSE,sep="",usefread=FALSE)
filesplit(nch,basenm,header=FALSE,seqnums=FALSE)
filesplitrand(cls,fname,newbasename,ndigs,header=FALSE,sep)
fileshuffle(inbasename, nout, outbasename, header = FALSE)
linecount(infile,header=FALSE,chunksize=100000)
filecat(cls, basenm, header = FALSE)  
readnscramble(cls,basenm,header=FALSE,sep= " ")
filesave(cls,dname,newbasename,ndigs,sep)
fileread(cls,fname,dname,ndigs,header=FALSE,sep=" ",usefread=FALSE)
getnumdigs(nch)
fileagg(fnames,ynames,xnames,header=FALSE,sep= " ",FUN,FUN1=FUN) 
dfileagg(cls,fnames,ynames,xnames,header=FALSE,sep=" ",FUN,FUN1=FUN) 
filegetrows(fnames,tmpdataexpr,header=FALSE,sep=" ") 
dfilegetrows(cls,fnames,tmpdataexpr,header=FALSE,sep=" ") 
}

\arguments{
  \item{cls}{A cluster for the \pkg{parallel} package.}
  \item{nch}{Number of chunks for the file split.}
  \item{basenm}{A chunked file name, minus suffix.}
  \item{infile}{Name of a nonchunked file.}
  \item{ndigs}{Number of digits in the chunked file name suffix.}
  \item{nodenum}{If non-NULL, get the name of the file chunk of cluster node
     \code{nodenum}; otherwise, get the name for the chunk associated
     with this node.}
  \item{infilenm}{Name of input file (without suffix, if distributed).}
  \item{outdfnm}{Name of output file (without suffix).}
  \item{infiledst}{If TRUE, infilenm is distributed.}
  \item{colnum}{Column number on which the sort will be done.  It is
     assumed that this data column is free of NAs.}
  \item{usefread}{If true, use \code{fread} instead of \code{read.table};
     generally much faster; requires \code{data.table} package.}
  \item{nsamp}{Number of records to sample in each file chunk to
     determine bins for the bucket sort.}
  \item{header}{TRUE if the file chunks have headers.}
  \item{seqnums}{TRUE if the file chunks will have sequence numbers.}
  \item{sep}{Field delimiter used in \code{read.table}.}
  \item{chunksize}{Number of lines to read at a time, for efficient I/O.}
  \item{dname}{Quoted name of a distributed data frame or matrix.  For
     \code{filesave}, the object must have column names.}
  \item{fname}{Quoted name of a distributed file.}
  \item{fnames}{Character vector of file names.}
  \item{newbasename}{Quoted name of the prefix of a distributed file,
     e.g. \code{xyz} for a distributed file \code{xyz.01}, \code{xyz.02}
     etc.}
  \item{ynames}{Vector of quoted names of variables on which \code{FUN}
     is to be applied.}
  \item{xnames}{Vector of quoted names of variables to be used for cell
     definition.}
  \item{tmpdataexpr}{Expression involving a data frame
     \code{tmpdataexpr}.  See below.}
  \item{FUN}{First-level aggregation function.}
  \item{FUN1}{Second-level aggregation function.}
  \item{inbasename}{basename of the input files, e.g. x for x.1, x.2, ...}
  \item{outbasename}{basename of the output files}
  \item{nout}{number of output files}
}

\details{

Use \code{filesplit} to convert a single file into distributed one, with
\code{nch} chunks.  The file header, if present, will be retained in the
chunks. If \code{seqnums} is TRUE, each line in a chunk will be preceded
by the line number it had in the original file.  

The reverse operation to \code{filesplit} is performed by
\code{filecat}, which converts a distributed file into a single one.

The \code{fileagg} function does an out-of-memory, multifile version of
\code{aggregate}, reading the specified files one at a time, and
returning a grand aggregation.  The function \code{dfileagg} partitions
the specified group of files to a \code{partools} cluster, has each
call \code{fileagg}, and again aggregates the results.

The function \code{filegetrows} reads in the files in \code{fnames}, one
at a time, naming the resulting in-memory data \code{tmpdata} each time.
(It is assumed that the data fit in memory.)  The function applies the
user command \code{tmpdataexpr} to \code{tmpdata}, producing a subset of
\code{tmpdata}.  All of these subsets are combined using \code{rbind},
yielding the return value.  The paired function \code{dfilegetrows} is a
distributed wrapper for \code{filegetrows}, just as \code{dfileagg} is
for \code{fileagg}.

Use \code{filesort} to do a file sort, with the input file being either
distributed or ordinary, placing the result as a distributed data
frame/matrix in the memories of the cluster nodes.  The first
\code{nsamp} records are read from the file, and are used to form one
quantile range for each cluster node.  Each node then reads the input
file, retaining the records in its assigned range, and sorts them.  This
results in the input file being sorted, in memory, in a distributed
manner across nodes, under the specifid name.  At present, this
utility is not very efficient.

Operations such as \code{ca} need i.i.d. data. If the original file
storage was ordered on some variable, one needs to randomize the data
first.  There are several options:

   \itemize{

      \item \code{readnscramble}:  This produces a distributed data 
      frame/matrix under the name \code{basenm}.  Note that a record in chunk
      \code{i} of the distributed file will likely end up in chunk \code{j} 
      in the distributed data frame/matrix, with \code{j} different from
      \code{i}.  

      \item \code{filesplitrand}: Use this you wish to directly produce a
      randomized distributed file from a monolithic one.  It will read
      the file into memory, chunk it at the cluster nodes, each of which
      will save its chunk to disk.

      \item \code{fileshuffle}: If you need to avoid reading big files 
      into memory, use this.  You must run \code{filesplit} first, and
      then run \code{fileshuffle} several times for a good shuffle.

      Note that this function is also useful if your cluster size
      changes.  A distributed file of m chunks can now be converted to
      one with n chunks, either more or fewer than before.
   }

If you wish to use this same randomized data in a future session, you
can save it as a distributed file by calling \code{filesave}. Of course,
this function is also useful if one wishes to save a distributed data
frame or matrix that was created computationally rather than from read
from a distributed file.  To go the other direction, i.e. read a
distributed file, use \code{fileread}.

Some of the functions here are useful mainly as intermediate operations
for the others:

   \itemize{
   
      \item The function \code{filechunkname} returns the name of the file
      chunk for the calling cluster node.
   
      \item The \code{linecount} function returns the number of lines in a
      text file.
   
      \item A call to \code{getnumdigs} returns the number of digits in a
      distributed file name suffix.
   
   }

}

\examples{
cls <- makeCluster(2)
setclsinfo(cls)

# example of filesplit()
# make test input file
m <- rbind(1:2,3:4,5:6) 
write.table(m,"m",row.names=FALSE,col.names=FALSE) 
# apply the function
filesplit(2,"m",seqnums=TRUE)
# file m.1 and m.2 created, with contents c(1,1,2) and
# rbind(c(2,3,4),c(3,5,6)), respectively
# check it
read.table("m.1",header=FALSE,row.names=1)
read.table("m.2",header=FALSE,row.names=1)
m

# example of filecat(); assumes filesplit() example above already done
# delete file m so we can make sure we are re-creating it
unlink("m")
filecat(cls,"m")
# check that file m is back
read.table("m",row.names=1)

# example of filesave(), fileread()
# make test distributed data frame
clusterEvalQ(cls,x <- data.frame(u = runif(5),v = runif(5)))
# apply filesave()
filesave(cls,'x','xfile',1,' ')
# check it
fileread(cls,'xfile','xx',1,header=TRUE,sep=' ')
clusterEvalQ(cls,xx)
clusterEvalQ(cls,x)


# example of filesort()
# make test distributed input file
m1 <- matrix(c(5,12,13,3,4,5,8,8,8,1,2,3,6,5,4),byrow=TRUE,ncol=3)
m2 <- matrix(c(0,22,88,44,5,5,2,6,10,7,7,7),byrow=TRUE,ncol=3)
write.table(m1,"m.1",row.names=FALSE)
write.table(m2,"m.2",row.names=FALSE)
# sort on column 2 and check result
filesort(cls,"m",2,"msort",infiledst=TRUE,ndigs=1,nsamp=3,header=TRUE)
clusterEvalQ(cls,msort)  # data should be sorted on V2
# check by comparing to input
m1
m2
m <- rbind(m1,m2)
write.table(m,"m",row.names=FALSE)
clusterEvalQ(cls,rm(msort))
filesort(cls,"m",2,"msort",infiledst=FALSE,nsamp=3,header=TRUE)
clusterEvalQ(cls,msort)  # data should be sorted on V2

# example of readnscramble()
co2 <- head(CO2,25) 
write.table(co2,"co2",row.names=FALSE)  # creates file 'co2'
filesplit(2,"co2",header=TRUE)  # creates files 'co2.1', 'co2.2'
readnscramble(cls,"co2",header=TRUE)  # now have distrib. d.f.
# save the scrambled version to disk
filesave(cls,'co2','co2s',1,sep=',')

# example of fileshuffle()
# make test file, 'test'
cat('a','bc','def','i','j','k',file='test',sep='\n')
filesplit(2,'test')  # creates files 'test.1','test.2'
fileshuffle('test',2,'testa')  # creates shuffled files 'testa.1','testa.2'

# example of filechunkname()
clusterEvalQ(cls,filechunkname("x",3))  # returns "x.001", "x.002"

# example of getnumdigs()
getnumdigs(156)  # should be 3

# examples of filesave() and fileread()
mtc <- mtcars
distribsplit(cls,"mtc")
# save distributed data frame to distributed file
filesave(cls,'mtc','ctm',1,',') 
# read it back in to a new distributed data frame
fileread(cls,'ctm','ctmnew',1,header=TRUE,sep=',') 
# check it
clusterEvalQ(cls,ctmnew) 
# try dfileagg() on it (not same as distribagg())
dfileagg(cls,c('ctm.1','ctm.2'),c("mpg","disp","hp"),c("cyl","gear"),header=TRUE,sep=",","max")
# check
aggregate(cbind(mpg,disp,hp) ~ cyl+gear,data=mtcars,FUN=max)
# extract the records with 4 cylinders and 4 gears (again, different
# from distribgetrows())
cmd <- 'tmpdata[tmpdata$cyl == 4 & tmpdata$gear == 4,]'
dfilegetrows(cls,c('ctm.1','ctm.2'),cmd,header=TRUE,sep=',') 
# check
mtc[mtc$cyl == 4 & mtc$gear == 4,]

stopCluster(cls)


}

\author{
Norm Matloff
}

