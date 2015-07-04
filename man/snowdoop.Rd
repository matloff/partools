\name{snowdoop,filechunkname,filesort,filesplit,readnscramble,linecount,filecat,getnumdigs,filesave,fileread}
\alias{snowdoop}
\alias{filechunkname}
\alias{filesort}
\alias{filesplit}
\alias{filecat}
\alias{readnscramble}
\alias{linecount}
\alias{getnumdigs}
\alias{filesave}
\alias{fileread}

\title{
Snowdoop.
}

\description{
Snowdoop operations:
Utilities for distributed computation, including distributed
file storage.
}

\usage{
filechunkname(basenm,ndigs,nodenum=NULL)
filesort(cls,basenm,ndigs,colnum,outname,nsamp = 1000,header = FALSE,sep = "")
filesplit(nch,basenm,header=FALSE,seqnums=FALSE)
filecat(cls, basenm, header = FALSE)  
readnscramble(cls,basenm,header=FALSE,sep= " ")
filesave((cls,dname,newbasename,ndigs,sep)
linecount(infile,chunksize=100000)
getnumdigs(nch)
}

\arguments{
  \item{cls}{A cluster for the \pkg{parallel} package.}
  \item{nch}{Number of chunks for the file split.}
  \item{basenm}{A chunked file name, minus suffix.}
  \item{ndigs}{Number of digits in the chunked file name suffix.}
  \item{nodenum}{If non-NULL, get the name of the file chunk of cluster node
     \code{nodenum}; otherwise, get the name for the chunk associated
     with this node.}
  \item{colnum}{Column number on which the sort will be done.}
  \item{outname}{Quoted name for the chunks of sorted output.}
  \item{nsamp}{Number of records to sample in each file chunk to
     determine bins for the bucket sort.}
  \item{header}{TRUE if the file chunks have headers.}
  \item{seqnums}{TRUE if the file chunks will have sequence numbers.}
  \item{sep}{Field delimiter used in \code{read.table}.}
  \item{infile}{Input file}
  \item{chunksize}{Number of lines to read at a time, for efficient I/O.}
  \item{dname}{Quoted name of a distributed data frame or matrix.}
  \item{fname}{Quoted name of a distributed file.}
  \item{ndigs}{Number of digits in file name suffix.}
  \item{newbasename}{Quoted name of the prefix of a distributed file,
     e.g. \code{xyz} for a distributed file \code{xyz.01}, \code{xyz.02}
     etc.}
  \item{sep}{A field delimiter for lines in a text file, e.g. 'm'.}
}

\details{

Those functions with argument \code{cls} presume that \code{setclsinfo}
has already been called.

Use \code{filesort} to do a distributed file sort, placing the result as
a distributed data frame/matrix in the memories of the cluster nodes.
The first \code{nsamp} records are read from 
each file chunk.  They are merged and quantiles formed, one quantile
range for each cluster node.  Each node then reads all the file chunks, 
retaining the records in its assigned range, and sorts them.  This 
results in the input file being sorted, in memory, in a distributed 
manner across nodes, under the name \code{basenm}. 

Use \code{filesplit} to convert a single file into distributed one, with
\code{nch} chunks.  The file header, if present, will be retained in the
chunks. If \code{seqnums} is TRUE, each line in a chunk will be preceded
by the line number it had in the original file.  

The reverse operation to \code{filesplit} is performed by
\code{filecat}, which converts a distributed file into a single one.

Operations such as \code{ca} need i.i.d. data. If the original file
storage was ordered on some variable, one needs to randomize the data
first, using \code{readnscramble}.  It produces a distributed data
frame/matrix under the name \code{basenm}.

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
filesplit(2,"m",seqnums=T)
# file m.1 and m.2 created, with contents c(1,1,2) and
# rbind(c(2,3,4),c(3,5,6)), respectively
# check it
read.table("m.1",header=FALSE,row.names=1)
read.table("m.2",header=FALSE,row.names=1)

# example of filecat(); assumes filesplit() example above already done
# delete file m so we can make sure we are re-creating it
unlink("m")
filecat(cls,"m")
# check that file m is back
read.table("m",row.names=1)

# example of filesort()
# make test distributed input file
m1 <- matrix(c(5,12,13,3,4,5,8,8,8,1,2,3,6,5,4),byrow=TRUE,ncol=3)
m2 <- matrix(c(0,22,88,44,5,5,2,6,10,7,7,7),byrow=TRUE,ncol=3)
write.table(m1,"m.1")
write.table(m2,"m.2")
# sort on column 2 and check result
filesort(cls,"m",1,2,"msort",nsamp=3,header=TRUE)
clusterEvalQ(cls,msort)  # data should be sorted on V2
# check by comparing to input
m1
m2

# example of readnscramble()
co2 <- head(CO2,25) 
write.table(co2,"co2",row.names=FALSE) 
filesplit(2,"co2",header=TRUE) 
readnscramble(cls,"co2",header=TRUE)
# save the scrambled version
filesave(cls,'co2','co2s',1,sep=',')

# example of filechunkname()
clusterEvalQ(cls,filechunkname("x",3))  # returns "x.1", "x.2"

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

}

\author{
Norm Matloff
}

