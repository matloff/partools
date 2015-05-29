\name{snowdoop,filechunkname,filesort,filesplit}
\alias{snowdoop}
\alias{filechunkname}
\alias{filesort}
\alias{filesplit}

\title{
Snowdoop.
}

\description{
Snowdoop operations.
}

\usage{
filechunkname(basename,ndigs,nodenum=NULL)
filesort(cls,basename,ndigs,colnum,outname,nsamp = 1000,header = FALSE,sep = "") 
filesplit(nch,basename,header=FALSE,seqnums=FALSE)
}

\arguments{
  \item{cls}{A cluster for the \pkg{parallel} package.}
  \item{nch}{Number of chunks for the file split.}
  \item{basename}{A chunked file name, minus suffix.}
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
}

\details{
See further documentation in "Snowdoop" vignette.
}

\value{The function \code{filechunkname} returns the name of the file
chunk for the calling cluster node.

For \code{filesort}, the first \code{nsamp} records are read from 
each file chunk.  They are merged and quantiles formed, one quantile
range for each cluster node.  Each node then reads all the file chunks, 
retaining the records in its assigned range, and sorts them.  This 
results in the input file being sorted, in memory, in a distributed 
manner across nodes. 

Use \code{filesplit} to convert a file into chunked form, with
\code{nch} chunks.  The file header, if present, will be retained in the
chunks. If \code{seqnums} is TRUE, each line in a chunk will be preceded
by the line number it had in the original file. 
}

\examples{
cls <- makeCluster(2)
setclsinfo(cls)
clusterEvalQ(cls,filechunkname("x",3))  # returns "x.001", "x.002"

# make distributed input file
m1 <- matrix(c(5,12,13,3,4,5,8,8,8,1,2,3,6,5,4),byrow=TRUE,ncol=3)
m2 <- matrix(c(0,22,88,44,5,5,2,6,10,7,7,7),byrow=TRUE,ncol=3)
write.table(m1,"m.1")
write.table(m2,"m.2")
# sort and check result
filesort(cls,"m",1,2,"msort",nsamp=3,header=TRUE)
clusterEvalQ(cls,msort)  # data should be sorted on V2

# file m is rbind(1:2,3:4,5:6)
scan(file="m",what="",sep="\n")
filesplit(2,"m",seqnums=T)
# file m.1 and m.2 created, with contents c(1,1,2) and
# rbind(c(2,3,4),c(3,5,6)), respectively
scan(file="m.1",what="",sep="\n")
scan(file="m.2",what="",sep="\n")

}

\author{
Norm Matloff
}

