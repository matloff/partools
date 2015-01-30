\name{formrowchunks,addlists,matrixtolist,setclsinfo,getpte}
\alias{formrowchunks}
\alias{matrixtolist}
\alias{addlists}
\alias{setclsinfo}
\alias{getpte}
\alias{exportlibpaths}

\title{
Utilities for cluster code.
}

\description{
Miscellaneous code snippets for use with the \pkg{parallel} package.}

\usage{
formrowchunks(cls,m,mchunkname) 
matrixtolist(rc,m) 
addlists(lst1,lst2,add)
setclsinfo(cls)
getpte()
exportlibpaths(cls)
}

\arguments{
  \item{cls}{A cluster for the \pkg{parallel} package.}
  \item{rc}{Set to 1 for rows, other for columns.}
  \item{m}{A matrix or data frame.}
  \item{mchunkname}{Quoted chunk name.}
  \item{lst1}{An R list, with numeric values.}
  \item{lst2}{An R list, with numeric values.}
  \item{add}{``Addition'' function, which could be summation,
     concatenation and so on.}
}

\details{The function \code{formrowchunks} forms chunks of rows of
\code{m}, corresponding to the number of worker nodes
in the cluster \code{m}.  For any given worker, the code places its
chunk in \code{mchunk} in the global space of the worker.

A call to \code{matrixtolist} extracts the rows or columns of a matrix
or data frame and forms an R list from them.

Say we have two lists, with numeric values.  We wish to form a new list,
with all the keys (names) from the two input lists appearing in the new list.
In the case of a key in common to the two lists, the value in the new
list will be the sum of the two individual values for that key.  For a
key appearing in one list and not the other, the value in the new list
will be the value in the input list.  The funciton \code{addlists} does
this.  

The function \code{exportlibpaths}, invoked from the manager, exports
the manager's R search path to the workers.

See vignettes for more information.
}

\value{In the case of \code{addlists}, the return value is the new list.
}

\examples{
l1 <- list(a=2, b=5, c=1)
l2 <- list(a=8, c=12, d=28)
addlists(l1,l2,sum)  # list with a=10, b=5, c=13, d=28
z1 <- list(x = c(5,12,13), y = c(3,4,5))
z2 <- list(y = c(8,88))
addlists(z1,z2,c)  # list with x=(5,12,13), y=(3,4,5,8,88)

cls <- makeCluster(2)

setclsinfo(cls)
clusterEvalQ(cls,partoolsenv$myid)  # returns 1, 2
clusterEvalQ(cls,partoolsenv$ncls)  # returns 2, 2

m <- rbind(1:2,3:4,5:6)
formrowchunks(cls,m,"mc")
clusterEvalQ(cls,mc)  # list of a 1x2 and a 2x2 matrix

matrixtolist(1,m)  # 3-component list, first is (1,2)
}

\author{
Norm Matloff
}

