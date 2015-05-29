\name{formrowchunks,addlists,matrixtolist,setclsinfo,getpte,distribsplit,distribcat,distribagg,docmd,}
\alias{formrowchunks}
\alias{matrixtolist} 
\alias{addlists}
\alias{setclsinfo}
\alias{getpte}
\alias{exportlibpaths}
\alias{distribsplit}
\alias{distribcat}
\alias{distribagg}
\alias{docmd}

\title{
Utilities for \pkg{parallel} cluster code, including ``Snowdoop.''
}

\description{
Miscellaneous code snippets for use with the \pkg{parallel} package,
including ``Snowdoop.''}

\usage{
formrowchunks(cls,m,mchunkname) 
matrixtolist(rc,m) 
addlists(lst1,lst2,add)
setclsinfo(cls)
getpte()
exportlibpaths(cls)
distribsplit(cls,dfname)
distribcat(cls,dfname) 
distribagg(cls,aggcmd,FUN,nby) 
docmd(toexec) 
}

\arguments{
  \item{cls}{A cluster for the \pkg{parallel} package.}
  \item{rc}{Set to 1 for rows, other for columns.}
  \item{m}{A matrix or data frame.}
  \item{mchunkname}{Quoted name to be given to the created chunks.}
  \item{lst1}{An R list.}
  \item{lst2}{An R list.}
  \item{add}{``Addition'' function, which could be summation,
     concatenation and so on.}
  \item{dfname}{Quoted name of a data frame, either centralized or
  distributed.} 
  \item{aggcmd}{Quoted string for the \code{x} and \code{by} portions of
  the desired call to \code{aggregate}.}
  \item{FUN}{Quoted string containing the name of \code{FUN} in the
  desired \code{aggregate} call. Must have the property that
  \code{FUN(c(a,b)) = FUN(FUN(a),FUN(b))}, such is the case for
  \code{sum} and \code{max}.}
  \item{nby}{Number of variables involved in \code{by}.}
  \item{toexec}{Quoted string containing command to be executed.}
}

\details{The function \code{formrowchunks} forms chunks of rows of
\code{m}, corresponding to the number of worker nodes
in the cluster \code{m}.  For any given worker, the code places its
chunk in \code{mchunk} in the global space of the worker.

A call to \code{matrixtolist} extracts the rows or columns of a matrix
or data frame and forms an R list from them.

The function \code{addlists} does this:  Say we have two lists, with
numeric values.  We wish to form a new list, with all the keys (names)
from the two input lists appearing in the new list.  In the case of a
key in common to the two lists, the value in the new list will be the
sum of the two individual values for that key.  (Here ``sum'' means the
result of applying \code{add}.) For a key appearing in one list and not
the other, the value in the new list will be the value in the input
list.  

The function \code{exportlibpaths}, invoked from the manager, exports
the manager's R search path to the workers.

The function \code{distribcat} splits a data frame \code{dfname} into
approximately  equal-sized chunks of rows, placing the chunks on the
cluster nodes, as global variables of the same name. The opposite action
is taken by \code{distribcat}, coalsecing variables of the given name in
the cluster nodes into one grand data frame as the calling (i.e.
manager) node.

The package's \code{distribagg} function is a distributed form of
\code{aggregate}.  Note the restriction on \code{FUN}.  To achieve the
effect of \code{mean}, use \code{sum} but first use \code{cbind} in
\code{x}, appending a column of 1s, so as to get the cell counts; them
divide sums by counts.  If the data frame \code{dfname} lacks columns
names, e.g. because it was read in from a headerless file, give the
columns now, e.g. via a call like \code{clusterEvalQ(cls,names(x) <-
c("a","bc","def")}; otherwise a conflict may occur when tries to
coalsece the reports from the different cluster nodes.

The \code{docmd} function executes the quoted command.  Useful for
building up complex command for remote execution.

See vignettes for more information.  
}

\value{
In the case of \code{addlists}, the return value is the new list.

The \code{distribagg} function returns a data frame, the same as would a
call to \code{aggregate}, though possibly in different row order.
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

# test of of distribagg(): 
# form test data
x <- sample(1:3,10,replace=T)  
y <- sample(0:1,10,replace=T)
z <- runif(10); cbind(x,y,z)
d <- data.frame(x,y,z)
# distribute to the cluster nodes
distribsplit(cls,"d")
clusterEvalQ(cls,d)  # got to the nodes?
# run our distributed aggregate() on the nodes
distribagg(cls,"x=d, by=list(d$x,d$y)","max",2)
# run aggregate() here, check that it matches
aggregate(x=d,by=list(d$x,d$y),max)
}

\author{
Norm Matloff
}

