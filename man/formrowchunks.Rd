\name{formrowchunks,addlists,matrixtolist,setclsinfo,getpte,distribsplit,distribcat,distribagg,distribrange,distribcounts,distribgetrows,docmd,geteltis,distribmeans,distribisdt,ipstrcat}
\alias{formrowchunks}
\alias{matrixtolist} 
\alias{addlists}
\alias{setclsinfo}
\alias{getpte}
\alias{exportlibpaths}
\alias{distribsplit}
\alias{distribcat}
\alias{distribagg}
\alias{distribrange}
\alias{distribcounts}
\alias{distribgetrows}
\alias{distribmeans}
\alias{docmd}
\alias{distribisdt}
\alias{geteltis}
\alias{ipstrcat}

\title{
Utilities for \pkg{parallel} cluster code.
}

\description{
Miscellaneous code snippets for use with the \pkg{parallel} package,
including ``Snowdoop.''}

\usage{
formrowchunks(cls,m,mchunkname,scramble=FALSE) 
matrixtolist(rc,m) 
addlists(lst1,lst2,add)
setclsinfo(cls)
getpte()
exportlibpaths(cls)
distribsplit(cls,dfname,scramble=FALSE)
distribcat(cls,dfname) 
distribagg(cls,ynames,xnames,dataname,FUN,FUNdim=1,FUN1=FUN) 
distribrange(cls,vec,na.rm=FALSE) 
distribcounts(cls,xnames,dataname)
distribmeans(cls,ynames,xnames,dataname,saveni=FALSE)
distribgetrows(cls,cmd)
distribisdt(cls,dataname) 
docmd(toexec) 
geteltis(lst,i)
ipstrcat(str1 = stop("str1 not supplied"), ..., outersep = "", innersep = "")
}

\arguments{
  \item{cls}{A cluster for the \pkg{parallel} package.}
  \item{scramble}{If TRUE, randomize the row order in the resulting data
     frame.}
  \item{rc}{Set to 1 for rows, other for columns.}
  \item{m}{A matrix or data frame.}
  \item{mchunkname}{Quoted name to be given to the created chunks.}
  \item{lst1}{An R list.}
  \item{lst2}{An R list.}
  \item{add}{``Addition'' function, which could be summation,
     concatenation and so on.}
  \item{dfname}{Quoted name of a data frame, either centralized or
     distributed.} 
  \item{ynames}{Vector of quoted names of variables on which \code{FUN} 
     is to be applied.}

  \item{...}{One of more vectors of character strings, where the vectors
     are typically of length 1.}
  \item{xnames}{Vector of quoted names of variables that define the
     grouping.}
  \item{dataname}{Quoted name of a distributed data frame or data.table.}
  \item{saveni}{If TRUE, save the chunk sizes.}
  \item{FUN}{Quoted name of a single-argument function to be used in
     aggregating within cluster nodes.  If \code{dataname} is the name
     of a data.table, \code{FUN} must be a vector of function names, of
     length equal to that of \code{ynames}.}
  \item{FUNdim}{Number of elements in the return value of \code{FUN}.
     Must be 1 for data.tables.}
  \item{FUN1}{Quoted name of function to be used in
     aggregation between cluster nodes.}
  \item{vec}{Quoted expression that evaluates to a vector.}
  \item{na.rm}{Remove NA values.}
  \item{cmd}{An R command.}
  \item{toexec}{Quoted string containing command to be executed.}
  \item{lst}{An R list of vectors.}
  \item{i}{A column index}
  \item{str1}{A character string.}
  \item{outersep}{Separator, e.g. a comma, between strings specified in
  ...}
  \item{innersep}{Separator, e.g. a comma, within string vectors specified in
  ...}
}

\details{The \code{setclsinfo} function does initialization needed for
use of the tools in the package.

The function \code{formrowchunks} forms chunks of rows of
\code{m}, corresponding to the number of worker nodes
in the cluster \code{m}.  For any given worker, the code places its
chunk in \code{mchunk} in the global space of the worker.

A call to \code{matrixtolist} extracts the rows or columns of a matrix
or data frame and forms an R list from them.

The function \code{addlists} does the following:  Say we have two lists,
with numeric values.  We wish to form a new list, with all the keys
(names) from the two input lists appearing in the new list.  In the case
of a key in common to the two lists, the value in the new list will be
the sum of the two individual values for that key.  (Here ``sum'' means
the result of applying \code{add}.) For a key appearing in one list and
not the other, the value in the new list will be the value in the input
list.  

The function \code{exportlibpaths}, invoked from the manager, exports
the manager's R search path to the workers.

The function \code{distribsplit} splits a data frame \code{dfname} into
approximately  equal-sized chunks of rows, placing the chunks on the
cluster nodes, as global variables of the same name. The opposite action
is taken by \code{distribcat}, coalsecing variables of the given name in
the cluster nodes into one grand data frame as the calling (i.e.
manager) node.

The package's \code{distribagg} function is a distributed (and somewhat
restricted) form of \code{aggregate}.  The latter is called to each
distributed chunk with the function \code{FUN}.  The manager collects
the results and calls \code{FUN1}.

The special cases of aggregating counts and means is handled by the
wrappers \code{distribcounts} and \code{distribmeans}.  In each case,
cells are defined by \code{xnames}, and aggregation done first within
workers and then across workers. 

The \code{distribrange} function is a distributed form of \code{range}.

The \code{distribgetrows} function is useful in a variety of situations.
It can be used, for instance, as a distributed form of \code{select}.
In the latter case, the specified rows will be selected at each cluster
node, then \code{rbind}-ed together at the caller.

The \code{docmd} function executes the quoted command, useful for
building up complex command for remote execution.  

An R \code{formula} will be constructed from the arguments \code{ynames}
and \code{xnames}, with the latter put on the left side of the \code{~}
sign, with \code{cbind} for combining, and the latter put on the right
side, with \code{+} signs as delimiters.

The \code{geteltis} function extracts from an R list of vectors element
\code{i} from each.

}

\value{
In the case of \code{addlists}, the return value is the new list.

The \code{distribcat} function returns the concatenated data frame;
\code{distribgetrows} works similarly.

The \code{distribagg} function returns a data frame, the same as would a
call to \code{aggregate}, though possibly in different row order;
\code{distribcounts} works similarly.

}

\examples{
# examples of addlists()
l1 <- list(a=2, b=5, c=1)
l2 <- list(a=8, c=12, d=28)
addlists(l1,l2,sum)  # list with a=10, b=5, c=13, d=28
z1 <- list(x = c(5,12,13), y = c(3,4,5))
z2 <- list(y = c(8,88))
addlists(z1,z2,c)  # list with x=(5,12,13), y=(3,4,5,8,88)

# need 'parallel' cluster for the remaining examples
cls <- makeCluster(2)
setclsinfo(cls)

# check it
clusterEvalQ(cls,partoolsenv$myid)  # returns 1, 2
clusterEvalQ(cls,partoolsenv$ncls)  # returns 2, 2

# formrowchunks example; see up a matrix to be distributed first
m <- rbind(1:2,3:4,5:6)
# apply the function
formrowchunks(cls,m,"mc")
# check results
clusterEvalQ(cls,mc)  # list of a 1x2 and a 2x2 matrix

matrixtolist(1,m)  # 3-component list, first is (1,2)

# test of of distribagg(): 
# form and distribute test data
x <- sample(1:3,10,replace=TRUE)
y <- sample(0:1,10,replace=TRUE)
u <- runif(10)
v <- runif(10)
d <- data.frame(x,y,u,v)
distribsplit(cls,"d")
# check that it's there at the cluster nodes, in distributed form
clusterEvalQ(cls,d) 
d
# try the aggregation function
distribagg(cls,c("u","v"), c("x","y"),"d","max")
# check result
aggregate(cbind(u,v) ~ x+y,d,max)

# real data
mtc <- mtcars
distribsplit(cls,"mtc")

distribagg(cls,c("mpg","disp","hp"),c("cyl","gear"),"mtc","max")
# check
aggregate(cbind(mpg,disp,hp) ~ cyl+gear,data=mtcars,FUN=max)

distribcounts(cls,c("cyl","gear"),"mtc")
# check
table(mtc$cyl,mtc$gear)

# find mean mpg, hp for each cyl/gear combination
distribmeans(cls,c('mpg','hp'),c('cyl','gear'),'mtc')

# extract and collect all the mtc rows in which the number of cylinders is 8
distribgetrows(cls,'mtc[mtc$cyl == 8,]')
# check
mtc[mtc$cyl == 8,]

# same for data.tables
mtc <- as.data.table(mtc)
setkey(mtc,cyl)
distribsplit(cls,'mtc')
distribcounts(cls,c("cyl","gear"),"mtc")
distribmeans(cls,c('mpg','hp'),c('cyl','gear'),'mtc')

stopCluster(cls)

}

\author{
Norm Matloff
}

