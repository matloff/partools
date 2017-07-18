\name{findrow,makedff,'['}
\alias{findrow}
\alias{makedff}
\alias{'['}

\title{Virtual Data Structures}

\description{
Accessing a Distributed Data Frame or Similar Object As a Virtual
Monolithic Object}

\usage{
findrow(cls, i, objname)
makeddf(dname,cls) 
}

\arguments{
  \item{cls}{A cluster run under the \pkg{parallel} package.}
  \item{i}{A row number in a distributed data frame or similar object.}} 
  \item{objname}{Name of such an object.}
  \item{dname}{Name of such an object.}
}

\details{
These functions enable the user at the manager node to treat a 
distributed data frame as a virtual monolithic one, querying the values in specified row and clumn ranges. 

Say we have a distributed data frame \code{d} on two worker nodes, with
five rows at the first node and five at the second.  Row 6 of the virtual
data frame, then, will consist of the first row in at the second node. 

Viewing this virtual data frame requires creating an object of class
\code{'ddf'}, using \code{makeddf}.  Note that there is no actual data
at the manager node.  This class overrides the reference operator \code{'['}.

The function \code{findrow} goes in the opposite direction.  For a given
row number in the virtual data frame, this function will return the row
number within node, and the node number.

}

\examples{
cls <- makeCluster(2)
setclsinfo(cls)
clusterEvalQ(cls,d <- data.frame(x=sample(1:10,5),y=sample(1:10,5)))
makeddf('d',cls) 
d[2,2]  # 9
d[6,2]  # 2
d[6,1]  # 10
d[,1]  # 3  9  8  4  6 10  1  8  4  6
d[8,]  # 8 6
d[,]  # the entire 10x2 data frame
findrow(cls,8,'d')  # 3 2; row 8 in the virtual df is row 3 of d in node 2
}

\author{
Norm Matloff and Reed Davis
}

