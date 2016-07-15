\name{parpdist}
\alias{parpdist}

\title{Partools Apps}

\description{
General parallel applications.
}

\usage{
parpdist(x,y,cls)
}

\arguments{
  \item{cls}{A cluster run under the \pkg{parallel} package.}
  \item{x}{A data matrix} 
  \item{y}{A data matrix} 
}

\details{Parallel wrapper for \code{pdist} from package of the same
name. Finds all the distances from rows in \code{x} to rows in
\code{y}.}


\value{Object of type \code{"pdist"}.}


\examples{
# set up 'parallel' cluster
cls <- makeCluster(2)
setclsinfo(cls)

x <- matrix(runif(20),nrow=5)
y <- matrix(runif(32),nrow=8)
# 2 calls should have identical resultsW
pdist(x,y,cls)@dist
parpdist(x,y,cls)@dist

stopCluster(cls)

}

\author{
Norm Matloff
}

