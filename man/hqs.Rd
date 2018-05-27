\name{hqs,hqsTest}
\alias{hqs}
\alias{hqsTest}

\title{Distributed Sort}

\description{Sort a distributed vector.}

\usage{
hqs(cls,xname)
hqsTest(vlength,clength)
}

\arguments{
  \item{cls}{A cluster for the \pkg{parallel} package.}
  \item{xname}{Name of a distributed vector.}
  \item{vlength}{Length of the test vector.}
  \item{clength}{Size of the test cluster.}
}

\details{ 

In \code{hqs}, the distributed vector is sorted using the Hyperquicksort
algorithm.  In keeping with partools' Leave It There philosophy,
both input and output are distributed; the sorted vector is
NOT returned to the caller.  The name of the sorted distributed vector
will be 'chunk'.  If the caller needs the sorted vector, this
can be obtained via \code{distribcat}.

}

\examples{

cls <- makeCluster(4) 
setclsinfo(cls) 
z <- sample(1:50,25) 
z  # view unsorted vector 
distribsplit(cls,'z')  # distribute it
hqs(cls,'z') 
# view the distributed sorted vector
clusterEvalQ(cls,chunk) 
# optionally collect the results at the caller
distribcat(cls,'chunk')

}

\author{
Robin Yancey, Norm Matloff
}

