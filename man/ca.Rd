\name{ca,cabase}
\alias{ca}
\alias{ca,cabase}

\title{Software Alchemy: Turning Complex Statistical Computations into
Embarrassingly-Parallel Ones}

\description{
Easy parallelization of most statistical computations.
}

\usage{
ca(cls,z,ovf,estf,estcovf=NULL,conv2mat=TRUE,findmean=TRUE)
cabase(cls,z,ovf,estf,estcovf=NULL,conv2mat=TRUE,findmean=TRUE)
}

\arguments{
  \item{cls}{A cluster run under the \pkg{parallel} package.}
  \item{z}{Data. In the case of \code{ca}, a data.frame, matrix or 
  vector, one observation per row/element. For \code{cabase}, \code{z}
  is the quoted name of a distributed data frame, such as is produced by
  \code{distribsplit} or \code{readnscramble}.}
  \item{ovf}{Overall statistical function, say \code{glm}.}
  \item{estf}{Function to extract the point estimate (possibly
  vector-valued) from the output of \code{ovf}.}
  \item{estcovf}{if provided, function to extract the estimated 
  covariance matrix of the output of \code{estf}}.
  \item{conv2mat}{If TRUE, convert data framee input to a matrix 
  (needed for some cases of 'ovf').}
  \item{findmean}{If TRUE, output the average of the estimates from the
  chunks; otherwise, output only the estimates themselves.}
}

\details{Implements the ``Software Alchemy'' method for parallelizing
statistical computations (N. Matloff, \emph{Parallel Computation for
Data Science}, Chapman and Hall, 2015.

The data are broken into chunks, and the given estimator is applied to
each.  The results are averaged, and an estimated covariance matrix
computed.

Note:  The method assumes i.i.d. data.  If your data set had been stored
in some sorted order, it must be randomized first, say using the
\code{scramble} option in \code{distribsplit} or by calling
\code{readnscramble}, depending on whether your data is already in memory or
still in a file.
}

\value{R list with these components:

   \itemize{
   
      \item \code{thts}, the results of applying the requested estimator to
      the chunks; the estimator from chunk is in row i
   
      \item \code{tht}, the chunk-averaged overall estimator, if requested
   
      \item \code{thtcov}, the estimated covariance matrix of \code{tht},
      if requested
      
   }

}

\examples{
cls <- makeCluster(2)
setclsinfo(cls)

# generate test data, as distributed data frame
u <- matrix(rnorm(5000),ncol=2)
u[,2] <- u[,1] + u[,2]
distribsplit(cls,"u")
# wrapper for lm(), as our estf()
lmwrap <- function(u) {k <- ncol(u); lm(u[,k]~u[,-k])}
# apply the function
cabase(cls,"u",lmwrap,coef,vcov)
# check; results should be approximately the same
a <- lm(u[,2] ~ u[,1])
coef(a)
vcov(a)
}

\author{
Norm Matloff
}

