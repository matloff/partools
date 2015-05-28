\name{ca}
\alias{ca}

\title{Software Alchemy: Turning Complex Statistical Computations into
Embarrassingly-Parallel Ones}

\description{
Easy parallelization of most statistical computations.
}

\usage{
ca(cls,z,ovf,estf,estcovf=NULL,conv2mat=TRUE,findmean=TRUE)
}

\arguments{
  \item{cls}{A cluster run under the \pkg{parallel} package.}
  \item{z}{Data (data.frame, matrix or vector), one observation per
  row.}
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

\details{Implements the chunk-averaging method of N. Matloff, "Software
Alchemy: Turning Complex Statistical Computations into
Embarrassingly-Parallel Ones," http://arxiv.org/abs/1409.5827, to appear
in the Journal of Statistical Software.  

The data are broken into chunks, and the given estimator is applied to
each.  The results are averaged, and an empirical covariance matrix
computed.
}

\value{R list with components \code{thts}, the results of applying the
requested estimator to the chunks; and optionally, \code{tht}, 
the result of chunk averaging, and \code{thtcov}, the empirical 
covariance matrix of \code{tht}.
}

\author{
Norm Matloff
}

