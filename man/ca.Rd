\name{ca,cabase,calm,caglm}
\alias{ca}
\alias{cabase}
\alias{calm}
\alias{caglm}

\title{Software Alchemy: Turning Complex Statistical Computations into
Embarrassingly-Parallel Ones}

\description{
Easy parallelization of most statistical computations.
}

\usage{
ca(cls,z,ovf,estf,estcovf=NULL,conv2mat=TRUE,findmean=TRUE)
cabase(cls,ovf,estf,estcovf=NULL,conv2mat=TRUE,findmean=TRUE,cacall=FALSE)
calm(cls,dataname,lmargs) 
caglm(cls,dataname,glmargs) 
}

\arguments{
  \item{cls}{A cluster run under the \pkg{parallel} package.}
  \item{z}{A data frame, matrix or vector, one observation per row/element.} 
  \item{dataname}{Quoted name of a distributed data frame, such as is 
     produced by \code{distribsplit} or \code{readnscramble}.}
  \item{ovf}{Overall statistical function, say \code{glm}.}
  \item{estf}{Function to extract the point estimate (possibly
     vector-valued) from the output of \code{ovf}.}
  \item{estcovf}{if provided, function to extract the estimated 
     covariance matrix of the output of \code{estf}}.
  \item{conv2mat}{If TRUE, convert data framee input to a matrix 
     (needed for some cases of 'ovf').}
  \item{findmean}{If TRUE, output the average of the estimates from the
     chunks; otherwise, output only the estimates themselves.}
  \item{lmargs}{Quoted string representing arguments to \code{lm},
     e.g. R formula, \code{data} specification.}
  \item{glmargs}{Quoted string representing arguments to \code{glm},
     e.g. R formula, \code{data} specification, and \code{family}
     arguments.}
  \item{cacall}{If TRUE, indicates that \code{cabase} had been called by
  \code{cac}.}
}

\details{Implements the ``Software Alchemy'' method for parallelizing
statistical computations (N. Matloff, \emph{Parallel Computation for
Data Science}, Chapman and Hall, 2015.  This can result in substantial
speedups in computation.

The data are broken into chunks, and the given estimator is applied to
each.  The results are averaged, and an estimated covariance matrix
computed.

The key point is that the resulting estimator is statistically
equivalent to the original, nonparallel one, in the sense that they have
the same asymptotic statistical accuracy.  Since one would use Software
Alchemy only with large data sets anyway (otherwise, parallel
computation would not be needed), the asymptotic aspect should not be an
issue.

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

# set up 'parallel' cluster
cls <- makeCluster(2)
setclsinfo(cls)

# generate simulated test data, as distributed data frame
n <- 5000
u <- matrix(nrow=n,ncol=4)
u[,1:3] <- rnorm(3*n)
u[,4] <- u[,1] + 2*u[,2] + u[,3]
distribsplit(cls,"u")
# apply the function
calm(cls,"u[,4] ~ u[,1]+u[,2]")$tht
# check; results should be approximately the same
lm(u[,4] ~ u[,1]+u[,2])

# Census data on programmers and engineers; include a quadratic term for
# age, due to nonmonotone relation to income
data(prgeng) 
distribsplit(cls,"prgeng") 
caout <- calm(cls,"wageinc ~ age+I(age^2)+sex+wkswrkd,data=prgeng")
caout$tht
# compare to nonparallel
lm(wageinc ~ age+I(age^2)+sex+wkswrkd,data=prgeng)
# get standard errors of the beta-hats
sqrt(diag(caout$thtcov))

data(newadult) 
distribsplit(cls,"newadult") 
caglm(cls," gt50 ~ ., family = binomial,data=newadult")$tht 
# compare to nonparallel
glm(gt50 ~ .,family=binomial,data=newadult) 

}

\author{
Norm Matloff
}

