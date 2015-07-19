\name{ca,cabase,calm,caglm,caprcomp,cakm,cameans,caquantile,caagg}
\alias{ca}
\alias{cabase}
\alias{calm}
\alias{caglm}
\alias{caprcomp}
\alias{cakm}
\alias{cameans}
\alias{caquantile}
\alias{caagg}

\title{Software Alchemy: Turning Complex Statistical Computations into
Embarrassingly-Parallel Ones}

\description{
Easy parallelization of most statistical computations.
}

\usage{
ca(cls,z,ovf,estf,estcovf=NULL,conv2mat=TRUE,findmean=TRUE)
cabase(cls,ovf,estf,estcovf=NULL,findmean=TRUE,cacall=FALSE)
calm(cls,lmargs) 
caglm(cls,glmargs) 
caprcomp(cls,prcompargs, p)
cakm(cls,mtdf,ncenters,p)
cameans(cls,cols,na.rm=FALSE) 
caquantile(cls,vec, probs = c(0.25, 0.5, 0.75),na.rm=FALSE) 
caagg(cls,ynames,xnames,dataname,FUN)
}

\arguments{
  \item{cls}{A cluster run under the \pkg{parallel} package.}
  \item{z}{A data frame, matrix or vector, one observation per row/element.} 
  \item{ovf}{Overall statistical function, say \code{glm}.}
  \item{estf}{Function to extract the point estimate (typically
     vector-valued) from the output of \code{ovf}.}
  \item{estcovf}{If provided, function to extract the estimated 
     covariance matrix of the output of \code{estf}}.
  \item{conv2mat}{If TRUE, convert data frame input to a matrix 
     (needed for some cases of 'ovf').}
  \item{findmean}{If TRUE, output the average of the estimates from the
     chunks; otherwise, output only the estimates themselves.}
  \item{lmargs}{Quoted string representing arguments to \code{lm},
     e.g. R formula, \code{data} specification.}
  \item{glmargs}{Quoted string representing arguments to \code{glm},
     e.g. R formula, \code{data} specification, and \code{family}
     argument.}
  \item{prcompargs}{Quoted string representing arguments to 
     \code{prcomp}.}
  \item{p}{Number of columns in data}
  \item{na.rm}{If TRUE, remove NA values from the analysis.}
  \item{mtdf}{Quoted name of a distributed matrix or data frame.}
  \item{ncenters}{Number of clusters to find.}
  \item{cacall}{If TRUE, indicates that \code{cabase} had been called by
     \code{ca}.}
  \item{cols}{A quoted string that evaluates to a data frame or matrix.}
  \item{vec}{A quoted string that evaluates to a vector.}
  \item{ynames}{A vector of quoted variable names.}
  \item{xnames}{A vector of quoted variable names.}
  \item{dataname}{Quoted name of a data frame or matrix.}
  \item{probs}{As in the argument with the same name in
     \code{quantile}. Should not be 0.00 or 1.00, as asymptotic
     normality doesn't hold.}
  \item{FUN}{Quoted name of a function.}
}

\details{Implements the ``Software Alchemy'' method for parallelizing
statistical computations (N. Matloff, \emph{Parallel Computation for
Data Science}, Chapman and Hall, 2015, research article to appear in the
\emph{Journal of Statistical Software}.)  This can result in substantial
speedups in computation.

The data are broken into chunks, and the given estimator is applied to
each.  The results are averaged, and an estimated covariance matrix
computed (optional).

In \code{cabase}, the data object is assumed to be a distributed data
frame or matrix, produced by \code{distribsplit} or
\code{readnscramble}. Note by the way that the data object is not
specified explicitly in the argument list; this is done through the
function \code{ovf}.

The key point is that the resulting estimator is statistically
equivalent to the original, nonparallel one, in the sense that they have
the same asymptotic statistical accuracy.  Since one would use Software
Alchemy only with large data sets anyway (otherwise, parallel
computation would not be needed for speed), the asymptotic aspect should
not be an issue.  In other words, one achieves the same statistical
accuracy while possibly attaining much faster computation.

Wrapper functions, applying Software Alchemy to the corresponding R
function (or function elsewere in this package):

   \itemize{

      \item \code{calm}: Wrapper for \code{lm}.

      \item \code{caglm}: Wrapper for \code{glm}.

      \item \code{caprcomp}: Wrapper for \code{prcomp}.

      \item \code{cakm}: Wrapper for \code{kmeans}.

      \item \code{cameans}: Wrapper for \code{colMeans}.

      \item \code{caquantile}: Wrapper for \code{quantile}.

      \item \code{caagg}: Like \code{distribagg}, but finds the
         average value of \code{FUN} across the cluster nodes.

   }

A note on NA values:  Some R functions such as \code{lm}, \code{glm} and
\code{prcomp} have an \code{na.action} argument.  The default is
\code{na.omit}, which means that cases with at least one NA value will
be discarded. (This is also settable via \code{options()}.) However,
\code{na.omit} seems to have no effect in \code{prcomp} unless that
function's \code{formula} option is used. When in doubt, apply the
function \code{na.omit} directly; e.g. \code{na.omit(d)} for a data
frame \code{d} returns a data frame consisting of only the intact rows of
\code{d}.

The method assumes that the base estimator is asymptotically normal, and
assumes i.i.d. data.  If your data set had been stored in some sorted
order, it must be randomized first, say using the \code{scramble} option
in \code{distribsplit} or by calling \code{readnscramble}, depending on
whether your data is already in memory or still in a file.

}

\value{R list with these components:

   \itemize{
   
      \item \code{thts}, the results of applying the requested estimator to
      the chunks; the estimator from chunk i is in row i
   
      \item \code{tht}, the chunk-averaged overall estimator, if requested
   
      \item \code{thtcov}, the estimated covariance matrix of \code{tht},
      if available
      
   }

The wrapper functions return the following list elements:

   \itemize{

      \item \code{calm}, \code{caglm}: estimated regression coefficients
      and their estimated covariance matrix

      \item \code{caprcomp}: \code{sdev} (square roots of the
      eigenvalues) and \code{rotation}, as with \code{prcomp};
      \code{thts} is returned as well.

      \item \code{cakm}: \code{centers} and \code{size}, as with
      \code{kmeans}; \code{thts} is returned as well.

   }

The wrappers that return \code{thts} are useful for algorithms that may
exhibit some instability.  For \code{prcomp}, for instance, the
eigenvectors corresponding to the smaller eigenvalues may have high
variances in the nonparallel version, which will be reflected in large
differences from chunk to chunk; thus \code{caprcomp} returns the
\code{thts} element from the output of \code{cabase}.  Note that this
reflects a fundamental problem with the algorithm on these variables,
not due to Software Alchemy; on the contrary, this is an important
advantage of the Software Alchemy approach.

}

\examples{

# set up 'parallel' cluster
cls <- makeCluster(2)
setclsinfo(cls)

# generate simulated test data, as distributed data frame
n <- 25000
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

# find mean age for all combinations of the cit and sex variables
caagg(cls,"age",c("cit","sex"),"prgeng","mean") 
# compare to nonparallel
aggregate(age ~ cit+sex,data=prgeng,mean)  

data(newadult) 
distribsplit(cls,"newadult") 
caglm(cls," gt50 ~ ., family = binomial,data=newadult")$tht 

caprcomp(cls,'newadult,scale=TRUE',5)$sdev
prcomp(newadult,scale=TRUE)$sdev

cameans(cls,"prgeng")
cameans(cls,"prgeng[,c('age','wageinc')]")
caquantile(cls,'prgeng$age')

pe <- prgeng[,c(1,3,8)] 
distribsplit(cls,"pe") 
z1 <- cakm(cls,'pe',3,3); z1$size; z1$centers 
# check algorithm unstable
z1$thts  # looks unstable

}

\author{
Norm Matloff
}

