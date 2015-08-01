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
ca(cls,z,ovf,estf,estcovf=NULL,findmean=TRUE,scramble=FALSE)
cabase(cls,ovf,estf,estcovf=NULL,findmean=TRUE,cacall=FALSE,z=NULL,scramble=FALSE)
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
     \code{ca}}
  \item{scramble}{If this and \code{cacall} are TRUE, randomize the data
     before distributing.}
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

\details{Implements the ``Software Alchemy'' (SA) method for
parallelizing statistical computations (N. Matloff, \emph{Parallel
Computation for Data Science}, Chapman and Hall, 2015, research article
to appear in the \emph{Journal of Statistical Software}.)  This can
result in substantial speedups in computation, as well as address limits
on physical memory.

The method involves breaking the data into chunks, and then applying the
given estimator to each one.  The results are averaged, and an estimated
covariance matrix computed (optional).  

Except for \code{ca}, it is assumed that the chunking has already been
done, say via \code{distribsplit} or \code{readnscramble}. 

Note that in \code{cabase}, the data object is not specified explicitly
in the argument list.  This is done through the function \code{ovf}.

Key point:  The SA estimator is statistically equivalent to the
original, nonparallel one, in the sense that they have the SAME
asymptotic statistical accuracy.  Neither the non-SA nor the SA
estimator is "better" than the other, and usually they will be quite
close to each other anyway.  Since we would use SA only with large data
sets anyway (otherwise, parallel computation would not be needed for
speed), the asymptotic aspect should not be an issue.  In other words,
with SA we achieve the same statistical accuracy while possibly
attaining much faster computation.

Wrapper functions, applying SA to the corresponding R
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
expose some instability in the original (i.e. non-SA) algorithm.  With
\code{prcomp}, for instance, the eigenvectors corresponding to the
smaller eigenvalues may have high variances in the nonparallel version,
which will be reflected in large differences from chunk to chunk in SA,
visible in \code{thts}.  Note that this reflects a fundamental problem
with the algorithm on the given data set, not due to Software Alchemy;
on the contrary, an important advantage of the SA approach is to expose
such problems.

}

\examples{

# set up 'parallel' cluster
cls <- makeCluster(2)
setclsinfo(cls)

# generate simulated test data, as distributed data frame
n <- 10000
p <- 2
tmp <- matrix(rnorm((p+1)*n),nrow=n)
u <- tmp[,1:p]  # "X" values
# add a "Y" col
u <- cbind(u,u \%*\% rep(1,p) + tmp[,p+1])
# now in u, cols 1,2 are the "X" variables, and col 3 is "Y", 
# with regress coefs (0,1,1), with tmp[,p+1] being the error term
distribsplit(cls,"u")  # form distributed d.f.
# apply the function
#### calm(cls,"u[,3] ~ u[,1]+u[,2]")$tht
calm(cls,"V3 ~ .,data=u")$tht
# check; results should be approximately the same
lm(u[,3] ~ u[,1]+u[,2])
# without the wrapper
ovf <- function() lm(V3 ~ .,data=z168)
ca(cls,u,ovf,estf=coef,estcovf=vcov)$tht

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

stopCluster(cls)

}

\author{
Norm Matloff
}

