\name{caclassfit,caclasspred,vote,re_code}
\alias{caclassfit}
\alias{caclasspred}
\alias{vote}
\alias{re_code}

\title{Software Alchemy for Machine Learning}

\description{
Parallelization of machine learning algorithms. 
}

\usage{
caclassfit(cls,fitcmd) 
caclasspred(fitobjs,newdata,yidx=NULL,...)
vote(preds)
re_code(x)
}

\arguments{
  \item{cls}{A cluster run under the \pkg{parallel} package.}
  \item{fitcmd}{A string containing a model-fitting command to be
     run on each cluster node.  This will typically include
     specification of the distributed data set.}
  \item{fitobjs}{An R list of objects returned by the \code{fitcmd}
     calls.}
  \item{newdata}{Data to be predicted from the fit computed by
  \code{caclassfit}.}
  \item{yidx}{If provided, index of the true class values in
     \code{newdata}, typically in a cross-validation setting.}
  \item{...}{Arguments to be passed to the underlying prediction
     function for the given method, e.g. \code{predict.rpart}.}
  \item{preds}{A vector of predicted classes, from which the "winner"
     will be selected by voting.}
  \item{x}{A vector of integers, in this context class codes.}
}

\details{This should work for almost any classification code that has a
``fit'' function and a \code{predict} method.

The method assumes i.i.d. data.  If your data set had been stored in
some sorted order, it must be randomized first, say using the
\code{scramble} option in \code{distribsplit} or by calling
\code{readnscramble}, depending on whether your data is already in
memory or still in a file.

It is assumed that class labels are 1,2,...  If not, use
\code{re_code}.

}

\value{The \code{caclassfit} function returns an R list of objects as in
\code{fitobjs} above.

The \code{caclasspred} function returns an R list with these components:

   \itemize{
   
      \item \code{predmat}, a matrix of predicted classes for
      \code{newdata}, one row per cluster node
   
      \item \code{preds}, the final predicted classes, after using
      \code{vote} to resolve possible differences in predictions among
      nodes
   
      \item \code{consensus}, the proportion of cases for which all
      nodes gave the same predictions (higher values indicating more
      stability)

      \item \code{acc}, if \code{yidx} is non-NULL, the proportion of
      cases in which \code{preds} is correct

      \item \code{confusion}, if \code{yidx} is non-NULL, the confusion matrix
      
   }

}

\examples{
\dontrun{
# set up 'parallel' cluster
cls <- makeCluster(2)
setclsinfo(cls)
# data prep
data(prgeng)
prgeng$occ <- re_code(prgeng$occ)
prgeng$bs <- as.integer(prgeng$educ == 13)
prgeng$ms <- as.integer(prgeng$educ == 14)
prgeng$phd <- as.integer(prgeng$educ == 15)
prgeng$sex <- prgeng$sex - 1
pe <- prgeng[,c(1,7,8,9,12,13,14,5)]
pe$occ <- as.factor(pe$occ)   # needed for rpart!
# go
distribsplit(cls,'pe')
library(rpart)
clusterEvalQ(cls,library(rpart))
fit <- caclassfit(cls,"rpart(occ ~ .,data=pe)")
predout <- caclasspred(fit,pe,8,type='class')
predout$acc  # 0.36 

stopCluster(cls)
}
}

\author{
Norm Matloff
}

