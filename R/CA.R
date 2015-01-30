
# chunks averaging method (N. Matloff, "Software Alchemy: Turning
# Complex Statistical Computations into Embarrassingly-Parallel Ones,"
# http://arxiv.org/abs/1409.5827), implemented for R's 'parallel' (formerly
# Snow) package

# arguments:
 
#    cls: 'parallel' cluster
#    z:  data (data.frame, matrix or vector), one observation per row
#    ovf:  overall statistical function, say glm()
#    estf:  function to extract the point estimate (possibly
#           vector-valued) from the output of ovf()
#    estcovf:  if provided, function to extract the estimated 
#              covariance matrix of the output of estf()  
#    conv2mat:  if TRUE, convert data framee input to a matrix
#    findmean:  if TRUE, output the average of the estimates from the
#               chunks; otherwise, output the estimates themselves
 
# value:

#    R list, consisting of the estimates from the chunks, and if
#    requested, their average, as well as, optionally, the 
#    estimated covariance matrix calculated from the chunks

ca <- function(cls,z,ovf,estf,estcovf=NULL,conv2mat=TRUE,findmean=TRUE) {
   # require(parallel)
   if (conv2mat) {
      if (is.data.frame(z)) z <- as.matrix(z)
      if (is.vector(z)) z <- matrix(z,ncol=1)
   }
   n <- nrow(z)
   # form the chunks and associated weights
   rowchunks <- clusterSplit(cls,1:n)
   chunks <- lapply(rowchunks,function(rowchunk) z[rowchunk,])
   ni <- sapply(rowchunks,length)  # chunk sizes
   wts <- ni / n  # weights in the averaging
   # apply the "theta hat" function, e.g. glm(), and extract the
   # theta-hats
   ovout <- clusterApply(cls,chunks,ovf)
   thts <- lapply(ovout,estf)
   lth <- length(thts[[1]])
   tht <- rep(0.0,lth)
   # find the estimated covariance of the chunk-averaged estimate, if
   # requested
   if (!is.null(estcovf)) {
      thtcov <- matrix(0,nrow=lth,ncol=lth)
      thtcovs <- lapply(ovout,estcovf)
   }
   # res will be the returned result
   res <- list()
   res$thts <- thts 
   # find the chunk-averaged theta hat, if requested
   if (findmean) {
      for (i in 1:length(thts)) {
         wti <- wts[i]
         tht <- tht + wti * thts[[i]]
         if (!is.null(estcovf)) 
            thtcov <- thtcov + wti^2 * thtcovs[[i]]
      }
      res$tht <- tht
      if (!is.null(estcovf)) res$thtcov <- thtcov
   } 
   res
}


