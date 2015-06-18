
# chunks averaging, aka Software Alchemy, method (N. Matloff, "Parallel
# Computation for Data Science," Chapman and Hall, 2015)

############################## ca() ################################

# arguments:
#
#    cls: 'parallel' cluster
#    z:  data (data.frame, matrix or vector), or the quoted name of such
#        an object; in the latter case, the data is assumed to be
#        distributed among the cluster nodes, under the name z at each
#    ovf:  overall statistical function, say glm()
#    estf:  function to extract the point estimate (possibly
#           vector-valued) from the output of ovf(), e.g.
#           coef() in the glm() case
#    estcovf:  if provided, function to extract the estimated 
#              covariance matrix of the output of estf()  
#    conv2mat:  if TRUE, convert z to a matrix (useful if ovf() requires
#               matrix input)
#    findmean:  if TRUE, output the average of the estimates from the
#               chunks; otherwise, output the estimates themselves
# 
# value:
#
#    R list, consisting of the estimates from the chunks, and if
#    requested, their average and its estimated covariance matrix 
#
ca <- function(cls,z,ovf,estf,estcovf=NULL,conv2mat=TRUE,findmean=TRUE) {
   if (conv2mat) {
      if (is.data.frame(z)) z <- as.matrix(z)
      if (is.vector(z)) z <- matrix(z,ncol=1)
   }
   z168 <- z
   distribsplit(cls,"z168")
   cabase(cls,"z168",ovf,estf,estcovf,conv2mat,findmean) 
}

############################## cabase() ##############################

# arguments:
#
#    as in ca() above, except
#    z: quoted name of a distributed data frame/matrix 
# 
# value:
#
#    as in ca() above, except
#
cabase <- function(cls,z,ovf,estf,estcovf=NULL,conv2mat=TRUE,findmean=TRUE) {
   clusterExport(cls,c("ovf","estf","estcovf"),envir=environment())
   clusterCall(cls,function(zname) z168 <<- get(zname),z)
   wts <- unlist(clusterEvalQ(cls,nrow(z168)))
   wts <- wts / sum(wts)
   # apply the "theta hat" function, e.g. glm(), and extract the
   # apply the desired statistical method at each chunk
   ovout <- clusterEvalQ(cls,ovf(z168))
   # theta-hats
   thts <- lapply(ovout,estf)
   lth <- length(thts[[1]])
   tht <- rep(0.0,lth)
   # find the estimated covariance of the chunk-averaged estimate, if
   # requested
   if (!is.null(estcovf)) {
      thtcov <- matrix(0,nrow=lth,ncol=lth)
      thtcovs <- lapply(ovout,estcovf)
   }
   # res will be the returned result of this function
   res <- list()
   res$thts <- thts 
   # find the chunk-averaged theta hat, if requested
   if (findmean) {
      for (i in 1:length(cls)) {
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


