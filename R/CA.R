
# chunks averaging, aka Software Alchemy, method (N. Matloff, "Parallel
# Computation for Data Science," Chapman and Hall, 2015)

# use cabase() if already have a distributed data frame; otherwise, use
# the ca() wrapper

############################## ca() ################################

# arguments:
#
#    cls: 'parallel' cluster
#    z:  data (data.frame, matrix or vector)
#    ovf:  overall statistical function, say glm()
#    estf:  function to extract the point estimate (possibly
#           vector-valued) from the output of ovf(), e.g.
#           coef() in the glm() case
#    estcovf:  if provided, function to extract the estimated 
#              covariance matrix of the output of estf(), e.g.
#              vcov() for glm()
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
#    dataname: quoted name of a distributed data frame/matrix 
#
#    remainder as in ca() above, except:
# 
# value:
#
#    as in ca() above
#
cabase <- function(cls,dataname,ovf,estf,estcovf=NULL,conv2mat=TRUE,findmean=TRUE) {
   clusterExport(cls,c("ovf","estf","estcovf"),envir=environment())
   clusterCall(cls,function(zname) z168 <<- get(zname),dataname)
   # apply the "theta hat" function, e.g. glm(), and extract the
   # apply the desired statistical method at each chunk
   ovout <- clusterEvalQ(cls,ovf(z168))
   # theta-hats, with the one for chunk i in row i
   thts <- t(sapply(ovout,estf))
   # res will be the returned result of this function
   res <- list()
   res$thts <- thts 
   # find the chunk-averaged theta hat and estimated covariance matrix, 
   # if requested
   if (findmean) {
      # dimensionality of theata
      lth <- length(thts[1,])
      # theta-hat is a weighted average of the ests at the chunks
      nrows <- unlist(clusterEvalQ(cls,nrow(z168)))
      wts <- nrows / sum(nrows)
      # compute mean
      tht <- rep(0.0,lth)
      for (i in 1:length(cls)) {
         wti <- wts[i]
         tht <- tht + wti * thts[i,]
      }
      res$tht <- tht
      # compute estimated covariance matrix, if requested
      # (code structured to possibly add empirical cov est later)
      if (!is.null(estcovf)) {
         thtcov <- matrix(0,nrow=lth,ncol=lth)
         for (i in 1:length(cls)) {
            wti <- wts[i]
            summand <- wti^2 * estcovf(ovout[[i]]) 
            thtcov <- thtcov + summand
         }
         res$thtcov <- thtcov
      } 
   }
   clusterEvalQ(cls,rm(ovf))
   res
}

# ca() wrapper for lm()
#
# arguments:
#
#    cls: cluster
#    dataname: quoted name of a distributed data frame
#    forla: quoted string representing R formula, 
#           e.g. "weight ~ height + age"
#
# value: Software Alchemy estimate, statistically equivalent to direct
#    nonparallel call to lm(); R list is in value of ca() 
#
calm <- function(cls,dataname,forla) {
   ovf <- function(u) {
      tmp <- paste("lm(",forla,",data=",dataname,")",collapse="")
      docmd(tmp)
   }
   cabase(cls,dataname,ovf,coef,vcov)
}

# ca() wrapper for glm()
# arguments and value as in calm()
caglm <- function(cls,dataname,forlafam) {
   ovf <- function(u) {
      tmp <- paste("glm(",forlafam,",data=",dataname,")",collapse="")
      docmd(tmp)
   }
   cabase(cls,dataname,ovf,coef,vcov)
}



