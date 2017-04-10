
# chunks averaging, aka Software Alchemy, method (N. Matloff, "Parallel
# Computation for Data Science," Chapman and Hall, 2015)

############################## ca() ################################

# wrapper for cabase(), e.g. for case where the data is not already
# distributed

# arguments:
#
#    cls: 'parallel' cluster
#    z:  (non-distributed) data (data.frame, matrix or vector)
#    ovf:  overall statistical function, say glm()
#    estf:  function to extract the point estimate (possibly
#           vector-valued) from the output of ovf(), e.g.
#           coef() in the glm() case
#    estcovf:  if provided, function to extract the estimated 
#              covariance matrix of the output of estf(), e.g.
#              vcov() for glm()
#    findmean:  if TRUE, output the average of the estimates from the
#               chunks; otherwise, output the estimates themselves
#    scramble:  randomize the order of z before distributing
# 
# value:
#
#    R list, consisting of the estimates from the chunks, thts, and if
#    requested, their average tht and its estimated covariance matrix 
#    thtcov

ca <- function(cls,z,ovf,estf,estcovf=NULL,findmean=TRUE,
         scramble=FALSE) {
   if (is.vector(z)) z <- data.frame(z)
   cabase(cls,ovf,estf,estcovf,findmean,cacall=TRUE,z=z,scramble=scramble) 
}

############################## cabase() ##############################

# serves as a manager for the Software Alchemy method, arranging for the
# estimates to be computed at each data chunk, gathering the results,
# averaging them etc.

# other than the case cacall = TRUE, ca() is not directly aware of the
# data chunks, e.g. their names, sizes etc.; it merely calls the
# user-supplied ovf(), which does that work

# arguments:
#
#    as in ca() above, except:
#
#    cacall: TRUE if cabase() was invoked by ca() 
#    z: if cacall is TRUE, the (non-distributed) data frame; the
#       data will be distributed among the cluster nodes, under the name
#       "z168"
#    scramble: if this and cacall are TRUE, randomize z before
#              distributing
#
# value: 
#
#    as in ca() above

cabase <- function(cls,ovf,estf, estcovf=NULL,findmean=TRUE,
             cacall=FALSE,z=NULL,scramble=FALSE) {
   if (cacall) {
      z168 <- z  # needed for CRAN issue
      distribsplit(cls,'z168',scramble=scramble)
   } else clusterEvalQ(cls,z168 <- NULL)
   clusterExport(cls,c("ovf","estf","estcovf"),envir=environment())
   # apply the "theta hat" function, e.g. glm(), and extract the
   # apply the desired statistical method at each chunk
   ovout <- ### if (cacall) clusterEvalQ(cls,ovf(z168)) else
                        clusterEvalQ(cls,ovf(z168)) 
   # theta-hats, with the one for chunk i in row i
   # thts <- t(sapply(ovout,estf))
   tmp <- lapply(ovout, estf)
   thts <- Reduce(rbind,tmp)
   if (is.vector(thts)) thts <- matrix(thts,ncol=1)
   # res will be the returned result of this function
   res <- list()
   res$thts <- thts 
   # find the chunk-averaged theta hat and estimated covariance matrix, 
   # if requested
   if (findmean) {
      # dimensionality of theta
      lth <- length(thts[1,])
      # since the chunk sizes will differ by a negligible amount, 
      # so use equal weights; commented-out code below allowed for
      # varying weights
      ## wts <- rep(1 / length(cls), length(cls))
      ## # compute mean (leave open for restoring weights in future
      ## # version)
      ## tht <- rep(0.0,lth)
      ## for (i in 1:length(cls)) {
      ##    wti <- wts[i]
      ##    tht <- tht + wti * thts[i,]
      ## }
      tht <- colMeans(thts)
      attr(tht,"p") <- attr(thts[1,],"p")
      res$tht <- tht
      # compute estimated covariance matrix; if requested, this will be
      # done from outputs of the calls on the chunks, e.g. from vcov();
      # otherwise, compute the matrix empirically
      nchunks <- length(cls)
      if (!is.null(estcovf)) {
         thtcov <- matrix(0,nrow=lth,ncol=lth)
         for (i in 1:nchunks) {
            summand <- estcovf(ovout[[i]]) 
            if (any(dim(thtcov) != dim(summand))) {
               print('dimension mismatch')
               stop('likely cause is constant variable in some chunk')
            }
            thtcov <- thtcov + summand
         }
         res$thtcov <- (thtcov / nchunks) / nchunks
      } else {
         res$thtcov <- cov(thts) / nchunks
      }
   }
   clusterEvalQ(cls,rm(ovf))
   res
}

# ca() wrapper for lm()
#
# arguments:
#    cls: cluster
#    lmargs: quoted string representing arguments to lm()
#            e.g. "weight ~ height + age, data-mlb"
# value: Software Alchemy estimate, statistically equivalent to direct
#    nonparallel call to lm(); R list is value of ca() 
#
calm <- function(cls,lmargs) {
   ovf <- function(u) {
      tmp <- paste("lm(",lmargs,")",collapse="")
      docmd(tmp)
   }
   cabase(cls,ovf,coef,vcov)
}

# ca() wrapper for glm()
# arguments and value as in calm(), except that it should include
# specification of family
caglm <- function(cls,glmargs) {
   ovf <- function(u) {
      tmp <- paste("glm(",glmargs,")",collapse="")
      docmd(tmp)
   }
   cabase(cls,ovf,coef,vcov)
}

# ca() wrapper for knnest(); 
# arguments:
#    cls: cluster
#    yname: name of distributed Y vector
#    k: number of nearest neighbors
#    xname: if nonempty, this is the distributed matrix of X values, 
#           which will be fed into preprocessx() to produce the 
#           global variable xdata at the nodes; if empty, xdata will be
#           generated
# value:
#    none at caller; xdata, kout are left there at the nodes 
#    for future use
caknn <- function(cls,yname,k,xname='') {
   if (xname != '') {
      # run preprocessx() to generate xdata
      cmd <- paste('xdata <<- preprocessx(',xname,',',k,')',sep='')
      doclscmd(cls,cmd)
   }
   cmd <- paste('kout <<- knnest(',yname,',xdata,',k,')',sep='')
   doclscmd(cls,cmd)
}

# needs fixing
### # kNN predict
### # arguments:
### #    predpts: matrix/df of X values at which to find est. reg. ftn.;
### #             if NULL, it is assumed that predpts already exists at the
### #             nodes
### # value:
### #    est. reg. ftn. value at those points
### # assumes kout present at the nodes
### caknnpred <- function(cls,predpts) {
###    if (!is.matrix(predpts)) 
###       predpts <- as.matrix(predpts)
###    clusterEvalQ(cls,library(regtools))
###    if (!is.null(predpts))
###       clusterExport(cls,'predpts',envir=environment())
###    predys <- doclscmd(cls,'predy <<- predict(kout,predpts)')
###    predys <- Reduce(rbind,predys)
###    colMeans(predys)
### }

# ca() wrapper for prcomp()
# arguments:
#
#    prcompargs: arguments to go into prcomp()
#    p: number of variables (cannot be inferred by the code, since
#       prcompargs can be so general
#
# value:
#
#    R list, with componentns:
#
#       sdev, rotation: as in prcomp()
#       thts: the sdev/rotation results of applying prcomp()
#             to the individual data chunksb
caprcomp <- function(cls,prcompargs,p) {
   ovf <- function(u) {
      tmp <- paste("prcomp(",prcompargs,")",collapse="")
      docmd(tmp)
   }
   # we're interested in both sdev and rotation, so string them together
   # into one single vector for averaging
   estf <- function(pcout) c(pcout$sdev,pcout$rotation)
   cabout <- cabase(cls,ovf,estf)
   # now extract the sdev and rotation parts back
   tmp <- cabout$tht
   res <- list()
   res$sdev <- tmp[1:p]
   res$rotation <- matrix(tmp[-(1:p)],ncol=p)
   res$thts <- cabout$thts
   res
}

# ca() wrapper for kmeans()
#
# arguments:
#
#    mtdf: matrix or data frame
#    ncenters: number of clusters to find
#    p: number of variables
#
# value: sdev and rotation from kmeans() output, plus thts to explore
# possible instability
#
cakm <- function(cls,mtdf,ncenters,p) {
   # need the same initial centers for each node; can take the first few
   # records from the first chunk, since the data is assumed to be
   # randomized
   hdcmd <- paste('head(',mtdf,',',ncenters,')',sep='')
   clusterExport(cls,'hdcmd',envir=environment())
   ctrs <- clusterEvalQ(cls,eval(parse(text=hdcmd)))[[1]]
   clusterExport(cls,'ctrs',envir=environment())
   ovf <- function(u) {
      tmp <- paste("kmeans(",mtdf,",centers=ctrs)",collapse="",sep="")
      docmd(tmp)
   }
   # as in caprcomp(), string the output entities together, then later
   # extract them back
   estf <- function(kmout) c(kmout$size,kmout$centers)
   kmout <- cabase(cls,ovf,estf)
   ncenters <- length(kmout$tht) / (1+p)
   tmp <- kmout$tht
   res <- list()
   res$size <- round(tmp[1:ncenters] * length(cls))
   res$centers <- matrix(tmp[-(1:ncenters)],ncol=p)
   res$thts <- kmout$thts
   res
}

# finds the means of the columns in the string expression cols
cameans <- function(cls,cols,na.rm=FALSE) {
   ovf <- function(u) {
      narm <- if(na.rm) 'TRUE' else 'FALSE'  
      narm <- paste("na.rm=",narm)
      tmp <- paste("colMeans(",cols,",",narm,")",collapse="")
      docmd(tmp)
   }
   estf <- function(z) z
   cabase(cls,ovf,estf)
}

# finds the quantiles of the vector defined in the string vec
caquantile <- function(cls,vec,probs=c(0.25,0.50,0.75),na.rm=FALSE) {
   ovf <- function(u) {
      probs <- paste(probs,collapse=",")
      probs <- paste("c(",probs,")",sep="")
      narm <- if(na.rm) 'TRUE' else 'FALSE'  
      narm <- paste("na.rm=",narm)
      tmp <- paste("quantile(",vec,",",probs,",",narm,")",collapse="")
      docmd(tmp)
   }
   estf <- function(z) z
   cabase(cls,ovf,estf)
}

# a Software Alchemy version of aggregate()/distribagg(); finds the
# groups, applying FUN to each, then averaging across the cluster
caagg <- function(cls,ynames,xnames,dataname,FUN) {
   distribagg(cls,ynames,xnames,dataname,FUN,"mean")
}
