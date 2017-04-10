
if (getRversion() >= "2.15.1")  utils::globalVariables(c("toexec", "tmpx"))

# general "Snow" (the part of 'parallel' adapted from the old Snow)
# utilities, some used in Snowdoop but generally applicable


# split matrix/data frame into chunks of rows, placing a chunk into each
# of the cluster nodes
#
# arguments:
#
# cls: a 'parallel' cluster
# m: a matrix, data frame or data.table
# mchunkname: name to be given to the chunks of m at the cluster nodes
# scramble: if TRUE, randomly assign rows of the data frame to the chunks; 
#           otherwsie, the first rows go to the first chunk, the next set
#           of rows go to the second chunk, and so on
#
# places the chunk named mchunkname in the global space of the worker
formrowchunks <- function(cls,m,mchunkname,scramble=FALSE) {
   nr <- nrow(m)
   idxs <- if (!scramble) 1:nr else sample(1:nr,nr,replace=FALSE)
   rcs <- clusterSplit(cls,idxs)
   rowchunks <- lapply(rcs, function(rc) m[rc,])
   # need to write chunk i to global at worker node i
   # need very convoluted workaround to avoid CRAN check's aversion
   # to globals, even though these globals are only at the worker
   # nodes; Uwe Ligges of CRAN advised workaround based on
   # globalVariables(), but this didn't work here; anyway, Uwe was big
   # on having CRAN automated, i.e. no special human intervention, so he
   # wants a workaround, and here it is; it involves a "surreptitious"
   # assign 
   invisible(
      clusterApply(
         cls,rowchunks,  
         function(onechunk) {
            cmd <- 
               paste('assign("',mchunkname,'",onechunk,envir = .GlobalEnv)',
                  sep='')
            eval(parse(text = cmd))
         }
      )
   )
}

# extracts rows (rc=1) or columns (rc=2) of a matrix, producing a list
matrixtolist <- function(rc,m) {
   if (rc == 1) {
      Map(function(rownum) m[rownum,],1:nrow(m))
   } else Map(function(colnum) m[,colnum],1:ncol(m))
}

# "add" 2 lists, applying the operation 'add' to elements in common,
# copying non-null others
addlists <- function(lst1,lst2,add) {
   lst <- list()
   for (nm in union(names(lst1),names(lst2))) {
      if (is.null(lst1[[nm]])) lst[[nm]] <- lst2[[nm]] else
      if (is.null(lst2[[nm]])) lst[[nm]] <- lst1[[nm]] else
      lst[[nm]] <- add(lst1[[nm]],lst2[[nm]])
   }
   lst
}

# give each node in the cluster cls an ID number myid, global to that
# node; does the same for nclus, the number of nodes in the cluster
setclsinfo <- function(cls) {
   clusterEvalQ(cls,partoolsenv <- new.env())
   clusterApply(cls,1:length(cls),function(i) tmpvar <<- i)
   clusterEvalQ(cls,partoolsenv$myid <- tmpvar)
   tmpvar <- length(cls)
   clusterExport(cls,"tmpvar",envir=environment())
   clusterEvalQ(cls,partoolsenv$ncls <- tmpvar)
   exportlibpaths(cls)
   # clusterEvalQ(cls,library(partools))
   doclscmd(cls,'library(partools)')
}

# returns a pointer to partoolsenv
getpte <- function() {
   # get("partoolsenv",envir=.GlobalEnv)
   get("partoolsenv",envir=.GlobalEnv)
}

# set the R library paths at the workers to that of the calling node
exportlibpaths <- function(cls) {
   lp <- .libPaths()
   clusterCall(cls,function(p) .libPaths(p),lp)
}

# split a vector/matrix/data frame into approximately equal-sized
# chunks across a cluster 
#
# arguments:
#
# cls: a 'parallel' cluster
# dfname: quoted name of matrix/data frame to be split
# scramble: if TRUE, randomly assign rows of the data frame to the chunks; 
#           otherwsie, the first rows go to the first chunk, the next set
#           of rows go to the second chunk, and so on
#
# each remote chunk, a data frame, will have the same name as the
# full object
distribsplit <- function(cls,dfname,scramble=FALSE) {
   dfr <- get(dfname,envir=sys.parent())
   if(!is.data.table(dfr)) dfr <- as.data.frame(dfr)
   formrowchunks(cls,dfr,dfname,scramble)
   for (j in 1:ncol(dfr)) {
      mdj <- mode(dfr[[j]])
      if (mdj == 'character') 
         warning('character column converted to factor') else
      if (mdj == 'factor') {
         usubj <- dfname
         ipstrcat(usubj,'[,')
         ipstrcat(usubj,as.character(j))
         ipstrcat(usubj,']')
         remotecmd <- usubj
         ipstrcat(remotecmd,' <- ')
         ipstrcat(remotecmd,'as.factor(')
         ipstrcat(remotecmd,usubj)
         ipstrcat(remotecmd,')')
         clusterEvalQ(cls,docmd(remotecmd))
      }
   }
}

# collects a distributed matrix/data frame specified by dfname at
# manager (i.e. caller), again with the name dfname, in global space of
# the latter
distribcat <- function(cls,dfname) {
   toexec <- paste("clusterEvalQ(cls,",dfname,")")
   tmp <- eval(parse(text=toexec))
   Reduce(rbind,tmp)
}

# distributed version of aggregate()

# arguments:

#    cls: cluster
#    ynames: names of variables on which FUN is to be applied
#    xnames: names of variables that define the grouping
#    dataname: quoted name of the data frame or data.table
#    FUN: quoted name(s) of aggregation function to be used in 
#         aggregation within cluster nodes; for a data frame,
#         this function has a single argument; in the case of 
#         a data.table, this will be a vector of length the 
#         same as the length of ynames (or recycling will be used)
#    FUN1: quoted name of the aggregation function to be used in 
#          aggregation between cluster nodes

# e.g. for the call at the nodes
#
#    aggregate(cbind(mpg,disp,hp) ~ cyl+gear,data=mtcars,FUN=max)
#
# we would call
#
#    distribagg(cls,c("mpg","disp","hp"),c("cyl","gear"),mtcars,max)

# return value: aggregate()-style data frame, with column of cell counts
# appended at the end

# currently cannot have FUNdim > 1 for data.tables;  duplicate ynames;
#
# distribagg(cls,c('mpg','mpg','disp','disp','hp','hp'),
#             c('cyl','gear'),'mtc1',
#             FUN=c('sum','length'))
# doesn't matter here 

distribagg <- function(cls,ynames,xnames,dataname,FUN,FUNdim=1,FUN1=FUN) {
   ncellvars <- length(xnames) # number of cell-specification variables
   nagg <- length(ynames) # number of variables to tabulate
   isdt <- distribisdt(cls,dataname)
   if (isdt) {
      if (length(FUN) != length(ynames))
         stop('lengths of FUN and ynames must be the same for data.tables')
      if (FUNdim > 1) stop('FUNdim must be 1 for data.tables')
      # if (length(FUN) < nagg) FUN <- rep_len(FUN,nagg*length(FUN))
      remotecmd <- paste(dataname,'[,.(',sep='')
      for (i in 1:nagg) {
         ipstrcat(remotecmd,FUN[i],'(',ynames[i],')')
         if (i == nagg) ipstrcat(remotecmd,')')
         ipstrcat(remotecmd,',') 
      }
      xns <- xnames
      quotemark <- '"'
      for (i in 1:length(xns)) {
         xns[i] <- paste(quotemark,xns[i],quotemark,sep='')
      }
      ipstrcat(remotecmd,'by=c(',xns,')]',innersep=',')
   } else {
        if (length(FUN) > 1) 
           stop('for data.frames,FUN must be a single string')
        # set up aggregate() command to be run on the cluster nodes
        ypart <- paste("cbind(",paste(ynames,collapse=","),")",sep="")
        xpart <- paste(xnames,collapse="+")
        # the formula
        frmla <- paste(ypart,"~",paste(xnames,collapse="+"))
        remotecmd <-
           paste("aggregate(",frmla,",data=",dataname,",",FUN,")",sep="")
   }
   clusterExport(cls,"remotecmd",envir=environment())
   # run the command, and combine the returned data frames into one big
   # data frame
   aggs <- clusterEvalQ(cls,docmd(remotecmd))
   agg <- Reduce(rbind,aggs)
   # typically a given cell will found at more than one cluster node;
   # they must be combined, using FUN1
   FUN1 <- get(FUN1)
   # if FUN returns a vector rather than a scalar, the "columns" of agg
   # associated with ynames will be matrices; need to expand so that
   # have real columns
   if (FUNdim > 1) {
      # note: names will be destroyed
      tmp  <- agg[,1:ncellvars,drop=FALSE]
      for (i in 1:(ncol(agg)-ncellvars))
         tmp <- cbind(tmp,agg[,ncellvars+i])
      agg <- tmp
      names(agg)[-(1:ncellvars)] <- rep(ynames,each=FUNdim)
   }
   if (isdt) {
      agg <- as.data.frame(agg)
      names(agg)[-(1:ncellvars)] <- ynames
   }
   aggregate(x=agg[,-(1:ncellvars)],by=agg[,1:ncellvars,drop=FALSE],FUN1)
}

# get the indicated cell counts, cells defined according to the
# variables in xnames 
distribcounts <- function(cls,xnames,dataname) {
   isdt <- distribisdt(cls,dataname)
   res <- 
      if (isdt) distribagg(cls,'.N',xnames,dataname,"sum",FUN1="sum") else
      distribagg(cls,xnames[1],xnames,dataname,"length",FUN1="sum")
   names(res)[length(xnames)+1] <- 'count'
   res
}

# determine whether the distributed object dataname is a data.table
distribisdt <- function(cls,dataname) {
   rcmd <- paste('is.data.table(',dataname,')',sep='')
   clusterExport(cls,"rcmd",envir=environment())
   clusterEvalQ(cls,docmd(rcmd))[[1]]
}

sumlength <- function(a) c(sum(a),length(a))

# get the indicated cell means of the variables in ynames,
# cells defined according to the variables in xnames; if saveni is TRUE,
# then add one column 'yni', giving the number of Y values in this cell
distribmeans <- function(cls,ynames,xnames,dataname,saveni=FALSE) {
   clusterExport(cls,'sumlength',envir=environment())
   isdt <- distribisdt(cls,dataname)
   if (!isdt) {
      da <- distribagg(cls,ynames,xnames,dataname,
         FUN='sumlength',FUNdim=2,FUN1='sum')
   } else {
      da <- distribagg(cls,rep(ynames,each=2),
         xnames,dataname,
         FUN=rep(c('sum','length'),length((ynames))),FUN1='sum')
   }
   nx <- length(xnames)
   tmp <- da[,1:nx]
   day <- da[,-(1:nx)]  # Y values in da
   ny <- length(ynames)
   for (i in 1:ny) {
      tmp <- cbind(tmp,day[,2*i-1] / day[,2*i])
   }
   tmp <- as.data.frame(tmp)
   if (saveni) {
      tmp$yni <- day[,2]
      names(tmp) <- c(xnames,ynames,'yni')
   }
   names(tmp)[-(1:nx)] <- ynames
   tmp
}


# currently not in service; xtabs() call VERY slow
# distribtable <- function(cls,xnames,dataname) {
#    tmp <- distribagg(cls,xnames[1],xnames,dataname,"length","sum")
#    names(tmp)[ncol(tmp)] <- "counts"
#    xtabs(counts ~ .,data=tmp)
# }

# find the smallest value in the indicated distributed vector; return
# value is a pair (node number, row number)
dwhich.min <- function(cls,vecname) {
   cmd <- paste('which.min(',vecname,')',sep='')
   mininfo <- function(x) {i <- which.min(x); c(i,x[i])}
   cmd <- paste('mininfo(',vecname,')',sep='')
   clusterExport(cls,c('mininfo','cmd'),envir=environment())
   rslt <- clusterEvalQ(cls,docmd(cmd))
   tmp <- unlist(Reduce(rbind,rslt))
   nodenum <- which.min(tmp[,2])
   rownum <- tmp[nodenum,1]
   c(nodenum,rownum)
}

# find the largest value in the indicated distributed vector; return
# value is a pair (node number, row number)
dwhich.max <- function(cls,vecname) {
   cmd <- paste('which.max(',vecname,')',sep='')
   maxinfo <- function(x) {i <- which.max(x); c(i,x[i])}
   cmd <- paste('maxinfo(',vecname,')',sep='')
   clusterExport(cls,c('maxinfo','cmd'),envir=environment())
   rslt <- clusterEvalQ(cls,docmd(cmd))
   tmp <- unlist(Reduce(rbind,rslt))
   nodenum <- which.max(tmp[,2])
   rownum <- tmp[nodenum,1]
   c(nodenum,rownum)
}

# find the k largest or smallest values in the specified distributed
# vector; UNDER CONSTRUCTION
# dwhich.extremek <- function(cls,vecname,k,largest=TRUE) {
#    extremek <- function(vecname,k,largest) {
#       tmpvec <- get('vecname')
#       tmporder <- order(tmpvec,!largest)[1:k]
#       cbind(order(tmporder,tmpvec[tmporder][
#    }
#    cmd <- paste('extremek(',vecname,',',k,'.',largest,')')
#    clusterExport(cls,'cmd),envir=environment())
#    rslt <- clusterEvalQ(cls,docmd(cmd))
#    tmp <- unlist(Reduce(rbind,rslt))
# 
# }

distribrange <- function(cls,vec,na.rm=FALSE) {
   narm <- if(na.rm) 'TRUE' else 'FALSE'  
   narm <- paste("na.rm=",narm)
   tmp <- paste("range(", vec, ",",narm,")",collapse = "")
   rangeout <- clusterCall(cls,docmd,tmp)
   vecmin <- min(geteltis(rangeout,1))
   vecmax <- max(geteltis(rangeout,2))
   c(vecmin,vecmax)
}

# execute the command cmd at each cluster node, typically select(), then
# collect using rbind() at the caller
distribgetrows <- function(cls,cmd) {
   clusterExport(cls,'cmd',envir=environment())
   res <- clusterEvalQ(cls,docmd(cmd))
   tmp <- Reduce(rbind,res)
   notallna <- function(row) any(!is.na(row))
   tmp[apply(tmp,1,notallna),]
}

######################### misc. utilities ########################

# execute the contents of a quoted command; main use is with
# clusterEvalQ()
docmd <- function(toexec) eval(parse(text=toexec))

# execute the contents of a quoted command at the cluster nodes
doclscmd <- function(cls,toexec) {
   dotoexec <- function() docmd(toexec)
   clusterExport(cls,c('dotoexec','toexec'),envir=environment())
   clusterEvalQ(cls,dotoexec())
}

#### seems a duplicate of what's in CA.R; comment out for now

#### # chunks averaging, aka Software Alchemy, method (N. Matloff, "Parallel
#### # Computation for Data Science," Chapman and Hall, 2015)
#### 
#### ############################## ca() ################################
#### 
#### # wrapper for cabase(), e.g. for case where the data is not already
#### # distributed
#### 
#### # arguments:
#### #
#### #    cls: 'parallel' cluster
#### #    z:  (non-distributed) data (data.frame, matrix or vector)
#### #    ovf:  overall statistical function, say glm()
#### #    estf:  function to extract the point estimate (possibly
#### #           vector-valued) from the output of ovf(), e.g.
#### #           coef() in the glm() case
#### #    estcovf:  if provided, function to extract the estimated 
#### #              covariance matrix of the output of estf(), e.g.
#### #              vcov() for glm()
#### #    findmean:  if TRUE, output the average of the estimates from the
#### #               chunks; otherwise, output the estimates themselves
#### #    scramble:  randomize the order of z before distributing
#### # 
#### # value:
#### #
#### #    R list, consisting of the estimates from the chunks, thts, and if
#### #    requested, their average tht and its estimated covariance matrix 
#### #    thtcov
#### 
#### ca <- function(cls,z,ovf,estf,estcovf=NULL,findmean=TRUE,
####          scramble=FALSE) {
####    if (is.vector(z)) z <- data.frame(z)
####    cabase(cls,ovf,estf,estcovf,findmean,cacall=TRUE,z=z,scramble=scramble) 
#### }
#### 
#### ############################## cabase() ##############################
#### 
#### # serves as a manager for the Software Alchemy method, arranging for the
#### # estimates to be computed at each data chunk, gathering the results,
#### # averaging them etc.
#### 
#### # other than the case cacall = TRUE, ca() is not directly aware of the
#### # data chunks, e.g. their names, sizes etc.; it merely calls the
#### # user-supplied ovf(), which does that work
#### 
#### # arguments:
#### #
#### #    as in ca() above, except:
#### #
#### #    cacall: TRUE if cabase() was invoked by ca() 
#### #    z: if cacall is TRUE, the (non-distributed) data frame; the
#### #       data will be distributed among the cluster nodes, under the name
#### #       "z168"
#### #    scramble: if this and cacall are TRUE, randomize z before
#### #              distributing
#### #
#### # value: 
#### #
#### #    as in ca() above
#### 
#### cabase <- function(cls,ovf,estf, estcovf=NULL,findmean=TRUE,
####              cacall=FALSE,z=NULL,scramble=FALSE) {
####    if (cacall) {
####       z168 <- z  # needed for CRAN issue
####       distribsplit(cls,'z168',scramble=scramble)
####    } else clusterEvalQ(cls,z168 <- NULL)
####    clusterExport(cls,c("ovf","estf","estcovf"),envir=environment())
####    # apply the "theta hat" function, e.g. glm(), and extract the
####    # apply the desired statistical method at each chunk
####    ovout <- ### if (cacall) clusterEvalQ(cls,ovf(z168)) else
####                         clusterEvalQ(cls,ovf(z168)) 
####    # theta-hats, with the one for chunk i in row i
####    # thts <- t(sapply(ovout,estf))
####    tmp <- lapply(ovout, estf)
####    thts <- Reduce(rbind,tmp)
####    if (is.vector(thts)) thts <- matrix(thts,ncol=1)
####    # res will be the returned result of this function
####    res <- list()
####    res$thts <- thts 
####    # find the chunk-averaged theta hat and estimated covariance matrix, 
####    # if requested
####    if (findmean) {
####       # dimensionality of theta
####       lth <- length(thts[1,])
####       # since the chunk sizes will differ by a negligible amount, 
####       # so use equal weights; commented-out code below allowed for
####       # varying weights
####       ## wts <- rep(1 / length(cls), length(cls))
####       ## # compute mean (leave open for restoring weights in future
####       ## # version)
####       ## tht <- rep(0.0,lth)
####       ## for (i in 1:length(cls)) {
####       ##    wti <- wts[i]
####       ##    tht <- tht + wti * thts[i,]
####       ## }
####       tht <- colMeans(thts)
####       attr(tht,"p") <- attr(thts[1,],"p")
####       res$tht <- tht
####       # compute estimated covariance matrix; if requested, this will be
####       # done from outputs of the calls on the chunks, e.g. from vcov();
####       # otherwise, compute the matrix empirically
####       nchunks <- length(cls)
####       if (!is.null(estcovf)) {
####          thtcov <- matrix(0,nrow=lth,ncol=lth)
####          for (i in 1:nchunks) {
####             summand <- estcovf(ovout[[i]]) 
####             if (any(dim(thtcov) != dim(summand))) {
####                print('dimension mismatch')
####                stop('likely cause is constant variable in some chunk')
####             }
####             thtcov <- thtcov + summand
####          }
####          res$thtcov <- (thtcov / nchunks) / nchunks
####       } else {
####          res$thtcov <- cov(thts) / nchunks
####       }
####    }
####    clusterEvalQ(cls,rm(ovf))
####    res
#### }
#### 
#### # ca() wrapper for lm()
#### #
#### # arguments:
#### #    cls: cluster
#### #    lmargs: quoted string representing arguments to lm()
#### #            e.g. "weight ~ height + age, data-mlb"
#### # value: Software Alchemy estimate, statistically equivalent to direct
#### #    nonparallel call to lm(); R list is value of ca() 
#### #
#### calm <- function(cls,lmargs) {
####    ovf <- function(u) {
####       tmp <- paste("lm(",lmargs,")",collapse="")
####       docmd(tmp)
####    }
####    cabase(cls,ovf,coef,vcov)
#### }
#### 
#### # ca() wrapper for glm()
#### # arguments and value as in calm(), except that it should include
#### # specification of family
#### caglm <- function(cls,glmargs) {
####    ovf <- function(u) {
####       tmp <- paste("glm(",glmargs,")",collapse="")
####       docmd(tmp)
####    }
####    cabase(cls,ovf,coef,vcov)
#### }
#### 
#### # ca() wrapper for knnest(); 
#### # arguments:
#### #    cls: cluster
#### #    yname: name of distributed Y vector
#### #    k: number of nearest neighbors
#### #    xname: if nonempty, this is the distributed matrix of X values, 
#### #           which will be fed into preprocessx() to produce the 
#### #           global variable xdata at the nodes; if empty, xdata will be
#### #           generated
#### # value:
#### #    none at caller; xdata, kout are left there at the nodes 
#### #    for future use
#### caknn <- function(cls,yname,k,xname='') {
####    clusterEvalQ(cls,library(regtools))
####    if (xname != '') {
####       # run preprocessx() to generate xdata
####       cmd <- paste('xdata <<- preprocessx(',xname,',',k,')',sep='')
####       doclscmd(cls,cmd)
####    }
####    cmd <- paste('kout <<- knnest(',yname,',xdata,',k,')',sep='')
####    doclscmd(cls,cmd)
#### }
#### 
#### # kNN predict
#### # arguments:
#### #    predpts: matrix/df of X values at which to find est. reg. ftn.;
#### #             if NULL, it is assumed that predpts already exists at the
#### #             nodes
#### # value:
#### #    est. reg. ftn. value at those points
#### # assumes kout present at the nodes
#### caknnpred <- function(cls,predpts) {
####    if (!is.matrix(predpts)) 
####       predpts <- as.matrix(predpts)
####    clusterEvalQ(cls,library(regtools))
####    if (!is.null(predpts))
####       clusterExport(cls,'predpts',envir=environment())
####    predys <- doclscmd(cls,'predy <<- predict(kout,predpts)')
####    predys <- Reduce(rbind,predys)
####    colMeans(predys)
#### }
#### 
#### # ca() wrapper for prcomp()
#### # arguments:
#### #
#### #    prcompargs: arguments to go into prcomp()
#### #    p: number of variables (cannot be inferred by the code, since
#### #       prcompargs can be so general
#### #
#### # value:
#### #
#### #    R list, with componentns:
#### #
#### #       sdev, rotation: as in prcomp()
#### #       thts: the sdev/rotation results of applying prcomp()
#### #             to the individual data chunksb
#### caprcomp <- function(cls,prcompargs,p) {
####    ovf <- function(u) {
####       tmp <- paste("prcomp(",prcompargs,")",collapse="")
####       docmd(tmp)
####    }
####    # we're interested in both sdev and rotation, so string them together
####    # into one single vector for averaging
####    estf <- function(pcout) c(pcout$sdev,pcout$rotation)
####    cabout <- cabase(cls,ovf,estf)
####    # now extract the sdev and rotation parts back
####    tmp <- cabout$tht
####    res <- list()
####    res$sdev <- tmp[1:p]
####    res$rotation <- matrix(tmp[-(1:p)],ncol=p)
####    res$thts <- cabout$thts
####    res
#### }
#### 
#### # ca() wrapper for kmeans()
#### #
#### # arguments:
#### #
#### #    mtdf: matrix or data frame
#### #    ncenters: number of clusters to find
#### #    p: number of variables
#### #
#### # value: sdev and rotation from kmeans() output, plus thts to explore
#### # possible instability
#### #
#### cakm <- function(cls,mtdf,ncenters,p) {
####    # need the same initial centers for each node; can take the first few
####    # records from the first chunk, since the data is assumed to be
####    # randomized
####    hdcmd <- paste('head(',mtdf,',',ncenters,')',sep='')
####    clusterExport(cls,'hdcmd',envir=environment())
####    ctrs <- clusterEvalQ(cls,eval(parse(text=hdcmd)))[[1]]
####    clusterExport(cls,'ctrs',envir=environment())
####    ovf <- function(u) {
####       tmp <- paste("kmeans(",mtdf,",centers=ctrs)",collapse="",sep="")
####       docmd(tmp)
####    }
####    # as in caprcomp(), string the output entities together, then later
####    # extract them back
####    estf <- function(kmout) c(kmout$size,kmout$centers)
####    kmout <- cabase(cls,ovf,estf)
####    ncenters <- length(kmout$tht) / (1+p)
####    tmp <- kmout$tht
####    res <- list()
####    res$size <- round(tmp[1:ncenters] * length(cls))
####    res$centers <- matrix(tmp[-(1:ncenters)],ncol=p)
####    res$thts <- kmout$thts
####    res
#### }
#### 
#### # finds the means of the columns in the string expression cols
#### cameans <- function(cls,cols,na.rm=FALSE) {
####    ovf <- function(u) {
####       narm <- if(na.rm) 'TRUE' else 'FALSE'  
####       narm <- paste("na.rm=",narm)
####       tmp <- paste("colMeans(",cols,",",narm,")",collapse="")
####       docmd(tmp)
####    }
####    estf <- function(z) z
####    cabase(cls,ovf,estf)
#### }
#### 
#### # finds the quantiles of the vector defined in the string vec
#### caquantile <- function(cls,vec,probs=c(0.25,0.50,0.75),na.rm=FALSE) {
####    ovf <- function(u) {
####       probs <- paste(probs,collapse=",")
####       probs <- paste("c(",probs,")",sep="")
####       narm <- if(na.rm) 'TRUE' else 'FALSE'  
####       narm <- paste("na.rm=",narm)
####       tmp <- paste("quantile(",vec,",",probs,",",narm,")",collapse="")
####       docmd(tmp)
####    }
####    estf <- function(z) z
####    cabase(cls,ovf,estf)
#### }
#### 
#### # a Software Alchemy version of aggregate()/distribagg(); finds the
#### # groups, applying FUN to each, then averaging across the cluster
#### caagg <- function(cls,ynames,xnames,dataname,FUN) {
####    distribagg(cls,ynames,xnames,dataname,FUN,"mean")
#### }

# extract element i of each element in the R list lst, which is a list
# of vectors
geteltis <- function(lst,i) {
   get1elti <- function(lstmember) lstmember[i]
   sapply(lst,get1elti)
}

# in-place string concatenation; appends the strings in ... to str1,
# assigning back to str1 (at least in name, if not memory location), in
# the spirit of string.append() in Python; here DOTS is one of more
# expressions separated by commas, with each expression being either a
# string or a vector of strings; within a vector, innersep is used as a
# separator, while between vectors it is outersep

# generaed from gtools code:
# ipstrcat <- defmacro(str1,DOTS,outersep='',innersep='',expr = (
#       for (str in list(...)) {
#          lstr <- length(str)
#          tmp <- str[1]
#          if (lstr > 1) 
#             for (i in 2:lstr) 
#                tmp <- paste(tmp,str[i],sep=innersep)
#          str1 <- paste(str1,tmp,sep=outersep)
#       }
#    )
# )

ipstrcat <- function (str1 = stop("str1 not supplied"), ..., outersep = "", 
    innersep = "") 
{
    tmp <- substitute((for (str in list(...)) {
        lstr <- length(str)
        tmp <- str[1]
        if (lstr > 1) for (i in 2:lstr) tmp <- paste(tmp, str[i], 
            sep = innersep)
        str1 <- paste(str1, tmp, sep = outersep)
    }))
    eval(tmp, parent.frame())
}

