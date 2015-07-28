
# general "Snow" (the part of 'parallel' adapted from the old Snow)
# utilities, some used in Snowdoop but generally applicable


# split matrix/data frame into chunks of rows, placing a chunk into each
# of the cluster nodes
#
# arguments:
#
# cls: a 'parallel' cluster
# m: a matrix or data frame
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
#    getrowchunk <- function(rc) 
#       assign(mchunkname,m[rc,],envir=.GlobalEnv)
#    invisible(clusterApply(cls,rcs,getrowchunk))
   invisible(clusterApply(cls,rowchunks,
      function(chunk) 
         assign(mchunkname,chunk,envir=.GlobalEnv)))
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
   clusterEvalQ(cls,library(partools))
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
   dfr <- as.data.frame(dfr)
   formrowchunks(cls,dfr,dfname,scramble)
}

# collects a distributed matrix/data frame specified by dfname at
# manager (i.e. caller), again with the name dfname, in global space of
# the latter
distribcat <- function(cls,dfname) {
   toexec <- paste("clusterEvalQ(cls,",dfname,")")
   tmp <- eval(parse(text=toexec))
   assign(dfname,Reduce(rbind,tmp),pos=.GlobalEnv)
}

# distributed version of aggregate()

# arguments:

#    cls: cluster
#    ynames: names of variables on which FUN is to be applied
#    xnames: names of variables that define the grouping
#    dataname: quoted name of the data frame
#    FUN: quoted name of aggregation function to be used in aggregation
#         within cluster nodes
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

#    distribagg(cls,"x=d, by=list(d$x,d$y)","max",2)

distribagg <- function(cls,ynames,xnames,dataname,FUN,FUN1=FUN) {
   nby <- length(xnames) # number in the "by" arg to aggregate()
   # set up aggregate() command to be run on the cluster nodes
   ypart <- paste("cbind(",paste(ynames,collapse=","),")",sep="")
   xpart <- paste(xnames,collapse="+")
   forla <- paste(ypart,"~",paste(xnames,collapse="+"))
   remotecmd <-
      paste("aggregate(",forla,",data=",dataname,",",FUN,")",sep="")
   clusterExport(cls,"remotecmd",envir=environment())
   # run the command, and combine the returned data frames into one big
   # data frame
   aggs <- clusterEvalQ(cls,docmd(remotecmd))
   agg <- Reduce(rbind,aggs)
   # typically a given cell will found at more than one cluster node;
   # they must be combined, using FUN1
   FUN1 <- get(FUN1)
   aggregate(x=agg[,-(1:nby)],by=agg[,1:nby,drop=FALSE],FUN1)
}

# get the indicated cell counts, cells defined according to the
# variables in xnames 
distribcounts <- function(cls,xnames,dataname) {
   distribagg(cls,xnames[1],xnames,dataname,"length","sum")
}

# currently not in service; xtabs() call VERY slow
# distribtable <- function(cls,xnames,dataname) {
#    tmp <- distribagg(cls,xnames[1],xnames,dataname,"length","sum")
#    names(tmp)[ncol(tmp)] <- "counts"
#    xtabs(counts ~ .,data=tmp)
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

# extract element i of each element in the R list lst, which is a list
# of vectors
geteltis <- function(lst,i) {
   get1elti <- function(lstmember) lstmember[i]
   sapply(lst,get1elti)
}
