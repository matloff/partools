
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
   clusterEvalQ(cls,library(partools))
   # doclscmd(cls,'library(partools)')
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

# TODO: Clark - Write this function
#distribunique <- function(cls,ynames,dataname) {
#}

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
distribgetrows <- function(cls,cmd,who=NULL) {
   # clusterExport(cls,'cmd',envir=environment())
   # res <- clusterEvalQ(cls,docmd(cmd))
   res <- doclscmd(cls,cmd,who)
   tmp <- Reduce(rbind,res,init = NULL)
   notallna <- function(row) any(!is.na(row))
   tmp[apply(tmp,1,notallna),]
}

######################### misc. utilities ########################

# execute the contents of a quoted command; main use is with
# clusterEvalQ()
docmd <- function(toexec) eval(parse(text=toexec))

# execute the contents of a quoted command 'toexec' at the cluster
# nodes; optionally, set 'who' to have the command done only at the
# nodes in 'who'
doclscmd <- function(cls,toexec,who=NULL)
{  dotoexec <- function() {
      if (!is.null(who) && !(myid %in% who)) 
         toexec <- 'NULL'
      docmd(toexec)
   }
   clusterEvalQ(cls,myid <- getpte()$myid)
   clusterExport(cls, c("dotoexec","toexec","who"), 
      envir = environment())
   clusterEvalQ(cls, dotoexec())
}

# extract element i of each element in the R list 'lst', which is a list
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

# try to load the package specified in pkgname
tryloadpkg <- function(pkgname) {
   cmd <- paste('require(',pkgname,')',sep='')
   docmd(cmd)
}

