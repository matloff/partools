
# form chunks of rows of m, corresponding to the number of worker nodes
# in the cluster cls; places the chunk named mchunkname in the global
# space of the worker
formrowchunks <- function(cls,m,mchunkname) {
   rcs <- clusterSplit(cls,1:nrow(m))
   getrowchunk <- function(rc) 
      assign(mchunkname,m[rc,],envir=.GlobalEnv)
   clusterApply(cls,rcs,getrowchunk)
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
   get("partoolsenv",envir=.GlobalEnv)
}

# set the R library paths at the workers to that of the calling node
exportlibpaths <- function(cls) {
   lp <- .libPaths()
   clusterCall(cls,function(p) .libPaths(p),lp)
}

