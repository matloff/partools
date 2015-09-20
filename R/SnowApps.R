

#########################  parpdist()  ############################0

# finds distances between all possible pairs of rows 
# in the matrix x and rows in the matrix y, as with 
# pdist() but in parallel

# arguments:
#    x:  data matrix
#    y:  data matrix
#    cls:  cluster

# value:
#    full distance matrix, as pdist object

parpdist <- function(x,y,cls) {
   require(pdist)
   nx <- nrow(x)
   ichunks <- formrowchunks(cls,x,'tmpx')
   clusterExport(cls,'y',env=environment())
   clusterEvalQ(cls,require(pdist))
   dists <- clusterEvalQ(cls,pdist(tmpx,y)@dist)
   tmp <- Reduce(c,dists)
   new("pdist", dist = tmp, n = nrow(x), p = nrow(y))
}

