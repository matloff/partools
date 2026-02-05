
# k-means clustering, using Snowdoop

# chunked data with name xname, nitrs iterations, nclus clusters;
# assumes for simplicity that a cluster will never become empty; ctrs is
# the matrix of initial centroids

# assumes setclsinfo already called

kmeans <- function(cls,xname,nitrs,ctrs) {
   # will tell everyone to read their chunks; first, find cluster size
   # and compute number of digits in file suffixes
   addlistssum <- function(lst1,lst2) addlists(lst1,lst2,sum)
   for (i in 1:nitrs) {
      # for each data point, find the nearest centroid, and tabulate; at
      # each worker and for each centroid, we compute a vector whose
      # first component is the count of the number of data points whose
      # nearest centroid is that centroid, and whose other components is
      # the sum of all such data points
      tmp <- clusterCall(cls,findnrst,xname,ctrs)
      # sum over all workers
      tmp <- Reduce(addlistssum,tmp)
      # compute new centroids
      for (i in 1:nrow(ctrs)) {
         tmp1 <- tmp[[as.character(i)]]
         ctrs[i,] <- (1/tmp1[1]) * tmp1[-1]
      }
   }
   ctrs
}

findnrst <- function(xname,ctrs) {
   require(pdist)
   x <- get(xname)
   dsts <- matrix(pdist(x,ctrs)@dist,ncol=nrow(x))
   # dsts[,i] now has the distances from row i of x to the centroids 
   nrst <- apply(dsts,2,which.min)
   # nrst[i] tells us the index of the centroid closest to row i of x
   mysum <- function(idxs,myx) 
      c(length(idxs),colSums(x[idxs,,drop=F]))
   tmp <- tapply(1:nrow(x),nrst,mysum,x)
}

test <- function(cls) {
   m <- matrix(c(4,1,4,6,3,2,6,6),ncol=2)
   formrowchunks(cls,m,"m")
   initc <- rbind(c(2,2),c(3,5))
   kmeans(cls,"m",1,initc) 
   # output should be matrix with rows (2.5,2.5) and (5.0,6.0)
}
