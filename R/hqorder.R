# hyperquicksort uses ptME message passing to keep data 
# distributed while sorting entire data set among nodes 
# for faster processing


# arguments:
#
#    cls: 'parallel' cluster (must be power of 2 nodes)
#    'xname': name of distributed dataframe/matrix (character string in '')
#    bycol: (optional) order by a specific column (maintaining original row)
#
# value:
#
#    none, just a placeholder 0 to avoid the communication cost 
#    of returning the sorted chunks; note that under the "Leave It
#    There" philosophy, we do not want to return those chunks to the
#    caller

#R list, consisting of the sorted data, distributed among nodes

# note: user must have called setclsinfo() and ptMEinit prior to
# calling hqs() (distribsplit() can also be used to split data if not
# already distributed)


hqorder <- function(cls, xname, bycol=1){
  if (exists("partoolsenv$myServers")==FALSE)
  {ptMEinit(cls)}
  cmd <- paste0('clusterEvalQ(cls, chunk <<- ',xname,')')
  eval(parse(text = cmd))
  hqsWorker  <- function(){
    getpte()
    myID  <-  partoolsenv$myid
    groupSize  <-  partoolsenv$ncls
    while (groupSize > 1){
      myrank  <-  (myID %% groupSize)
      if (myrank==0){
        pivot  <-  median(chunk[,bycol])
        for (i in 1:(groupSize-1)){
          ptMEsend(pivot,myID-i)}
      }
      else{
        pivot <- ptMErecv(myID+(groupSize-myrank))
      }
      lower <-  chunk[chunk[,bycol] < pivot,]
      upper <-  chunk[chunk[,bycol] >= pivot,]
      if (myrank <= (groupSize/2) && myrank > 0) {
        ptMEsend(upper,myID+(groupSize/2) )
        newUpper <- ptMErecv(myID+(groupSize/2))
        chunk <- rbind(lower, newUpper) # works!
      }
      else {
        newLower <- ptMErecv(myID-(groupSize/2))
        ptMEsend(lower,myID-(groupSize/2))
        chunk <- rbind(newLower, upper)
      }
      groupSize <- groupSize/2
    }
    
    chunks <<- chunk[order(chunk[,bycol]),]
    return(0)  # avoid expensive return of last computed item
  }
  clusterExport(cls,c('hqsWorker',"bycol"), envir=environment())
  chunks <- clusterCall(cls, hqsWorker)
  
}


# to be comparable to hqs(), with the "leave it there" philosophy, must
# gather the distributed vector to the manager, do a serial sort there,
# then distribute back to the workers
serialorder  <-  function(cls,y,bycol=1) {
  distribsplit(cls,'y')
  ptm  <-  proc.time()
  temp <- clusterEvalQ(cls,y)
  temp <- do.call('rbind',temp) 
  y <- temp[order(temp[,bycol]),]
  distribsplit(cls,'y')
  print(proc.time() - ptm)
  chunks <- clusterEvalQ(cls, y) # to check
  chunks
}


# row_length: rows in data-frame
# col_length: columns in data-frame
# clength: test cluster length
# bycol: sort by another column of data frame besides 1st (optional)
hqorderTest  <-  function(row_length,col_length,clength,bycol=1){
  cls  <-  makeCluster(clength)
  setclsinfo(cls)
  set.seed(9999)
  x <- as.data.frame(matrix(sample.int(1000, replace = TRUE),nrow=row_length, ncol=col_length))
  if (row_length < 10 && col_length < 10) {print("Original unsorted data:")
    print(x)}
  distribsplit(cls,"x",scramble=FALSE)
  ptMEinit(cls)
  ptm  <-  proc.time()
  chunks <- hqorder(cls,'x',bycol) 
  print(proc.time() - ptm)
  if (row_length < 10 && col_length < 10) print(clusterEvalQ(cls, chunks)) 
  chunks <- serialorder(cls,x,bycol)
  if (row_length < 10 && col_length < 10) print(chunks)
}
