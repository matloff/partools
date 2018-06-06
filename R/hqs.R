
# hyperquicksort uses ptME message passing to keep data 
# distributed while sorting entire data set among nodes 
# for faster processing


# arguments:
#
#    cls: 'parallel' cluster (must be power of 2 nodes)
#    'xname': name of distributed dataframe/vector/matrix (character string in '')
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

hqs <- function(cls,xname){
  if (exists("partoolsenv$myServers")==FALSE)
  {ptMEinit(cls)}
  #clusterEvalQ(cls,assign("chunk",xname))
  cmd <- paste0('clusterEvalQ(cls,chunk <<- ',xname,')')
  eval(parse(text = cmd))
  hqsWorker  <- function(){
    myID  <-  partoolsenv$myid
    groupSize  <-  partoolsenv$ncls
    while (groupSize > 1){
      myrank  <-  (myID %% groupSize)
      if (myrank==0){
        pivot  <-  median(chunk)
        for (i in 1:(groupSize-1)){
          ptMEsend(pivot,myID-i)}
      }
      else{
        pivot <- ptMErecv(myID+(groupSize-myrank))
      }
      lower <-  chunk[chunk < pivot]
      upper <-  chunk[chunk >= pivot]
      if (myrank <= (groupSize/2) && myrank > 0) {
        ptMEsend(upper,myID+(groupSize/2) )
        newUpper <- ptMErecv(myID+(groupSize/2))
        chunk <- c(lower, newUpper)
      }
      else {
        newLower <- ptMErecv(myID-(groupSize/2))
        ptMEsend(lower,myID-(groupSize/2))
        chunk <- c(newLower,upper)
      }
      groupSize <- groupSize/2
    }
  
    chunks <- sort(chunk)

    #return(0)
  }
  chunks <-clusterCall(cls, hqsWorker)

}

# to be comparable to hqs(), with the "leave it there" philosophy, must
# gather the distributed vector to the manager, do a serial sort there,
# then distribute back to the workers
serialqs  <-  function(cls,y) {
  temp <- unlist(clusterEvalQ(cls,y))
  y <- sort(temp)
  distribsplit(cls,'y')
  chunks <- clusterEvalQ(cls, y)
  chunks
}

# vlength: test vector length
# clength: test cluster length
hqsTest  <-  function(vlength,clength){
  cls  <-  makeCluster(clength)
  setclsinfo(cls)
  set.seed(9999)
  x  <-  sample(1:50, vlength, replace = TRUE)
  distribsplit(cls,"x",scramble=FALSE)
  #ptm  <-  proc.time()
  ptMEinit(cls)
  ptm  <-  proc.time()
  chunks <- hqs(cls,'x')
  print(proc.time() - ptm)
  if (vlength < 100)  print(chunks)#it was still printing "chunks" as if it was back at the master
  y  <-  sample(1:50, vlength, replace = TRUE)
  distribsplit(cls,'y')
  ptm  <-  proc.time()
  chunks <- serialqs(cls,y)
  print(proc.time() - ptm)
  if (vlength < 100) print(chunks)
}
