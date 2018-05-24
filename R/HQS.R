
# still being tested

############################## hqs() ################################
# hyperquicksort uses ptME message passing to keep data 
# distributed while sorting entire data set among nodes 


# arguments:
#
#    cls: 'parallel' cluster
#    xname: name of vector to be sorted; if xdistr is TRUE, the
#           vector is already distributed under the name xname;
#           otherwise the code here will distributed it
# 
# value:
#
#    none; a distributed data frame named 'yname' will containt the
#          sorted list

hqs <- function(cls,xname,xdistr=FALSE){

browser()
dbqmsgstart(cls)

  # get everything ready
  if (!xdistr) {
     distribsplit(cls,xname,scramble=FALSE)
     # cmd <- paste0(xname,' <- as.numeric(',xname,'[,1])')
     # clusterExport(cls,'cmd', envir=environment())
     # clusterEvalQ(cls,eval(parse(text=cmd)))
  }
  
  ptMEinit(cls)

  # clusterCall(cls, hqsWorker)
  clusterExport(cls,'hqsWorker')
  clusterEvalQ(cls,hqsWorker(xname))

}

  # this function, to be executed by each worker node, does the main
  # work
hqsWorker <-function(xname) {
    myID <- partoolsenv$myid
    groupSize <- partoolsenv$ncls
    ### chunk <- as.vector(xname[,1])
    #ptm <- proc.time()
    
    while (groupSize > 1){

      # this node's ID with respect to current subcube
      myrank <- (myID %% groupSize)
dbqmsg(myrank)
      if (myrank == 0){
        pivot <- median(chunk)
        for (i in 1:(groupSize-1)){
          ptMEsend(pivot,myID-i)}
      }
      else{
        pivot<-ptMErecv(myID+(groupSize-myrank))
      }
      lower<- chunk[chunk < pivot]
      upper<- chunk[chunk >= pivot]
      if (myrank <= (groupSize/2) && myrank > 0) {
        ptMEsend(upper,myID+(groupSize/2) )
        newUpper<-ptMErecv(myID+(groupSize/2))
        chunk<-c(lower, newUpper)
      }
      else {
        newLower<-ptMErecv(myID-(groupSize/2))
        ptMEsend(lower,myID-(groupSize/2))
        chunk<-c(newLower,upper)
      }
      groupSize<-groupSize/2
    }
    
    chunk <- sort(chunk)
    assign(paste0(xname,'.sorted'),chunk,envir = .GlobalEnv)
    #time<-proc.time() - ptm
    #time
    return(0)
}


# enter host pcs (a power of 2 nodes) name as list of strings, 
# with pc names,IP addresses, or run on "localhost"
testhqs <- function() 
{
   hostpcs <- c(rep("localhost",2))
   cls <- makeCluster(hostpcs)
   setclsinfo(cls)
   # enter test data
   dta <<- data.frame(sample(1:50, 25, replace = TRUE))
   hqs(cls,'dta')
   clusterEvalQ(cls,dta.sorted)
}

