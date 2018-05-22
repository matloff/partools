
# still being tested

############################## hqs() ################################
# hyperquicksort uses ptME message passing to keep data 
# distributed while sorting entire data set among nodes 


# arguments:
#
#    cls: 'parallel' cluster
#    xname: data frame argument; if NULL, there is already a
#           distributed data frame with name 'xname' on the 
#           cluster; otherwise one will be created
# 
# value:
#
#    none; a distributed data frame named 'yname' will containt the
#          sorted list

hqs <- function(cls,xname=NULL){

  # get everything ready
  if (is.null(xname) distribsplit(cls,"xname",scramble=FALSE)
  ptMEinit(cls)

  # this function, to be executed by each worker node, does the main
  # work
  hqsWorker <-function(){
    # myID <- partoolsenv$myid
    # groupSize <- partoolsenv$ncls
    chunk <- as.vector(t(xname))
    #ptm <- proc.time()
    
    while (groupSize > 1){
      
      myrank <- (myID %% groupSize)
      
      if (myrank==0){
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
    
    chunk<-sort(chunk)
    chunk
    #time<-proc.time() - ptm
    #time
    
  }
  clusterCall(cls, hqsWorker)
}


# enter host pcs (a power of 2 nodes) name as list of strings, 
# with pc names,IP addresses, or run on "localhost"
hostpcs<-c(rep("localhost",4))
cls<-makeCluster(hostpcs)
# enter test data
data<-data.frame(sample(1:50, 1000, replace = TRUE))
hqs(cls,data)
