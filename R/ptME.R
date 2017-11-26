

################## ptME: partools message exchange ########################

# code to allow worker nodes in partools to communicate directly with
# each other

# run from the manager:
ptMEinit <- function(cls) {
   # send workers all the client IPs
   ncls <- length(cls)
   wrkrIPs <- unlist(Map(function(i) cls[i][[1]]$host, 1:ncls))
   clusterCall(cls,function(IPs)
      partoolsenv$wrkrIPs <- IPs,wrkrIPs)
   # myServers is a vector at each worker, which will list connections
   # to all workers with IDs above the worker
   clusterEvalQ(cls,partoolsenv$myServers <- 
      vector('list', length=partoolsenv$ncls - partoolsenv$myid))
   # provide workers with needed functions
   clusterExport(cls,c('ptMEinitSrvrs','ptMEinitCons'), envir=environment())
   # have each worker initialize and set up a server socket (NA for ID 1)
   portnums <- unlist(clusterEvalQ(cls,ptMEinitSrvrs()))
   # make all workers aware of the various server port numbers
   clusterCall(cls,function(ports) partoolsenv$portnums <- ports,portnums)
   # set up the connections
   for (srvr in 2:ncls) {
      clusterCall(cls,ptMEinitCons,srvr)
   }
   clusterExport(cls,c('ptMEsend','ptMErecv','ptMEclose'),envir = environment())
}

# servers initialize, and choose a port
ptMEinitSrvrs <- function() {
  myID <- partoolsenv$myid
  # my role as a server:
  if (myID > 1) {
     # prepare for a client connection from all workers
     # with lower IDs than mine
     partoolsenv$myClients <- vector('list',length=myID-1)
     # TODO: have code make repeated attempts in order to find a port
     port <- 5000 + myID + sample(1:100,1)
  } else port <- NA
  port
}

# set up the connections
ptMEinitCons<- function(srvr) {
  myID <- partoolsenv$myid
  host <- partoolsenv$wrkrIPs[srvr]
  port <- partoolsenv$portnums[srvr]
  if (myID == srvr) {  
     # make connections with lower-ID workers; note that among those
     # workers, they will not necessarily contact this server in the
     # order of their IDs, so we must ask them for IDs
     for (i in 1:(srvr-1)) {
        con <- socketConnection(host=host,port=port, 
                blocking=TRUE, server=TRUE, open="w+b")
        clientnum <- unserialize(con)
        partoolsenv$myClients[[clientnum]] <- con
     }
  } else if (myID < srvr) {  
      # make connections with higher-ID server, ID srvr
      Sys.sleep(2)  # make sure server acts before clients
      con <- socketConnection(host=host, port=port, blocking=TRUE,
         server=FALSE, open="w+b")
      serialize(myID,con)  # notify server of my ID
      partoolsenv$myServers[[srvr]] <- con
  }
}

# wrapper
ptMEsend <- function(obj,dest) {
   myID <- partoolsenv$myid
   con <- if (myID < dest) partoolsenv$myServers[[dest]] else
      partoolsenv$myClients[[dest]]
   serialize(obj,con)
}

# wrapper
ptMErecv <- function(src) {
   myID <- partoolsenv$myid
   con <- if (myID < src) partoolsenv$myServers[[src]] else
      partoolsenv$myClients[[src]]
   unserialize(con)
}

ptMEclose <- function() {
   myID <- partoolsenv$myid
   for (con in partoolsenv$myServers[[src]]) close(con)
   for (con in partoolsenv$myClients[[src]]) close(con)
}

ptMEtest <- function(cls) {
   myID <- partoolsenv$myid
   ncls <- partoolsenv$ncls
   myLeft <- if (myID > 1) myID-1 else ncls
   myRight <- if (myID < ncls) myID+1 else 1
   ptMEsend(myID,myRight)
   ptMErecv(myLeft)
}
  

