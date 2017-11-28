

################## ptME: partools message exchange ########################

# code to allow worker nodes in partools to communicate directly with
# each other

SOCKET_RANGE = 3000:4000 # range of socket available to ptME
# run from the manager:
ptMEinit <- function(cls) {
  # send workers all the client IPs
  ncls <- length(cls)
  wrkrIPs <- unlist(Map(function(i) cls[i][[1]]$host, 1:ncls))
  ptMEenv <<- environment() # this will be global 
  assign('cls', cls, env=ptMEenv)


  clusterExport(cls, "wrkrIPs", env=environment())
  clusterExport(cls, "ncls", env=environment())
  # clusterCall(cls, assign("partoolsenv$wrkrIPs", wrkrIPs, env=partoolsenv))
  # print(clusterEvalQ(cls, ls(env=partoolsenv )))
  #   stop("here")
  clusterEvalQ(cls, partoolsenv$wrkrIPs <- wrkrIPs )
  clusterEvalQ(cls, partoolsenv$connections <- vector('list', length=ncls) )

  clusterExport(cls,c('ptMEinitSrvrs','ptMEinitCons'), envir=environment())

  clusterExport(cls,c('ptMEsend','ptMErecv','ptMEclose', ".establishConnection"),envir = environment())

  .establishConnections(cls)
  ptMEenv
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

# This function will testify the availalibilty of a port by opening a server on a host
# and trying to connect to it. 
# To save time, it opens a server on cls[[2]] to cls[[length(cls)]]
# and the first node will connect to all the server established 
.thrustSockets <- function(cls){
  nworker <- length(cls)
  available_ports <- vector('list', length=nworker)
  
  for (serverPtr in 2:nworker) {
    host <- cls[serverPtr][[1]]$host

    ptMEbroadcast(serverPtr)
    ptMEbroadcast(host)
    repeat{
      # print(serverPtr)
      port <- sample(SOCKET_RANGE, 1)

      ptMEbroadcast(port);

      results <- ptMEexecute( 
            .establishConnection(   serverId=serverPtr, 
                                    clientId=1, 
                                    host=host, 
                                    port=port) 
                            )

      if( results[[serverPtr]] && results[[1]] ){
        available_ports[[serverPtr]] = port
        break
      }
        
    }
    
  }
  available_ports
}


# This function finds the available port in each server, 
# Then it establish connections between the nodes 
.establishConnections <- function(cls){
  nworker <- length(cls)
  ports <- .thrustSockets(cls)
  print(ports)

  #print(clusterEvalQ(cls, partoolsenv$connections))
  #stop("here")
  # Establish connections
  for (serverPtr in 3:nworker  ){
    host <- cls[serverPtr][[1]]$host
    port <- ports[[serverPtr]]

    ptMEbroadcast(serverPtr)
    ptMEbroadcast(host)
    ptMEbroadcast(port)
    # it starts from 2 because the first node is connected to 
    # all the server while getting the ports 
    for (clientPtr in 2:(serverPtr-1)){
      result <- FALSE
      ptMEbroadcast(clientPtr)
    # print(paste(serverPtr, host, port, clientPtr))
      # print(paste("client: ", clientPtr, "|| serverId: ", serverPtr))
      # print(serverPtr)
      results <- ptMEexecute(
            .establishConnection( serverId=serverPtr,
                                  clientId=clientPtr, 
                                  host=host, 
                                  port=port) )
      # print( paste(results[[serverPtr]], results[[clientPtr]]) )
      result <- results[[serverPtr]] && results[[clientPtr]] 
      
    }
  }
}

.establishConnection = function(serverId, clientId, host, port, .NUM_OF_TRIALS=70){
  # establish server
  MyId <- partoolsenv$myid
  if (MyId == serverId){
    tryCatch({
      curWorkerCons <- partoolsenv$connections;
      con = socketConnection(
          host="localhost", 
          port = port, 
          blocking=TRUE,
          server=TRUE,
          open="r+b")
      curWorkerCons[[clientId]] <- con
      partoolsenv$connections <<- curWorkerCons
      return(TRUE);
    }, 
    error=function(e){
      # stop(e)
      print("Connection failed, retry...")
      return (FALSE);
    })
  }
  # connect to server 
  else if(MyId == clientId){
    con = -1

    curWorkerCons <- partoolsenv$connections;
    
    # 
    for (i in 1:.NUM_OF_TRIALS){

      tryCatch({
          con = socketConnection(
            host=host, 
            port = port, 
            blocking=TRUE,
            server=FALSE, 
            open="r+b")

          # connect successfully, 
          # save the connection and return true
          curWorkerCons[[serverId]] <- con
              
          partoolsenv$connections <<- curWorkerCons
          return (TRUE)
        }, 
        error=function(e){
          # connect to server failed, return FALSE
          print(paste("connect to server faile at ", 
              MyId, e))
        })
    }
    return (FALSE)

  }
  return (FALSE)
}

# set up the connections
ptMEinitCons<- function(srvr) {
  myID <- partoolsenv$myid
  host <- partoolsenv$wrkrIPs[[srvr]]
  port <- partoolsenv$portnums[[srvr]]
  #return(myID)
  if (myID == srvr) {  
     # make connections with lower-ID workers; note that among those
     # workers, they will not necessarily contact this server in the
     # order of their IDs, so we must ask them for IDs
     for (i in 1:(srvr-1)) {
        con <- socketConnection(host=host,port=port, 
                blocking=TRUE, server=TRUE, open="w+b")
        clientnum <- unserialize(con)
        partoolsenv$myClients[[clientnum]] <<- con
        return(con)
     }
  } else if (myID < srvr) {  

      # make connections with higher-ID server, ID srvr
      for(i in 1:30){

      tryCatch({
        # Sys.sleep(0.5*myID)  # make sure server acts before clients
        con <- socketConnection(host=host, port=port, blocking=TRUE,
           server=FALSE, open="w+b")
        serialize(myID,con)  # notify server of my ID
        partoolsenv$myServers[[srvr]] <<- con
        return(con)
      }, error=function(e){})
    }
  }
}

# wrapper
ptMEsend <- function(obj,dest) {
   # myID <- partoolsenv$myid
   # con <- if (myID < dest) partoolsenv$myServers[[dest]] else
   #    partoolsenv$myClients[[dest]]
   
  if (dest == get("myid", env=partoolsenv) || 
    dest <= 0 || 
    dest > get("ncls", env=partoolsenv))
    return()

  serialize(obj, partoolsenv$connections[[dest]])
}

# wrapper
ptMErecv <- function(src) {
   # myID <- partoolsenv$myid
   # con <- if (myID < src) partoolsenv$myServers[[src]] else
   #    partoolsenv$myClients[[src]]
   if (src == get("myid", env=partoolsenv) || 
        src <= 0 || 
        src > get("ncls", env=partoolsenv))
        return()
      
  unserialize(partoolsenv$connections[[src]])
}

ptMEbroadcast = function(obj, name=deparse(substitute(obj))){
  assign(name, obj, env=environment())
  clusterExport(ptMEenv$cls, name, env=environment())
}

ptMEexecute = function(cmd){
      
  scmd <- substitute(cmd)
  # scmd
  silence = ptMEbroadcast(scmd)
  return(clusterEvalQ(ptMEenv$cls, eval(scmd)))
}

ptMEclose <- function() {
  
  clusterEvalQ(ptMEenv$cls, 
    for (con in partoolsenv$connections) tryCatch( {close(con)}, error=function(e){})
  )
   #for (con in partoolsenv$myClients[[src]]) close(con)
}

ptMEtest <- function(cls) {
   myID <- partoolsenv$myid
   ncls <- partoolsenv$ncls
   myLeft <- if (myID > 1) myID-1 else ncls
   myRight <- if (myID < ncls) myID+1 else 1
   ptMEsend(myID,myRight)
   ptMErecv(myLeft)
}
  
