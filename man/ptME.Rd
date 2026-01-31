\name{ptMEinit,ptMEinitSrvrs,ptMEinitCons,ptMEsend,ptMErecv,ptMEclose, ptMEtest,ptMEtestWrkr}
\alias{ptMEinit}
\alias{ptMEinitSrvrs}
\alias{ptMEinitCons}
\alias{ptMEsend}
\alias{ptMErecv}
\alias{ptMEclose}
\alias{ptMEtest}
\alias{ptMEtestWrkr}

\title{
Message-passing utilities.
}

\description{Simple MPI-like functions.}

\usage{
ptMEinit(cls)
ptMEinitSrvrs()
ptMEinitCons(srvr)
ptMEsend(obj,dest)
ptMErecv(dest)
}

\arguments{
  \item{cls}{A cluster for the \pkg{parallel} package.}
  \item{srvr}{A server, one of the worker nodes.}
  \item{src}{A worker node from which to receive a message.}
  \item{dest}{A worker node to which a message is to be sent.}
  \item{obj}{An R object.}
}

\details{ 
This system of functions implements a message-passing system, similar to
MPI/Rmpi but much simpler and without the need for configuration.

Functions:

\itemize{

   \item \code{ptMEinit}:  General system initialization.

   \item \code{ptMEinitSrvrs}:  Called by \code{ptMEinit}. Sets up
   socket connections for each pair of worker nodes. Each worker node
   hosts a server for use by all nodes having \code{ partoolsenv$myid} 
   less than the server.  Returns the server port.

   \item \code{ptMEinitCons}: Also called by \code{ptMEinit}. Each worker
   node, acting as a client, makes a connection with all servers having
   \code{partoolsenv$myid} greater than the client.

   \item \code{ptMEsend}:  Send the given object to the given
   destination.

   \item \code{ptMErecv}:  Receive an object from the given
   source.  Returns the received object.

   \item \code{ptMEclose}:  Close all worker-worker connections.

}

}

\value{

The function \code{ptMErecv()} returns the received value.  The
intermediate function \code{ptMEinitSrvrs} returns a randomly chosen
server port number.

}

\author{
Robin Yancey, Norm Matloff
}

