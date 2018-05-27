
\name{partools-package}
\alias{partools-package}
\alias{partools}
\docType{package}         
\title{Overview and Package Reference Guide}
                                                                                \description{ 

This package provides a broad collection of functions for parallel data
manipulation and numerical computation in R, either on multicore
machines or clusters.  It includes both high-level functions such as
distributed aggregate, as well as low-level building blocks.

This man page here is intended as a quick overview for newcomers, and as
a list that experienced \pkg{partools} users can use for quick
reference.  }

\details{

\bold{Definitions}

The user has an instance of R, the \emph{manager} node, running 
as the "main" function.  One first sets up a (virtual) cluster there,
using R's built-in \pkg{parallel} package.  The elements 
of the cluster will be referred to as \emph{worker} nodes.

A \emph{distributed} object, typically a data frame, is held in parts,
one part per worker node.  An ordinary object, held at the manager node,
is termed \emph{monolithic}.  

A distributed file will consist of parts, each of which is in a separate
physical file.  For example, a distributed file \code{x} might consist
of physical files \code{x.01}, \code{x.02} and so on, but viewed
programmaticly at a single file.  The file contents are assumed to be in
the standard format of a constant number of fields per record.

\bold{The "Leave It There" Principle}

Making the best use of this package centers around our Leave It There
principle, which simply says that one keeps objects distributed as long
as possible.  An object, say a data frame, may originally be created on
the manager node but then be split into a distributed version at the
worker nodes.  As much as possible, the work in the user's R session
will involve that distributed data frame, with the outputs of the user's
various operations NOT being collected back at the manager.  This is a
crucial point, as it saves communication overhead, thus speeding up
one's application code.

\bold{Software Alchemy}

This is our term for a statistical method, studied by a number of
authors, for parallelizing computaton.  Say for instance we are
performing logistic regression.  Our data is converted to distributed
form (if not already in that form); we run the logit model at each
worker node, yielding a vector of estimated regression. coefficients,
then average those vectors to obtain our final set of estimated
coefficients.

This will often result in linear, or even superlinear, speedup.

Also referred to as \emph{chunk averaging}, 'ca'.

\bold{Startup and Global Information}

The user forms a \code{parallel} cluster \code{cls}, then calls
\code{setclsinfo(cls)} to initialize it.  This creates an R environment
\code{partoolsenv} at each worker node, with components \code{myid}, the
node's ID, and \code{ncls}, the number of workers in the cluster.

\bold{Function List}

\emph{Functions for Forming Distributed Files and Data Frames, 
Manipulating Them, and Amalgamating Them}

\itemize{

   \item \code{filesplit():} Create a distributed file from a monolithic one.

   \item \code{filesplitrand():}  Create a distributed file from
   monotlithic one, but randomize the record order.

   \item \code{filecat():}  Create a monotlithic file from distributed
   one.

   \item \code{fileread():}  Read a distributed file into distributed
   data frame.

   \item \code{readnscramble():}  Read a distributed file into
   distributed data frame, but randomize the record order.

   \item \code{filesave():}  Write a distributed data frame to a
   distributed file.

   \item \code{filechunkname():} Returns the full name of the file chunk,
   associated with the calling cluster node, including suffix, e.g.
   '01', '02' etc.

   \item \code{filesort():}  Disk-based sort.

   \item \code{distribsplit():}  Create a distributed data frame/matrix
   from monotlithic one.

   \item \code{distribcat():}  Create a monotlithic data frame/matrix from
   distributed one.

   \item \code{distribagg():}  Distributed analog of R's
   \code{aggregate()}, returning result to manager.  Has 
   special-case functions \code{distribcounts} and 
   \code{distribmeans}.  The function \code{fileagg()} is 
   a file-based analog of \code{distribagg()}, while \code{dfileagg()}
   returns results as a distributed data frame.

   \item \code{distribrange():}  Distributed analog of R's
   \code{range()}.

   \item \code{distribrange():}  Distributed analog of R's
   \code{range()}.

   \item \code{dwhich.min(), dwhich.max():}  Distributed analog of R's
   \code{which.min()} and \code{which.max()}.

   \item \code{distribgetrows():}  Distributed analog of R's
   \code{select()}, inputing a distributed data frame and 
   returning the result to the manager.  The function
   \code{filegetrows()} does the same on a distributed file, and
   \code{dfilegetrows()} does this too except that the result is a
   distributed data frame.

   \code{dTopKVals():}  Finds the k largest/smallest values in a
   distributed vector.

   \code{parpdist():}  Parallel computation of the distances matrix from
   one matrix to another.

}

\emph{Software Alchemy Functions}

\itemize{

   \item \code{ca():} General chunk averaging.  Core is \code{cabase()}.

   \item \code{calm(), caglm(), caprcomp(), cakm(), caknn(), carq():} 
   Chunk averaging versions of linear and generalized linear models, 
   k-Nearest Neighbors and quantile regression.

   \item \code{cameans(), caquantile():}  Chunk averaging methods for
   finding means and quantiles.

}

\emph{Sorting Functions}

The main one is \code{hqs()}, which performs a hyperquicksort among the
worker nodes without manager node intervention.  Note that this function
operates in keeping with the Leave It There principle; both inputs and
outputs are distributed vectors.  Timing comparisons to R's built-in
sequential sort should then collect a distributed vector to the manager
node, sort there, then distribute back to the workers.

Two versions of disk-based sorting are available, \code{filesort()} and
\code{disksort()}.  These should be considered experimental.

\emph{Message Passing Functions}

These provide direct communication between worker nodes, useful for
instance in \code{hqs()}.  Only simple send and receive are available at
present.

\itemize{

  \item \code{ptMEinit():}  Initialize.  Calls \code{ptMEinitSrvrs()}
  and \code{ptMEinitCons()}, which set up the servers and the
  client-server connections.

  \item \code{ptMEsend(), ptMErecv():}  Send and receive functions.

 }

\emph{Helper Functions}

\itemize{

   \item \code{formrowchunks():}  Does just that, forms chunks of rows
   of a data frame or matrix.

   \item \code{addlists():}  Helper function.  Adds two lists having the
   same keys.

   \item \code{geteltis():}  Extracts from a list of R vectors element
   \code{i} from each.

   \item \code{getnumdigs():}  Determines the number of digits in a
   positive integer, e.g. 1 for 8, 2 for 12, 3 for 550 and so on.

   \item \code{makeddf():}  Enables a distributed data frame to be
   viewed virtually as a monolithic one, using global row numbers.
   The function \code{findrow} goes in the opposite direction.  For a
   given row number in the virtual data frame, this function will
   return the row number within node, and the node number.

 }

}

