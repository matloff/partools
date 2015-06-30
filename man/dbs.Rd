\name{dbs}
\alias{dbs}
\alias{writemgrscreen}
\alias{writewrkrscreens}
\alias{killdebug}
\alias{dbsmsg}
\alias{dbsmsgstart}

\title{
Debugging aid for \pkg{parallel} cluster code.
}

\description{
Aids in debugging of code written for the cluster operations in the
\pkg{parallel} package.
}

\usage{
dbs(nwrkrs,xterm=NULL,src=NULL,ftn=NULL)
writemgrscreen(cmd)
killdebug()
dbsmsgstart(cls)
dbsmsg(msg)
}

\arguments{
  \item{cls}{A cluster for the \pkg{parallel} package.}
  \item{nwrkrs}{Number of workers, i.e. size of the cluster.}
  \item{xterm}{The string "xterm" or name of compatible terminal.}
  \item{src}{Name of the source file to be debugged.}
  \item{ftn}{Name of the function to be debugged.}
  \item{cmd}{R command to be executed in manager screen.}
  \item{msg}{A message to write to the debugging record file. Can be
     either a character string or any expression that is printable by
     \code{cat}.}
}

\details{A major obstacle to debugging cluster-based \pkg{parallel}
applications is the lack of a terminal, thus precluding direct use of
\code{debug} and \code{browser}.  This set of functions consists of
two groups, one for ``quick and dirty'' debugging, that writes
debugging information to disk files, and the other for more
sophisticated work that deals with the terminal restriction.  For both
methods, make sure \code{setclsinfo} has been called.

For ``quick and dirty'' debugging, there is \code{dbsmsg}, which prints
messages to files, invoked from within code running at the cluster nodes.  
There is one file for each member of the cluster,
e.g. \code{dbs.001}, \code{dbs.002} and so on, and \code{dbsmsg} writes
to the file associated with the worker invoking it.  Initialize via
\code{dbsmsgstart}.  

The more elaborate debugging tool, \code{dbs}, is the only one in this
\pkg{partools} package requiring a Unix-family system (Linux, Mac).  To
discuss it, suppose you wish to debug the function \code{f} in the file
\code{x.R}.  Run, say, \code{dbs(2,xterm="xterm",src="x.R",ftn="f")}.
Then three new terminal windows will be created, one for the cluster
manager and two for the cluster workers.  The cluster will be named
\code{cls}.  Automatically, the file \code{x.R} will be sourced by the
worker windows, and \code{debug(f)} will be run in them.

Then you simply debug as usual.  Go to the manager window, and run
your \pkg{parallel} application launch call in the usual way, say
\code{clusterEvalQ(cls,f(5))}.  The function \code{f} will run in each
worker window, with execution automatically entering browser mode.  You
are now ready to single-step through them, or execute any other browser
operation.

If \code{xterm} is NULL, you will be prompted to create the terminal
windows by hand (or use existing ones), and run \code{screen} there as
instructed.  \code{Terminal} works on Macs; label the windows by hand,
by clicking "Shell" then "Edit".

When finished with the debugging session, run \code{killdebug} from the
original window (the one from which you invoked \code{dbs}) to quit the
various \code{screen} processes.
}

% \value{
% }

\examples{
\dontrun{
# quick-and-dirty method
cls <- makeCluster(2) 
setclsinfo(cls)
# define 'buggy' function
g <- function(x,y) {u<-x+y; v<-x-y; dbsmsg(c(u,v)); u^2+v^2} 
clusterExport(cls,"g") 
# set x and y at cluster nodes
clusterEvalQ(cls,{x <- runif(1); y <- runif(1)}) 
# start debugging session
dbsmsgstart(cls) 
# run
clusterEvalQ(cls,g(x,y)) 
# files dbs.1 and dbs.2 created, each reporting u,v values

# dbs() method
# make a test file
cat(c("f <- function(x) {","   x <- x + 1","   x^2","}"),file="x.R",sep="\n")
dbs(2,src="x.R",ftn="f")
# now type in manager window:
clusterEvalQ(cls,f(5))
# the 2 worker windows are now in the browser, ready for debugging
}
}

\author{
Norm Matloff
}

