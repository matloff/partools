
\name{partools-package}
\alias{partools-package}
\alias{partools}
\docType{package}         
\title{Categorized List of Functions}

                                                                                \description{ 

This package provides a broad collection of functions for parallel
computation in R, either on multicore machines or clusters.  It includes
both high-level functions such as distributed aggregate, as well as
low-level building blocks.

For \bold{a more detailed introductio}n to the package, refer to the vignette.
This man page here is intended as a list that experienced
\code{partools} users can use \bold{for quick reference}.
                                                                                }

\details{

UNDER CONSTRUCTION

\bold{Forming Distributed files and data frames, and amalgamating them}

\itemize{

   \item \bold{filesplit():} Create a distributed file from monolithic one.

   \item \bold{filesplitrand():}  Create a distributed file from
monotlithic one, but randomize the record order.

   \item \bold{filecat():}  Create a monotlithic file from distributed
one.

   \item \bold{fileread():}  Read a distributed file into distributed
data
   frame.

   \item \bold{readnscramble():}  Read a distributed file into
distributed
   data frame,
   but randomize the record order.

   \item \bold{filesave():}  Write a distributed data frame to a
distributed
   file.

   \item \bold{filechunkname():} Returns the full name of the file chunk,
   associated with the calling cluster node, including suffix, e.g.
   '01', '02' etc.

   \item \bold{distribsplit():}  Create a distributed data frame/matrix
   from monotlithic one.

   \item \bold{distribcat():}  Create a monotlithic data frame/matrix
from
   distributed one.

}

}

