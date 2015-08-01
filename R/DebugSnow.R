
# (in addition to dbs(), see "quick and dirty" utils at the end of this
# file)

# dbs() ("debug Snow") and the other functions here serve to automate
# the process of debugging code written in Snow, meaning the portion of
# R's "parallel" library derived from the old "snow" package, e.g. the
# function clusterApply(); this part of the library will be referred to
# as "snow" below

# may also be used to debug Rdsm applications, since that library uses
# snow for launching

# requires the Unix utility "screen" (Linux, Mac, Cygwin)

# typical usage pattern (see alsodbscat() below):

#    say have function f() in x.R to be debugged, and
#    say we'll debug using 2 workers
#
#    1.  run 
#
#           > dbs(2,src="x.R",ftn="f")  # 2 workers, running xterm
#
#    2.  the function will open new windows for the workers and the
#        manager, with the having already automatically
#        sourced x.R and called debug(f)
#
#    3.  in the manager window, launch your snow app as usual, e.g.
#
#           > clusterEvalQ(cls,f(5))
#    
#        and then single-step through f() in each worker window, as usual

# an outline of how the code works is given at the end of this file

# the function writewrkrscreens() sends a command to all worker windows;
# useful examples:

# in Mac, if no xterm, use
# /Applications/Utilities/Terminal.app/Contents/MacOS/Terminal

# see notes on screen management at the end of this file

dbs <- function(nwrkrs,xterm=NULL,src=NULL,ftn=NULL) {
   killdebug()  # in case any left over from before
   # set up manager window
   makescreen("Manager",xterm)
   if (!is.null(xterm)) {
      cat("wait a moment\n")
      Sys.sleep(5)  
   } else {
      readline()
   }
   writemgrscreen("R")
   writemgrscreen("library(parallel)")
   # set up debuggee (worker) windows
   for (i in 1:nwrkrs) {
      makescreen("Worker",xterm)
   if (!is.null(xterm)) {
      cat("wait a moment\n")
      Sys.sleep(5)  
   } else {
      readline()
   }
   }
   # launch Snow by hand from manager window
   port <- sample(11000:11999,1)
   cmd <- 
      paste("cls <- makeCluster(",nwrkrs,",manual=T,port=",port,")",sep="")
   writemgrscreen(cmd)
   # need throttling
   ### if (xterm) Sys.sleep(5)
   # at each worker window, have run R, connected to manager
   rcmd <- paste("R --vanilla --args MASTER=localhost PORT=",port,
      " SNOWLIB=/usr/local/lib/R/library TIMEOUT=2592000 METHODS=TRUE",sep="")
   writewrkrscreens(rcmd)
   writewrkrscreens("parallel:::.slaveRSOCK()")
   # make sure we are all in the same directory
   mydir <- getwd()
   paste('setwd("',mydir,'")',sep='')
   setmydir <- paste('setwd("',mydir,'")',sep='')
   writemgrscreen(setmydir)  # mgr screen now in this directory
   wsetmydir <- paste('clusterEvalQ(cls,',setmydir,')',sep='')
   writemgrscreen(wsetmydir)  # wrk screens now in this directory
   if (!is.null(src)) {
      tmp <- paste("clusterEvalQ(cls,source('",src,"'))",sep="")
      writemgrscreen(tmp)
      if (!is.null(ftn)) {
         tmp <- paste("clusterEvalQ(cls,debug(",ftn,"))",sep="")
         writemgrscreen(tmp)
      }
   }
   # set library paths at manager, workers
   lp <- .libPaths()
   writemgrscreen(paste(".libPaths('",lp,"')",sep=""))
   writemgrscreen("library(partools)")
   writemgrscreen("exportlibpaths(cls)")
   writemgrscreen("clusterEvalQ(cls,library(partools))")
   cat("ready to launch your app in the manager window\n")
}

# set up a window, whether manager or worker (specified in screentype)
makescreen <- function(screentype,xterm) {
   dn <- screentype
   if (!is.null(xterm)) {  # make window and run screen there
      cmd <- paste(xterm,' -e "screen -S',sep="")
      cmd <- paste(cmd,dn)
      cmd <- paste(cmd,'" &')
      system(cmd)
   } else {
      msg <- paste("screen -S",dn)
      msg <- paste("in a terminal window run",msg,
         "in that window, then hit Enter here")
      cat(msg,"\n")
   }
}

# write command to manager window
writemgrscreen <- function(cmd) {
   cmd <- paste(cmd,'\n',sep="")
   cmd <- shQuote(cmd)
   mgr <- getmgrscreen()
   tosend <- paste('screen -S ',mgr,' -X stuff ', cmd, sep="")
   system(tosend)
}

# write command to all worker windows
writewrkrscreens <- function(cmd) {
   cmd <- paste(cmd,'\n',sep="")
   for (dn in getwrkrscreens()) {
      tosend <- paste('screen -S ',dn,' -X stuff ', "'",cmd,"'", sep="")
      system(tosend)
   }
}

# determine screen names of all worker windows
getwrkrscreens <- function() {
   scrns <- vector()
   tmp <- system("screen -ls",intern=T)
   for (l in tmp) {
      # debuggee screen?
      if (length(grep("Worker",l)) > 0) {
         scrn <- getscreenname("Worker",l)
         scrns <- c(scrns,scrn)
      }
   }
   scrns
}

# determine screen name of manager window
getmgrscreen <- function(warning=T) {
   tmp <- system("screen -ls",intern=T)
   for (l in tmp) {
      # mgr screen?
      if (length(grep("Manager",l)) > 0) {
         scrn <- getscreenname("Manager",l)
         return(scrn)
      }
   }
   if (warning) cat("warning:  no manager screen found")
   return(NULL)
}

# for a line lsentry from the output of "screen -ls" and given
# screentype (here "Worker" or "Manager"), returns the screen name, in
# the form of OS process number + screentype
getscreenname <- function(screentype,lsentry) {
   # remove leading TAB character
   substr(lsentry,1,1) <- ""
   startoftype <- gregexpr(screentype,lsentry)[[1]]
   endofname <- startoftype + nchar(screentype) - 1
   substr(lsentry,1,endofname)
}

# kill all worker and manager screens
killdebug<- function() {
   scrns <- getwrkrscreens()
   scrns <- c(scrns,getmgrscreen(warning=F))
   for (scrn in scrns) {
      cmd <- paste("screen -X -S ",scrn," kill",sep="")
      system(cmd)
   }
}

# screen management:

#    the routines here, e.g. killdebug() should be sufficient to
#    delete zombie screens and so on, but it is worth knowing these
#    commands, if at least to understand the above code:

#        screen -ls:  lists all screens, in a OS process number + name
#                     format
#        screen -S screenname -X stuff cmd:  write cmd to the specified screen
#        screen -X -S screenname kill:  kill the specified screen

# dbs() does the following:

#   uses "screen" create new terminal windows in which the debuggees will run,
#      one window for each worker, and one for the manager
#   has the manager window run a makeCluster() command 
#   starts up R processes in the worker windows
#   writes the string "parallel:::.slaveRSOCK()" to each of the windows,
#      which means they now loop around looking for commands from the manager,
#      as in usual Snow; NOTE:  however, while in browser mode, input 
#       will be directly from the user typing into the worker window (or
#       from writewrkrscreens() from the original window)

###################################################################

# "quick and dirty" debugging, by printing to a file; node 1 prints
# messages to the file dbs.001, node 2 to dbs.002 etc.

dbsmsgstart <- function(cls) {
   ncls <- length(cls)
   # ndigs <- ceiling(log10(ncls))
   ndigs <- getnumdigs(ncls)
   for (i in 1:length(cls))
      cat("\n",file=filechunkname("dbs",ndigs,i),"\n")
}

dbsmsg <- function(msg) {
   pte <- getpte()
   # ndigs <- ceiling(log10(pte$ncls))
   ndigs <- getnumdigs(pte$ncls)
   fn <- filechunkname("dbs",ndigs)
   cat(msg,file=fn,append=TRUE,"\n")
}
              
