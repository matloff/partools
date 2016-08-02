
# calars(), SA computation of lars()

# arguments:

#    cls: 'parallel' cluster
#    ddf: name of a distributed data frame
#    xcols: column numbers of ddf of X matrix
#    ycol: column number of ddf of Y vector
#    type: type of analysis
#    outfl: name of output file

calars <- function(cls,ddf,xcols,ycol,type='lasso',outfl=NULL) {
   clusterExport(cls,c('xcols','ycol'),envir=environment())
   cmd <- paste('x <<- as.matrix(',ddf,'[,',xcols,'])',sep='')
   doremotecmd(cls,cmd)
   cmd <- paste('y <<- as.matrix(',ddf,'[,',ycol,'])',sep='')
   doremotecmd(cls,cmd)
}

# executes the command in the string cmd at the worker nodes of the
# cluster
doremotecmd <- function(cls,cmd) {
   clusterExport(cls,'cmd',envir=environment())
   clusterEvalQ(cls,docmd(cmd))
}


##  data(prgeng) 
##  pe <- prgeng 
distribsplit(cls,'pe') 
clusterEvalQ(cls,head(pe)) 
calars(cls,'pe',c(1,7),8) 
clusterEvalQ(cls,head(x)) 

