
# calars(), SA computation of lars()

# arguments:

#    cls: 'parallel' cluster
#    ddf: quoted name of a distributed data frame
#    xcols: quoted column numbers of ddf of X matrix
#    ycol: column number of ddf of Y vector
#    type: type of analysis
#    outfl: name of output file

# calars <- function(cls,ddf,xcols,ycol,type='lasso',outfl=NULL) {
calars <- function(cls,larscmd,outfl=NULL) {
#    clusterExport(cls,c('xcols','ycol'),envir=environment())
#    cmd <- paste('x <- as.matrix(',ddf,'[,',xcols,'])',sep='')
#    doremotecmd(cls,cmd)
#    cmd <- paste('y <- as.matrix(',ddf,'[,',ycol,'])',sep='')
#    doremotecmd(cls,cmd)
   cmd <- paste('larsout <- ',larscmd,sep='')
   doremotecmd(cls,cmd)
}

# executes the command in the string cmd at the worker nodes of the
# cluster
doremotecmd <- function(cls,cmd) {
   clusterExport(cls,'cmd',envir=environment())
   clusterEvalQ(cls,docmd(cmd))
}

# library(partools)
# cls <- makeCluster(2)
# setclsinfo(cls)
# data(prgeng) 
# pe <- prgeng 
# distribsplit(cls,'pe') 
# clusterEvalQ(library(lars))
# calars(cls,'lars(as.matrix(pe[,c(7,8)]),pe[,8])')
# clusterEvalQ(cls,head(x)) 
# 
