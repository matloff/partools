
# calars(), SA computation of lars()

# arguments:

#    cls: 'parallel' cluster
#    larscmd:  a call to lars(), quoted
#    outfl: name of output file

# calars <- function(cls,ddf,xcols,ycol,type='lasso',outfl=NULL) {
calars <- function(cls,larscmd,outfl=NULL) {
   cmd <- paste('larsout <- ',larscmd,sep='')
   # larsouts <- doremotecmd(cls,cmd)
   larsouts <- doclscmd(cls,cmd)
   if (!is.null(outfl)) 
   
   
   save(larscmd,file=outfl)
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
