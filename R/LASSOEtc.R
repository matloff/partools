
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
   cmd <- paste('x <<- as.matrix(',ddf,'[,xcols])',sep='')
#    clusterExport(cls,'cmd',envir=environment())
#    clusterEvalQ(cls,x <- docmd(cmd))
   doremotecmd(cls,cmd)

   # clusterEvalQ(cls,y <- ddf[,ycol])
   # clusterEvalQ(cls,larsout <- lars(x,y,type))
}

# executes the command in the string cmd at the worker nodes of the
# cluster
doremotecmd <- function(cls,cmd) {
   clusterExport(cls,'cmd',envir=environment())
   clusterEvalQ(cls,docmd(cmd))
}


##  data(prgeng) 
##  pe <- prgeng 
##  x <- pe[,c(1,6,8)] 
##  y <- pe$wageinc 
##  xx <- cbind(x,matrix(rnorm(nrow(pe)*500),ncol=500)) 
##  system.time(o1 <- lars(xx,y)) 

