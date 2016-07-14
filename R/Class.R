

# wrapper to do distributed fit 


# arguments:

#    cls: an R 'parallel' cluster
#    fitcmd: quoted string to do a classfication fit, e.g. 
#       'rpart(Type ~ ., data = Glass)'; to be run on each chunk of 
#       distributed data

# value: 

#    an R list, whose i-th element is the object returned by
#    running 'fitcmd' on the i-th chunk of the distributed data

caclassfit <- function(cls,fitcmd) {
   clusterExport(cls,'fitcmd',envir=environment())
   clusterEvalQ(cls,fit <- docmd(fitcmd))
}

# arguments:

#    cls: an R 'parallel' cluster
#    fitobjs: output of a call to caclassfit(); an R list of elements of
#             class returned by the fitting algorithm, e.g. by rpart()
#    newdata: data to be predicted
#    yidx: index, if any, of the class variable in newdata
#    ...: additional parameters specific to the given algorithm

# value:

#    object of class 'caclasspred', with components

#       predmat: matrix of predicted values, with row i being the
#                predicted classes for newdata based on fitobj[[i]]
#       preds:   predicted classes for newdata based on voting
#                analysis of predmat
#       consensus:  proportion of newdata cases in which all rows of
#                   predmat agree
#       acc:  if yidx non-NULL, overall proportion of correct 
#             classification 
#       confusion:  if yidx non-NULL, confusion matrix

caclasspred <- function(cls,fitobjs,newdata,yidx=NULL,...) {
   res <- list()
   class(res) <- 'caclasspred'
   newdatax <- newdata[,-yidx]
   chunkpreds <- lapply(fitobjs, function(fitobj)
      as.numeric(predict(fitobj,newdatax,...)))
   res$predmat <- Reduce(rbind,chunkpreds)
   res$preds <- apply(res$predmat,2,vote)
   allsame <- function(u) all(u == u[1])
   res$consensus <- mean(apply(res$predmat,2,allsame))
   if (!is.null(yidx)) {
      newys <- newdata[,yidx]
      res$acc <- mean(res$preds == newys)
      res$confusion <- table(true=newys,pred=res$preds)
   }
   res
}

# finds all values in preds having max freq, and randomly chooses one of 
# them
vote <- function(preds) {
   tpreds <- table(preds)
   tmp <- which(tpreds == max(tpreds))
   whichmostfreq <- as.numeric(names(tmp))
   sample(whichmostfreq,1)
}

