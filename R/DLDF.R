
# code to allow queries at the manager node of the form, e.g. d[['8.3]]
# for a distributed list of data frames d; needs a fake object of the same name,
# e.g. d, at the manager node, of class 'dldf'; one can construct such an
# object via makedldf()

# make object of class 'dldf', representing the distributed list of data frames
# named 'dname' on cluster 'cls'
makedldf <- function(dname,cls) {
   tmp <- 0
   class(tmp) <- 'dldf'
   attr(tmp,'dname') <- dname
   attr(tmp,'cluster') <- cls
   eval(parse(text =
      paste0('assign("',dname,'",tmp,envir=.GlobalEnv)')))
}

"[[.dldf" <- function(obj, str=NULL){
  objname <- deparse(substitute(obj))
  cls <- attr(obj,'cluster')
  cmd  <- paste0(objname, "[['", str, "']]")
  dr <- distribgetrows(cls, cmd)
}

printdldf <- function(str,mtcsp){
  cat(paste0("##### mtcsp[[", str, "]] #####"))
  print(mtcsp[[str]])
}
# test class dldf
testdldf <- function(){
  library(partools)
  cl <- makeCluster(2)
  mtc <- mtcars
  distribsplit(cl, 'mtc')
  #print(clusterEvalQ(cl, mtc))
  print(clusterEvalQ(cl, mtcsp <- split(mtc, list(mtc$cyl, mtc$gear))))
  print(clusterEvalQ(cl, mtcsp[['8.3']]))

  makedldf("mtcsp", cl)
  partoolsenv <- new.env()
  clusterExport(cl, c("getpte", "partoolsenv"), envir=environment())
  printdldf("4.3")
  printdldf("6.3")
  printdldf("8.3")
  printdldf("4.4")
  printdldf("6.4")
  printdldf("8.4")
  printdldf("4.5")
  printdldf("6.5")
  printdldf("8.5")

  readline("Press enter to stopCluster, escape to exit")
  stopCluster(cl)
}
