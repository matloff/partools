
# execute the command cmd at each cluster node, typically select(), then
# collect using rbind() at the caller
distribgetrows1 <- function(cls,cmd) {
  clusterExport(cls,'cmd',envir=environment())
  res <- clusterEvalQ(cls,docmd(cmd))
  tmp <- Reduce(rbind,res)
  notallna <- function(row) any(!is.na(row))
  tmp[apply(tmp,1,notallna),]
}

# execute the command cmd at each cluster node, typically select(), then
# collect using rbind() at the caller
distribgetrows2 <- function(cls,cmd) {
  clusterExport(cls,'cmd',envir=environment())
  res <- clusterEvalQ(cls,docmd(cmd))
  res <- res[lapply(res,length)>0] # delete elements of length 0
  tmp <- Reduce(rbind,res)
  if (length(tmp) > 1){ # do only if more than 1 element
    notallna <- function(row) any(!is.na(row))
    tmp[apply(tmp,1,notallna),]
  }
  tmp
}

numstr <- function(j){
  if (length(j) == 1){
    str <- as.character(j)
  } else {
    str <- paste("c(", paste(j, sep="", collapse = ","), ")")
  }
}

findrow <- function(cls, i, objname){
  # get number of rows per worker
  cmd <- paste0("dim(", objname, ")[1]")
  nr <- distribgetrows2(cls, cmd) #DEBUG
  last <- cumsum(nr)
  frst <- c(0, last[1:length(last)-1]) + 1
  here <- i >= frst & i <= last
  iwrk <- 1:length(last)
  df <- data.frame(iwrk, frst, last, here, stringsAsFactors = FALSE)
  df$irow <- 1 + i - df$frst
  # calculate row to request and worker on which it exists
  irow <- df$irow[df$here == TRUE]
  iwrk <- df$iwrk[df$here == TRUE]
  c(irow, iwrk)
}

"[.ddf" <- function(obj, i=NULL, j=NULL){
  objname <- deparse(substitute(obj))
  cls <- attr(d,'cluster')
  # e.g. user called d[,]
  if (is.null(i) & is.null(j)){
    dr <- distribcat(cls, objname)
  }
  else if (is.null(i)){
    if (length(j) == 1){
      #cmd  <- paste0(objname, "[,", j, "]")
      #dr <- distribgetcols(cls, cmd)
      cmd  <- paste0(objname, "[,c(", j, ",", j, ")]")
      dr <- distribgetrows2(cls, cmd)[,1]
    } else {
      cmd  <- paste0(objname, "[,", numstr(j), "]")
      dr <- distribgetrows2(cls, cmd)
    }
  }
  else if (is.null(j)){
    if (length(i) == 1){ # could be done in else loop
      irow <- findrow(cls, i, objname)
      cmd <- paste0(objname, "[", irow[1], ",]")
      dr <- distribgetrows2(cls, cmd)[irow[2],]
    } else {
      dr <- NULL
      for (k in 1:length(i)){
        irow <- findrow(cls, i[k], objname)
        cmd <- paste0(objname, "[", irow[1], ",]")
        dr1 <- distribgetrows2(cls, cmd)[irow[2],]
        dr <- rbind(dr, dr1)
      }
    }
  }
  else{
    if (length(i) == 1){
      irow <- findrow(cls, i, objname)
      cmd <- paste0(objname, "[", irow[1], ",", numstr(j), "]")
      dr <- distribgetrows2(cls, cmd)[irow[2]]
    } else {
      dr <- NULL
      for (k in 1:length(i)){
        irow <- findrow(cls, i[k], objname)
        cmd <- paste0(objname, "[", irow[1], ",", numstr(j), "]")
        dr1 <- distribgetrows2(cls, cmd)[irow[2],]
        dr <- rbind(dr, dr1)
      }
    }
  }
  dr
}

# test

### nrows <- 9
### ncols <- 4 # cannot be changed
### cl <- makeCluster(4) # from 'parallel' library
### setclsinfo(cl) # from 'partools'
### 
### col1 <- seq(11, nrows*10 + 1, by = 10)
### col2 <- seq(12, nrows*10 + 2, by = 10)
### col3 <- seq(13, nrows*10 + 3, by = 10)
### col4 <- seq(14, nrows*10 + 4, by = 10)
### d <- data.frame(col1,col2,col3,col4)
### rownames(d) <- c(nrows:1) # should have no effect
### 
### # TO WORK AS EXPECTED, THE FOLLOWING ARE REQUIRED:
### # 1) cls must be defined
### # 2) the distributed variable must have a local definition
### # 3) the local definition must have ddf as its first class
### # There is a problem with single variables such as d[2,3] returning any of the following three:
### # 1) [1] 23
### # 2) init
### #      23
### # 3) <0 x 0 matrix>
### # Warning message:
### # In f(init, x[[i]]) :
### #   number of columns of result is not a multiple of vector length (arg 2)
### 
### distribsplit(cl, "d", scramble = FALSE)
### # print(clusterEvalQ(cl, { print(d) }))
### clusterEvalQ(cl, {d})
### dd <- d
### rm(d)
### d <- 123
### class(d) <- append("ddf", class(d))
### attr(d,'cluster') <- cl
### print("********** d **********")
### d
### print("********** d[,] **********")
### print(d[,])
### print("********** d[i,] **********")
### for (i in 1:nrows){
###   print(d[i,]) 
### }
### print("********** d[,j] **********")
### for (j in 1:4){
###   print(d[,j]) 
### }
### for (i in 1:nrows){
###   print(paste0("********** d[",i,",] **********"))
###   for (j in 1:4){
###     #print(paste0("********** d[",i,",",j,"] **********"))
###     print(d[i,j])
###   }
### }
### print(paste0("********** d[2:4,] **********"))
### print(d[2:4,])
### print(paste0("********** d[,2:4] **********"))
### print(d[,2:4])
### print(paste0("********** d[5:8,1:3] **********"))
### print(d[5:8,1:3])
### print(paste0("********** d[c(2,4),] **********"))
### print(d[c(2,4),])
### print(paste0("********** d[,c(2,4)] **********"))
### print(d[,c(2,4)])
### print(paste0("********** d[c(1,2,4,8),2:3] **********"))
### print(d[c(1,2,4,8),2:3])
### 
### readline("Press enter to stopCluster, escape to exit")
### stopCluster(cl)
### 
