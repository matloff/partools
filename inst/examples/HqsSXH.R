library(partools)


# return true if '@param:numberToCheck' is a power of '@param:power'
isPowerOfNum = function(numToCheck, power) {
	if (numToCheck == 0)
        return ( FALSE );
	while (numToCheck != 1)
	{
        if (numToCheck%%power != 0)
        return ( FALSE );
        numToCheck = numToCheck/power;
	}
	return ( TRUE );

}

# @overview: compute the partner who 'me' will be exchanging data with
# @param 'me': the id of the worker
# @param 'leadWorker': the first node on each hyper cube
# @param 'groupSize': the size of the hyper cubes
getPartner <- function(me, leadWorker, groupSize){
	if (me < leadWorker + groupSize){
		return( me + groupSize )
	}
	else {	
		return( me - groupSize )
	}
	
}

#################
# COMPUTE PIVOT # 
#################
# @overview: compute the the first @param:numOfMedian2gather nodes' medians
# 			 compute their median, which will be the pivot point
# @param:'me': the id of the worker
# @param:'leadWorker': the first node on each hyper cube
# @param:'nworker': the size of each hyper cube
getPivot <- function(me, partner, leadWorker, numOfMedian2gather, nworkers){
	if( nworkers < numOfMedian2gather ) stop("Invalid Argument");

	pivot = 0;
	# nodes2gatherMedian <- 
	groupSize = numOfMedian2gather

	# the half of the nodes whose data will determine 
	# the pivot points for the whole hyper cube 
	if (me < leadWorker + groupSize){ # send median

		if ( me == leadWorker )
		{
			myMedian = median(data2sort)
			## this part will not work when group size = 1
			ms = rep(0.0, groupSize / 2 - 1)

			# no need to gather median if we only need 1
			if (groupSize > 1){
				# gather median from the first half nodes 
				# and compute the median of the medians gathered from workers
				ms <- sapply((leadWorker+1):(leadWorker + groupSize/2),
							  function(worker)
								{
									return(ptMErecv(worker))
								}, 
							  simplify = TRUE, USE.NAMES = TRUE)
				pivot = median(c(myMedian, ms))
			}
			else
				pivot = myMedian;

			# send computed pivot to the workers 
			sapply((leadWorker+1):(leadWorker + nworkers - 1), 
					function(worker)
						{ptMEsend (pivot, worker)}, 
					simplify = TRUE, USE.NAMES = TRUE)
		}
		else{
			ptMEsend (median(data2sort),leadWorker)
		}
	}

	# get pivot from lead worker
	if( me != leadWorker){
		pivot <- ptMErecv(leadWorker);
	}
	
	return (pivot)

}

# @overview:
#	 1. computes the pivot point of each hyper cube
#	 2. broadcast the pivot to each nodes
#	 3. shift data with each computed 'partner' node 
#	 Note: this function will be called on each iteration
# @param:'leadWorker': the first node on each hyper cube
# @param:'nworker': the size of each hyper cube
HyperQuickSort_helper <- function(leadWorker, nworkers){
	groupSize = nworkers / 2;
	if( groupSize < 1 )
		return()
	me <- partoolsenv$myid;

	# the partner node who will be exchanging data with 'me'
	partner <- getPartner(me, leadWorker, groupSize)
	# the pivot point for all the dataset
	pivot <- getPivot(me, partner, leadWorker, groupSize, nworkers)


	###############
	# DIVIDE DATA #
	###############
	lower <- data2sort[data2sort < pivot];
	upper <- data2sort[data2sort >= pivot];

	#############
	# SEND DATA # 
	#############
	# if 'me' is from the first hypercube of the current group
	# Then send the upper half of the data to 'partner'
	if (me <= (leadWorker + groupSize - 1) ){ 
		ptMEsend (upper, partner)
		newData <- ptMErecv(partner)
		data2sort <<- c(lower, newData)	
	}
	# if 'me' is from the second hypercube of the current group
	# Then send the lower half of the data to 'partner'
	else {			
		newData <- ptMErecv(partner)
		ptMEsend (lower, partner)
		data2sort <<- c(upper, newData)
	}
}

# Initialization function of hyper quick sort
# the manager node will be calling this function 
# Note: This function assume PtME was initialized, and it will not close ptME
hqs <- function (cls, xname){
	nworkers = length(cls)

	# check if the number of nodes is a power of 2 
	# Else return error
	if(!isPowerOfNum(nworkers, 2)) stop( "nworkers must be of power of 2.");
	
	numOfIteration = log2(nworkers);
	clusterExport(cls, 
		c(	'xname',
			'nworkers', 
			'HyperQuickSort_helper', 
			'getPivot', 
			'getPartner',
			'ARRAY_SIZE'),
		env=environment()
		)
	# print(clusterEvalQ(cls, get(xname[partoolsenv$myid], env=globalenv())))
	clusterEvalQ(cls, 
		data2sort <- c(	get(xname[partoolsenv$myid], 
						env=globalenv()))  )

	for (iteration in 1:numOfIteration){
		# size of the hyper cube for the current iteration
		groupSize = nworkers / 2^iteration; 
		# number of hyper cube for the current iteration
		numOfLeads = nworkers / groupSize;
		
		#leaderlist: a list specifying the first node of 
		# each hyper cube
		leaders <- seq(1,nworkers, groupSize*2) 
		leaderlist <- unlist(lapply(leaders,
								function(x){rep(x, groupSize*2)}))
		# broadcast variables
		clusterExport(cls, 
			c(	'leaderlist', 
				'groupSize'),
			env=environment()
		)

		# leaderlist[partoolsenv$myid]: get the lead for each node
		clusterEvalQ(cls, HyperQuickSort_helper(	
						leaderlist[partoolsenv$myid], 
						groupSize*2))
		
	}
	# replace the variable xname pointing to 
	clusterEvalQ(cls, assign(xname[partoolsenv$myid], data2sort))

}
