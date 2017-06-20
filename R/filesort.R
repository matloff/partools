#' Sort File On Disk
#'
#' This function is designed to handle files larger than memory. At most
#' \code{nrows} will be present in memory at once. It is not parallel.
#' For this to work efficiently it's necessary that the data between
#' \code{breaks} fits into memory.
#'
#' @param infile unsorted file like object to read from. See \code{\link[utils]{read.table}}.
#' @param outfile where to write the sorted file. See
#' \code{\link[utils]{write.table}}. If \code{infile} is the name of a file
#' then the default prepends "sorted_" to this name.
#' @param sortcolumn which column of the data frame to sort on
#' @param breaks vector giving points to split data for binning
#' @param nrows number of rows in the data.frame held in memory
#' @param nbins number of bins for bin sort. Ignored if \code{breaks} is
#' specified.
#' @param read.table.args named list of extra arguments to read.table
#' @param write.table.args named list of extra arguments to write.table.
#' Defaults to using read.table.args to preserve the original formatting.
#' @param cleanup remove intermediate files?
#' @export
disksort = function(infile
                    , outfile = NULL
                    , sortcolumn = 1L
                    , breaks = NULL
                    , nrows = 1000L
                    , nbins = 10L #TODO: This breaks if too few unique values
                    , read.table.args = NULL
                    , write.table.args = NULL
                    , cleanup = FALSE
                    )
{
    # Prepare input / output connections

    if(is.character(infile)){
        if(is.null(outfile))
            outfile = paste0("sorted_", infile)
        infile = file(infile)
    }

    if(is.character(outfile)){
        if(file.exists(outfile))
            stop(outfile, " already exists.")
        outfile = file(outfile)
    }

    if(!isOpen(infile))
        open(infile, "rt")

    if(!isOpen(outfile))
        open(outfile, "at")

    # It would be more robust to sample from the whole file.
    # But it's not possible to seek on a more general connection.

    firstchunk = read.table(infile, nrows = nrows)

    if(is.null(breaks)){
        samp = sort(firstchunk[, sortcolumn])
        per_bin = round(nrows / nbins)
        breaks = samp[per_bin * (1:(nbins-1))]
    }

    # Write intermediate bin files
    binresult = streambin(infile = infile
        , firstchunk = firstchunk
        , sortcolumn = sortcolumn
        , breaks = breaks
        , nrows = nrows
        , read.table.args = read.table.args
        )

    # Actual sorting
    lapply(binresult[["bin_file_names"]], sortbin
           , sortcolumn = sortcolumn
           , outfile = outfile
           , nchunks = binresult[["nchunks"]]
           )

    # All done
    close(outfile)
    if(cleanup){
        unlink(binresult[["bindir"]], recursive = TRUE)
    }
}


#' @describeIn disksort Stream File Into Bins
#'
#' Read a data frame, split it into bins, and write to those
#' bins on disk.
streambin = function(infile
                    , firstchunk
                    , sortcolumn = 1L
                    , breaks = NULL
                    , nrows = 1000L
                    , read.table.args = NULL
                    )
{
    # Store intermediate binned files in this directory
    bindir = paste0(summary(infile)[["description"]], "_chunks")
    if(dir.exists(bindir))
        stop("Rename or remove the following directory to proceed: ", bindir)
    dir.create(bindir)

    bin_file_names = paste0(bindir, "/", c("", breaks), "_to_", c(breaks, ""))

    bin_files = lapply(bin_file_names, function(filename){
        f = file(filename)
        open(f, "wb")
        f
    })

    # Initialize before the loop
    nchunks = 0L
    moreinput = TRUE
    chunk = firstchunk

    while(moreinput){
        nchunks = nchunks + 1L
        writechunk(chunk, bin_file_names, bin_files, breaks, sortcolumn)

        tryCatch(chunk <- read.table(infile, nrows = nrows),
            error = function(e) moreinput <<- FALSE
        )
    }

    close(infile)
    lapply(bin_files, close)

    list(bin_file_names = bin_file_names
         , nchunks = nchunks
         , bindir = bindir
         )
}


#' Cut Into Bins
#' 
#' No boundaries on the endpoints, and handles character \code{x}.
#' A little different than normal \code{\link[base]{cut}}.
#' 
#' @param x column to be cut
#' @param breaks define the bins
#' @param bin_names names for the result
#' @return bins factor
cutbin = function(x, breaks, bin_names)
{
    bins = rep(1L, length(x))

    i = 2L
    for(b in breaks){
        bins[x > b] = i
        i = i + 1L
    }
    factor(bins, levels = seq_along(bin_names), labels = bin_names)
}


# Signals the last element
#SENTINEL = NULL
#test_sentinel = function(x) is.null(x)


#' Write Chunk Into Bins
#'
#' Intermediate step in disksort. 
#'
#' @param chunk \code{data.frame} to be binned
#' @param bin_files list of files opened in binary append mode
#' @param breaks defines the bins
#' @param sortcolumn column determining the bin
writechunk = function(chunk, bin_names, bin_files, breaks, sortcolumn
                     #, last = FALSE
                     )
{
    # if(last){
    #     lapply(bin_files, function(f) serialize(SENTINEL, f))
    #     return()
    # }

    bins = cutbin(chunk[, sortcolumn], breaks, bin_names)
    # SENTINEL value can be added in here later if it's needed.
    binned_chunk = split(chunk, bins)
    mapply(serialize, binned_chunk, bin_files)
}


#' Sort Bin And Write To Outfile
#'
#' Last step of disksort
#'
#' @param fname name of an intermediate file
#' @param sortcolumn See \code{\link{disksort}}
#' @param outfile See \code{\link{disksort}}. 
#' @param nchunks total number of chunks expected
sortbin = function(fname, sortcolumn, outfile, nchunks)
{
    f = file(fname)
    open(f, "rb")
    chunks = replicate(nchunks, unserialize(f), simplify = FALSE)
    unsorted = do.call(rbind, chunks)
    myorder = order(unsorted[, sortcolumn])
    sorted = unsorted[myorder, ]
    write.table(sorted, outfile, row.names = FALSE, col.names = FALSE)
    close(f)
}
