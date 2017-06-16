#' Sort File On Disk
#'
#' This function was designed to handle files larger than memory. At most
#' \code{nrows} will be present in memory at once.
#'
#' @param infile unsorted file to read from. See \code{\link[utils]{read.table}}.
#' @param outfile where to write the sorted file. See
#' \code{\link[utils]{write.table}}. If \code{infile} is the name of a file
#' then the default prepends "sorted_" to this name.
#' @param sortcolumn which column of the data frame to sort on
#' @param nrows number of rows in the data.frame held in memory
#' @param read.table.args named list of extra arguments to read.table
#' @param write.table.args named list of extra arguments to write.table.
#' Defaults to using read.table.args to preserve the original formatting.
#' @export
disksort = function(infile
                    , outfile = NULL
                    , sortcolumn = 1L
                    , nrows = 1000L
                    , nbins = 10L
                    , read.table.args = NULL
                    , write.table.args = NULL
                    ){

    if is.character(infile){
        if is.null(outfile)
            outfile = paste0("sorted_", infile)
        infile = file(infile)
    }

    if is.character(outfile)
        outfile = file(outfile)

    if !isOpen(infile)
        open(infile, "rt")

    if !isOpen(outfile)
        open(outfile, "at")

    # TODO: reorganize. Perhaps with initialize_disksort(...)
    # Saving this until I have a better idea of what the structure should
    # be.

    chunk = read.table(infile, nrows)

    # It would be more robust to sample from the whole file.
    # But it's not possible to seek on a more general connection.
    samp = sort(chunk[, sortcolumn])
    per_bin = round(nrows / nbins)
    breaks = samp[per_bin * (1:(nbins-1))]

    # Store intermediate binned files in this directory
    bindir = paste0(summary(infile)[["description"]], "_chunks")
    if dir.exists(bindir) stop()
    dir.create(bindir)

    bin_file_names = paste0(c("", breaks), "_to_", c(breaks, ""))

    bin_files = lapply(bin_file_names, function(filename){
        f = file(filename)
        open(f, "at")
        f
    })

    while(nrow(chunk) > 0){
        write_chunk(chunk, bin_files, breaks, sortcolumn)
        chunk = read.table(infile, nrows = nrows)
    }

    close(infile)
}


#' Place Chunk Into Bins
#'
#' Intermediate step in disksort
#'
#' @param chunk \code{data.frame} of data to be binned
#' @param bin_files list of files opened in append mode
#' @param breaks where to cut each 
#' @param sortcolumn determines which bin it falls into
bin_chunk = function(chunk, bin_files, breaks, sortcolumn)
{
}
