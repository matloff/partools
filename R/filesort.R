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

    chunk = read.table(infile, nrows)

    while(nrow(chunk) > 0){
        write.table(chunk, outfile)
        chunk = read.table(infile)
    }

    close(infile)
    close(outfile)
}
