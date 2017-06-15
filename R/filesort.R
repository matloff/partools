# Generate corresponding .Rd file
# roxygen2::rd_roclet


#' Sort File On Disk
#'
#' Sorts 
#'
#' @param infile unsorted file to read from. See \code{\link[utils]{read.table}}.
#' @param outfile where to write the sorted file. See
#' \code{\link[utils]{write.table}}. Default prepends "sorted_" to
#' \code{infile}
#' @param sortcolumn which column of the data frame to sort on
#' @param nrows number of rows in the data.frame held in memory
#' @param read.table.args named list of extra arguments to read.table
#' @param write.table.args named list of extra arguments to write.table.
#' Defaults to using read.table args
#' @export
disksort = function(infile
                    , outfile = NULL
                    , sortcolumn = 1L
                    , nrows = NULL
                    , read.table.args = NULL
                    , write.table.args = NULL
                    )
{
}
