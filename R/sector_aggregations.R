#' Compile the list of final demand sectors
#'
#' The `IEATools` package contains a list of final demand sectors
#' in `IEATools::fd_sectors`.
#' However, we aggregate and rename sectors according to an aggregation map.
#' The names of the aggregation map items should also count as
#' final demand sectors.
#' This function adds the aggregated final demand sector names
#' to the `IEATools` final demand sectors.
#'
#' @param iea_fd_sectors The list of final demand sectors, according to the IEA.
#' @param sector_aggregation_map The aggregation map used to aggregate final demand sectors.
#'
#' @return A larger list of final demand sectors comprised of
#'         the items in `iea_fd_sectors` and the names of `sector_aggregation_map`.
#'
#' @export
create_fd_sectors_list <- function(iea_fd_sectors, sector_aggregation_map) {
  append(iea_fd_sectors, names(sector_aggregation_map))
}
