#' Reset a database pipeline to original condition
#'
#' Sometimes, you just need to start over from scratch.
#' This function destroys the local `targets` cache
#' and remove all tables in the database at `conn`.
#' This is a very destructive operation, so
#' be sure you know what you're doing!
#'
#' @return If successful, `TRUE`, otherwise `FALSE`.
#'
#' @export
pl_destroy <- function(conn = DBI::dbConnect(drv = RPostgres::Postgres(),
                                             dbname = conn_args$dbname,
                                             host = conn_args$host,
                                             port = conn_args$port,
                                             user = conn_args$user),
                       conn_params) {

  # Destroy the local targets cache
  targets::tar_destroy(ask = FALSE)

  # Remove all tables in the database
  DBI::dbListTables(conn) |>
    purrr::map(function(this_table) {
      DBI::dbRemoveTable(conn, this_table)
    })

  return(TRUE)
}
