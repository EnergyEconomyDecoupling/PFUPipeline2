#' Reset the CL-PFU database pipeline to original condition
#'
#' Sometimes, you just need to start over from scratch.
#' This function destroys the local `targets` cache
#' and removes all tables in the database at `conn`.
#' Be sure you know what you're doing!
#'
#' `conn`'s user must have superuser privileges.
#'
#' Because this is a very destructive function,
#' the caller must opt into behaviors.
#'
#' If `drop_tables` is `TRUE`,  `destroy_cache` is implied to be `TRUE`,
#' and the targets cache is destroyed.
#'
#' @param conn A `DBI` connection to a database.
#' @param store The path to the `targets` store.
#'              Default is `targets::tar_config_get("store")`,
#'              which is normally `_targets`.
#' @param destroy_cache A boolean that tells whether to destroy the local `targets` cache.
#'                      Default is `FALSE`.
#' @param drop_tables A boolean that tells whether to delete all tables in `conn`.
#'                    Default is `FALSE`.
#'
#' @return If successful, `TRUE` (invisibly);
#'         otherwise, an error is thrown.
#'
#' @export
pl_destroy <- function(conn,
                       store = targets::tar_config_get("store"),
                       destroy_cache = FALSE,
                       drop_tables = FALSE) {

  if (destroy_cache | drop_tables) {
    # Destroy the local targets cache
    targets::tar_destroy(verbose = FALSE, ask = FALSE, store = store)
  }

  if (drop_tables) {
    # Remove all tables in the database
    DBI::dbListTables(conn) |>
      purrr::map(function(this_table_name) {
        DBI::dbExecute(conn, paste0('DROP TABLE "', this_table_name, '" CASCADE;'))
      })
  }
  return(invisible(TRUE))
}
