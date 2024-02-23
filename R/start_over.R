#' Start over from the beginning
#'
#' Sometimes, you just need to begin again.
#'
#' This is a dangerous function.
#' It destroys the `targets` cache
#' and deletes all tables in the database.
#' User confirmation is required.
#'
#' @param drv The database driver.
#'            Default is `RPostgres::Postgres()`.
#' @param dbname The name of the database you want nuke.
#'               Default is "v2.0a1".
#' @param host The database host.
#'             Default is "eviz.cs.calvin.edu".
#' @param port The database port.
#'             Default is `5432`.
#' @param user The database user.
#'             Default is "postgres".
#' @param conn The database connection used internally.
#'             Default is constructed from the
#'             `drv`, `dbname`, `host`, `port`, and `user`
#'             arguments.
#'
#' @export
start_over <- function(drv = RPostgres::Postgres(),
                       dbname = "v2.0a1",
                       host = "eviz.cs.calvin.edu",
                       port = 5432,
                       user = "postgres",
                       conn = DBI::dbConnect(drv = drv,
                                             dbname = dbname,
                                             host = host,
                                             port = port,
                                             user = user)) {

  if (yesno::yesno(paste0("\nAre you sure you want to start over?\n\n**** ", dbname, " ****\n"))) {
    PFUPipelineTools::pl_destroy(conn, destroy_cache = TRUE, drop_tables = TRUE)
  }
}
