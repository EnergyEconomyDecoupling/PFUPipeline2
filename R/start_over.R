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
#' @param host The database host.
#'             Default is "eviz.cs.calvin.edu".
#' @param port The database port.
#'             Default is `6432`.
#' @param user The database user.
#'             Default is "dbcreator".
#' @param conn The database connection used internally.
#'             Default is constructed from the
#'             `drv`, `dbname`, `host`, `port`, and `user`
#'             arguments.
#'
#' @export
start_over <- function(drv = RPostgres::Postgres(),
                       dbname = "ScratchMDB",
                       host = "mexer.site",
                       port = 6432,
                       user = "dbcreator",
                       conn = DBI::dbConnect(drv = drv,
                                             dbname = dbname,
                                             host = host,
                                             port = port,
                                             user = user),
                       drop_tables = FALSE) {

  if (dbname == "MexerDB") {
    stop("You cannot start_over() with MexerDB")
  }

  if (yesno::yesno(paste0("\nAre you sure you want to start over?\n\n**** ",
                          dbname,
                          " ****\n"))) {
    PFUPipelineTools::pl_destroy(conn, destroy_cache = TRUE, drop_tables = drop_tables)
  }
}
