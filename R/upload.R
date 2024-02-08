#' Upload a schema for the CL-PFU database
#'
#' When you need to start over again,
#' you need to delete database tables and re-upload the schema.
#' This function makes it easy.
#'
#' Optionally, deletes existing tables in the database before uploading.
#'
#' `conn`'s user must have superuser privileges.
#'
#'
#' @param .dm A data model from the `dm` package to upload.
#' @param conn A `DBI` connection to a database.
#' @param drop_tables A boolean that tells whether to delete
#'                    existing tables before uploading the new schema.
#'
#' @return The remote data model
#'
#' @export
upload_schema <- function(.dm, conn, drop_tables = FALSE) {
  pl_destroy(conn, destroy_cache = FALSE, drop_tables = drop_tables)
  dm::copy_dm_to(conn, .dm, temporary = FALSE)
}
