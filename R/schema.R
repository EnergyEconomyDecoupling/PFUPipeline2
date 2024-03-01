#' Create and upload a data model from schema file
#'
#' The file at `schema_file_path` is read and uploaded
#' to the database at `conn`.
#' The simple foreign key tables are read from `schema_file_path`
#' and uploaded to the database at `conn`.
#' Both the data model and foreign tables are returned in a list
#' for futher extraction into separate targets.
#'
#' @param schema_file_path The path to the schema file.
#' @param conn The connection to the database.
#'
#' @return A list containing two items,
#'         the data model (a `dm` object named "dm") and
#'         the simple foreign key tables
#'         (a list of `tibble`s named "simple_fk_tables",
#'         each `tibble` in the list is named
#'         by its table name in the database).
#'
#' @export
set_dm_fk_tables <- function(schema_file_path, conn) {
  # Read the schema table
  schema_table <- schema_file_path |>
    PFUPipelineTools::load_schema_table()
  # Read the simple foreign key tables
  simple_fk_tables <- schema_file_path |>
    PFUPipelineTools::load_fk_tables()
  # Create the data model
  this_data_model <- schema_table |>
    PFUPipelineTools::schema_dm()
  PFUPipelineTools::pl_upload_schema_and_simple_tables(schema = this_data_model,
                                                       simple_tables = simple_fk_tables,
                                                       conn = conn,
                                                       drop_db_tables = TRUE)
  return(list(dm = this_data_model,
              simple_fk_tables = simple_fk_tables))
}
