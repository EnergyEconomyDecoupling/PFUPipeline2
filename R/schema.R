#' Create and upload a data model from schema file
#'
#' The file at `schema_file_path` is read and uploaded
#' to the database at `conn`.
#' The simple foreign key tables are read from `schema_file_path`
#' and uploaded to the database at `conn`.
#' Both the data model and foreign tables are returned in a list
#' for further extraction into separate targets.
#'
#' @param schema_file_path The path to the schema file.
#' @param conn The connection to the database.
#'
#' @return A list containing two items,
#'         the data model (a `dm` object named "dm") and
#'         the simple foreign key tables
#'         (a list of `tibble`s named "fk_tables",
#'         each `tibble` in the list is named
#'         by its table name in the database).
#'
#' @export
set_dm_fk_tables <- function(schema_file_path, conn) {

  # Read the schema table
  schema_table <- PFUPipelineTools::load_schema_table(schema_path = schema_file_path)
  # Read the simple foreign key tables
  fk_tables <- PFUPipelineTools::load_fk_tables(simple_tables_path = schema_file_path)
  # Create the data model
  this_data_model <- schema_table |>
    PFUPipelineTools::schema_dm()
  PFUPipelineTools::pl_upload_schema_and_simple_tables(schema = this_data_model,
                                                       simple_tables = fk_tables,
                                                       conn = conn,
                                                       drop_db_tables = TRUE)
  return(list(dm = this_data_model,
              fk_tables = fk_tables))
}


#' Create the index map for row and column names
#'
#' The tables given by `*_table_name`
#' are combined into an appropriate list for the pipeline.
#'
#' @param index_table_name Name of the Index row/col table in the database.
#'                         Default is "Index".
#' @param conn The database connection.
#' @param schema The data model (`dm` object) for the database in `conn`.
#'               See details.
#' @param fk_parent_tables A named list of all parent tables
#'                         for the foreign keys in `db_table_name`.
#'                         See details.
#'
#' @return A named list appropriate to be an
#'         index map for this database.
#' @export
create_index_map <- function(index_table_name = "Index",
                             rc_type_table_name = "RCType",
                             rc_type_colname = rc_type_table_name,
                             conn,
                             schema = PFUPipelineTools::schema_from_conn(conn),
                             fk_parent_tables = PFUPipelineTools::get_all_fk_tables(conn = conn, schema = schema)) {

  index_table <- index_table_name |>
    PFUPipelineTools::pl_filter_collect(collect = TRUE,
                                        conn = conn,
                                        schema = schema,
                                        fk_parent_tables = fk_parent_tables)

  rc_type_table <- rc_type_table_name |>
    PFUPipelineTools::pl_filter_collect(collect = TRUE,
                                        conn = conn,
                                        schema = schema,
                                        fk_parent_tables = fk_parent_tables)

  # Get the names of all row and column types from the RCType column
  rc_type_names <- rc_type_table[[rc_type_colname]]

  # Create the outgoing list, with each row and column getting the same index table,
  # for simplicity
  index_table |>
    RCLabels::make_list(n = length(rc_type_names), lenx = 1) |>
    magrittr::set_names(rc_type_names)
}
