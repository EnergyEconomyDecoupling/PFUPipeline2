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
                             conn,
                             schema = PFUPipelineTools::schema_from_conn(conn),
                             fk_parent_tables = PFUPipelineTools::get_all_fk_tables(conn = conn, schema = schema)) {

  index_table <- index_table_name |>
    PFUPipelineTools::pl_filter_collect(collect = TRUE,
                                        conn = conn,
                                        schema = schema,
                                        fk_parent_tables = fk_parent_tables)

  # All row and column types use the same table
  list(Industry = index_table,
       Product = index_table,
       Unit = index_table,
       Other = index_table)
}


#' #' `tarchetypes` hook to download a target's dependency
#' #'
#' #' Many targets depend on previous targets that have been
#' #' stored in the CL-PFU database.
#' #' This function downloads the dependencies as part of the
#' #' tar_hook_inner process.
#' #'
#' #' @param .hashed_dependency A hashed data frame that is the "ticket"
#' #'                           for downloading the real data.
#' #'                           Usually the result of a prior target.
#' #' @param countries The countries that should be downloaded.
#' #'                  Default is `NULL`, meaning all countries will be downloaded.
#' #' @param years The years that should be downloaded.
#' #'              Default is `NULL`, meaning all years will be downloaded.
#' #' @param index_map The mapping between row and column indices and
#' #'                  row and column names.
#' #' @param rctypes A description of row and column types.
#' #' @param conn The database connection.
#' #' @param schema A `dm` object, the database schema.
#' #'               Default is `PFUPipelineTools::schema_from_conn(conn)`.
#' #'               Override the default if you want to avoid the overhead
#' #'               of reading the schema with every upsert.
#' #' @param fk_parent_tables The foreign key parent tables
#' #'                         for the database at `conn.`
#' #'                         Default is `PFUPipelineTools::get_all_fk_tables(conn = conn, schema = schema)`.
#' #'                         Override if you want to avoid the overhead
#' #'                         of readhing the schema with every upsert.
#' #' @param country The name of the Country column in the table.
#' #'                Default is `IEATools::iea_cols$country`.
#' #' @param dataset_colname The name of the dataset column.
#' #'                        Default is `PFUPipelineTools::dataset_info$dataset_colname`.
#' #' @param tar_group_colname The name of the targets group column name.
#' #'                          Default is "tar_group".
#' #'
#' #' @return The downloaded dependency described by the "ticket"
#' #'         supplied in `.hashed_dependency`.
#' #'
#' #' @export
#' download_dependency <- function(.hashed_dependency,
#'                                 countries = NULL,
#'                                 years = NULL,
#'                                 index_map,
#'                                 rctypes,
#'                                 conn,
#'                                 schema = PFUPipelineTools::schema_from_conn(conn),
#'                                 fk_parent_tables = PFUPipelineTools::get_all_fk_tables(conn = conn, schema = schema),
#'                                 country = IEATools::iea_cols$country,
#'                                 year = IEATools::iea_cols$year,
#'                                 dataset_colname = PFUPipelineTools::dataset_info$dataset_colname) {
#'
#'   to_download <- .hashed_dependency
#'   if (!is.null(countries)) {
#'     to_download <- to_download |>
#'       dplyr::filter(.data[[country]] %in% countries)
#'   }
#'   if (!is.null(years)) {
#'     to_download <- to_download |>
#'       dplyr::filter(.data[[year]] %in% years)
#'   }
#'
#'   if (nrow(to_download) == 0) {
#'     return(NULL)
#'   }
#'
#'   to_download |>
#'     PFUPipelineTools::pl_collect_from_hash(set_tar_group = TRUE,
#'                                            index_map = index_map,
#'                                            rctypes = rctypes,
#'                                            conn = conn,
#'                                            schema = schema,
#'                                            fk_parent_tables = fk_parent_tables) |>
#'     dplyr::mutate(
#'       "{dataset_colname}" := NULL
#'     )
#' }
