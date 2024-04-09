#' `tarchetypes` hook to download a target's dependency
#'
#' Many targets depend on previous targets that have been
#' stored in the CL-PFU database.
#' This function downloads the dependencies as part of the
#' tar_hook_inner process.
#'
#' If `.hashed_dependency` contains the `tar_group_colname`,
#' the `countries` argument is ignored, and
#' no filtering of `.hashed_dependency` is performed;
#' it is assumed that filtering has already been
#' accomplished by the target via `map(Countries)`
#' or something similar.
#'
#' @param .hashed_dependency A hashed data frame that is the "ticket"
#'                           for downloading the real data.
#'                           Usually the result of a prior target.
#' @param countries The countries that should be downloaded.
#' @param index_map The mapping between row and column indices and
#'                  row and column names.
#' @param rctypes A description of row and column types.
#' @param conn The database connection.
#' @param schema A `dm` object, the database schema.
#'               Default is `PFUPipelineTools::schema_from_conn(conn)`.
#'               Override the default if you want to avoid the overhead
#'               of reading the schema with every upsert.
#' @param fk_parent_tables The foreign key parent tables
#'                         for the database at `conn.`
#'                         Default is `PFUPipelineTools::get_all_fk_tables(conn = conn, schema = schema)`.
#'                         Override if you want to avoid the overhead
#'                         of readhing the schema with every upsert.
#' @param country The name of the Country column in the table.
#'                Default is `IEATools::iea_cols$country`.
#' @param dataset_colname The name of the dataset column.
#'                        Default is `PFUPipelineTools::dataset_info$dataset_colname`.
#' @param tar_group_colname The name of the targets group column name.
#'                          Default is "tar_group".
#'
#' @return The downloaded dependency described by the "ticket"
#'         supplied in `.hashed_dependency`.
#'
#' @export
download_dependency_hook <- function(.hashed_dependency,
                                     countries = NULL,
                                     years = NULL,
                                     index_map,
                                     rctypes,
                                     conn,
                                     schema = PFUPipelineTools::schema_from_conn(conn),
                                     fk_parent_tables = PFUPipelineTools::get_all_fk_tables(conn = conn, schema = schema),
                                     country = IEATools::iea_cols$country,
                                     year = IEATools::iea_cols$year,
                                     dataset_colname = PFUPipelineTools::dataset_info$dataset_colname,
                                     tar_group_colname = "tar_group") {

  if (!is.null(countries) & (country %in% colnames(.hashed_dependency))) {
    .hashed_dependency <- .hashed_dependency |>
      dplyr::filter(.data[[country]] %in% countries)
  }
  if (!is.null(years) & (year %in% colnames(.hashed_dependency))) {
    .hashed_dependency <- .hashed_dependency |>
      dplyr::filter(.data[[year]] %in% years)
  }
  if (nrow(.hashed_dependency) == 0) {
    return(NULL)
  }
  .hashed_dependency |>
  PFUPipelineTools::pl_collect_from_hash(set_tar_group = TRUE,
                                         index_map = index_map,
                                         rctypes = rctypes,
                                         conn = conn,
                                         schema = schema,
                                         fk_parent_tables = fk_parent_tables) |>
  dplyr::mutate(
    "{dataset_colname}" := NULL
  )
}


#'
#' After most targets, the resulting data frame should be
#' uploaded to the database for storage.
#' This function provides a `tarchetypes` "hook" to wrap each target
#' for that purpose.
#'
#' By default, [PFUPipelineTools::pl_upsert()] will delete all zero entries
#' in matrices before upserting.
#' But for some countries and years,
#' that could result in missing matrices, such as **U_EIOU**.
#' Set `retain_zero_structure = TRUE`
#' to do otherwise and preserve all entries in a zero matrix.
#'
#' @param .df A data frame to be upserted into table `db_table_name`
#'            in the database at `conn`.
#' @param db_table_name The name of the table into which `.df` will be upserted.
#' @param dataset The name of the dataset to which these data belong.
#' @param index_map The mapping for matrix row and column indices,
#'                  a two-column data frame with an integer column
#'                  for indices and a string column for names.
#' @param retain_zero_structure A boolean that tells whether to retain the stucture
#'                              of zero matrices.
#'                              See details.
#' @param conn The database connection.
#' @param schema A `dm` object, the database schema.
#'               Default is `PFUPipelineTools::schema_from_conn(conn)`.
#'               Override the default if you want to avoid the overhead
#'               of reading the schema with every upsert.
#' @param fk_parent_tables The foreign key parent tables
#'                         for the database at `conn.`
#'                         Default is `PFUPipelineTools::get_all_fk_tables(conn = conn, schema = schema)`.
#'                         Override if you want to avoid the overhead
#'                         of readhing the schema with every upsert.
#' @param dataset_colname See `PFUPipelineTools::dataset_info`.
#'
#' @return A hashed data frame that serves as the "ticket"
#'         with which the full data frame can be retrieved at a later time.
#'
#' @export
upsert_hook <- function(.df,
                        db_table_name,
                        dataset,
                        index_map,
                        retain_zero_structure = TRUE,
                        conn,
                        schema = PFUPipelineTools::schema_from_conn(conn),
                        fk_parent_tables = PFUPipelineTools::get_all_fk_tables(conn = conn, schema = schema),
                        dataset_colname = PFUPipelineTools::dataset_info$dataset_colname) {

  if (is.null(.df)) {
    return(NULL)
  }

  .df |>
    # Add dataset column
    dplyr::mutate(
      "{dataset_colname}" := dataset
    ) |>
    dplyr::relocate(dplyr::all_of(dataset_colname)) |>
    # Upload to the database and return the "ticket"
    PFUPipelineTools::pl_upsert(in_place = TRUE,
                                db_table_name = db_table_name,
                                index_map = index_map,
                                # Don't keep single unique columns,
                                # because groups may have different columns
                                # with single unique values.
                                keep_single_unique_cols = FALSE,
                                conn = conn,
                                schema = schema,
                                fk_parent_tables = fk_parent_tables)
}


#' Extract a table name from a target name
#'
#' The convention in the CL-PFU pipeline is that
#' targets are named for the database table that they populate.
#' This function extracts a table name from a target name,
#' aware that target names could be suffixed with an "_"
#' followed by a hash.
#'
#' @param target_name The name of the target, usually obtained by
#'                    `targets::tar_name()`.
#'
#' @return The database table associated with this target.
#'
#' @export
db_table_name_hook <- function(target_name) {
    strsplit(target_name, "_")[[1]][[1]]
}
