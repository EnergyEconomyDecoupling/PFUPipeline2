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
#' @param version The version to be downloaded from the database.
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
                                     version = NULL,
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

  if (is.null(.hashed_dependency)) {
    # Likely no data for this country.
    # Just return NULL.
    return(NULL)
  }
  if (nrow(.hashed_dependency) == 0) {
    return(NULL)
  }
  if (!is.null(countries) & (country %in% colnames(.hashed_dependency))) {
    .hashed_dependency <- .hashed_dependency |>
      dplyr::filter(.data[[country]] %in% countries)
  }
  if (!is.null(years) & (year %in% colnames(.hashed_dependency))) {
    .hashed_dependency <- .hashed_dependency |>
      dplyr::filter(.data[[year]] %in% years)
  }
  out <- .hashed_dependency |>
    PFUPipelineTools::pl_collect_from_hash(version_string = version,
                                           index_map = index_map,
                                           rctypes = rctypes,
                                           conn = conn,
                                           schema = schema,
                                           fk_parent_tables = fk_parent_tables)

  if (is.null(out)) {
    # No country data
    return(NULL)
  }
  return(out)
}


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
#'                Default is `NULL`, meaning that no `dataset_colname` column will be added.
#'                In this case, the target assumes responsibility for
#'                managing `dataset_colname`.
#' @param compress_data A boolean that tells whether to compress data
#'                      across versions upon upserting to a table.
#'                      Default is `TRUE`.
#' @param version_string The name of the version for which these data are valid.
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
#' @param dataset_colname,valid_from_version_colname,valid_to_version_colname See `PFUPipelineTools::dataset_info`.
#' @param value_colname The name of the value column in `db_table_name`.
#'                      Default is [PFUPipelineTools::mat_colnames]`$value` or
#'                      "`r PFUPipelineTools::mat_colnames$value`".
#'
#' @return A hashed data frame that serves as the "ticket"
#'         with which the full data frame can be retrieved at a later time.
#'
#' @export
upsert_hook <- function(.df,
                        db_table_name,
                        dataset = NULL,
                        compress_data = TRUE,
                        version_string,
                        index_map,
                        retain_zero_structure = TRUE,
                        conn,
                        schema = PFUPipelineTools::schema_from_conn(conn),
                        fk_parent_tables = PFUPipelineTools::get_all_fk_tables(conn = conn, schema = schema),
                        dataset_colname = PFUPipelineTools::dataset_info$dataset_colname,
                        value_colname = PFUPipelineTools::mat_colnames$value,
                        valid_from_version_colname = PFUPipelineTools::dataset_info$valid_from_version_colname,
                        valid_to_version_colname = PFUPipelineTools::dataset_info$valid_to_version_colname) {

  if (is.null(.df)) {
    return(NULL)
  }

  to_upload <- .df

  if (!is.null(dataset)) {
    to_upload <- to_upload |>
      dplyr::mutate(
        # Add dataset column
        "{dataset_colname}" := dataset
      )
  }

  to_upload |>
    dplyr::mutate(
      # Add version columns.
      # The single incoming version is applied to both
      # valid_from and valid_to columns.
      # Later (with a server-side command),
      # identical rows will be deleted and the valid_to_version
      # entry will be increased.
      "{valid_from_version_colname}" := version_string,
      "{valid_to_version_colname}" := version_string
    ) |>
    dplyr::relocate(dplyr::all_of(dataset_colname)) |>
    # Upload to the database and return the "ticket"
    # PFUPipelineTools::pl_upsert(in_place = TRUE,
    PFUPipelineTools::pl_upsert_and_compress(
      in_place = TRUE,
      db_table_name = db_table_name,
      index_map = index_map,
      value_colname = value_colname,
      # Don't keep single unique columns,
      # because groups may have different columns
      # with single unique values.
      keep_single_unique_cols = FALSE,
      # Set retain_zero_structure TRUE
      # to preserve U_EIOU, r_EIOU, and
      # other matrices when they're otherwise
      # absent or all zeroes.
      retain_zero_structure = TRUE,
      # Should we compress the table across versions?
      compress = compress_data,
      # Round any double columns to 14 digits to
      # assist with comparisons before compressing.
      round_double_columns = TRUE,
      digits = 14,
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


#' Get the name of the value column from the table name
#'
#' When uploading tables to the database,
#' we need to know the name of the value column.
#' This function bundles, in one place,
#' the logic for deciding the value column name.
#' For most tables, the value column name will be "value".
#' But some require special logic.
#'
#' @param table_name The name of the table being uploaded.
#' @param default_value_colname The default name of the value column.
#'                              Default is "value".
#'
#' @returns The name of the value column in `table_name`.
#'
#' @export
value_colname_hook <- function(table_name,
                               default_value_colname = PFUPipelineTools::mat_colnames$value) {
  if (table_name == "PhiConstants") {
    value_colname = "phi"
  } else if (table_name %in% c("AllMachineData",
                               "IncompleteAllocationTables", "CompletedAllocationTables",
                               "IncompleteEfficiencyTables", "CompletedEfficiencyTables",
                               "CompletedPhiuTables")) {
    # Not capitalization difference
    value_colname <- "Value"
  } else if (table_name %in% c("AMWPFUDataRaw", "AMWPFUData", "HMWPFUDataRaw", "HMWPFUData",
                               "AllIEAData", "IEAData", "BalancedIEAData", "SpecifiedIEAData")) {
    value_colname <- IEATools::iea_cols$e_dot
  } else if (table_name == "AggEtaPFU") {
    value_colname <- c(Recca::aggregate_cols$aggregate_primary,
                       Recca::aggregate_cols$aggregate_final,
                       Recca::aggregate_cols$aggregate_useful,
                       Recca::efficiency_cols$eta_pf,
                       Recca::efficiency_cols$eta_fu,
                       Recca::efficiency_cols$eta_pu)
  } else if (table_name == "SectorAggEtaFU") {
    value_colname <- c(Recca::all_stages$final,
                       Recca::all_stages$useful,
                       Recca::efficiency_cols$eta_fu)
  } else {
    value_colname = default_value_colname
  }
  return(value_colname)
}
