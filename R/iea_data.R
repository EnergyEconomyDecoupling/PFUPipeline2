#' Load a tidy IEA data frame
#'
#' Loads IEA data from `iea_data_path` and pre-processes
#' to make a data frame.
#'
#' `iea_data_path` can point to an individual file or a folder
#' of IEA data files, possibly one for each country.
#' If `iea_data_path` points to a folder,
#' files in the folder are read according to `countries` and
#' `base_iea_country_filename` with the following code:
#' `paste0(base_iea_country_file_name, " ", countries, ".csv")`.
#' The resulting data frames are stacked via
#' [dplyr::bind_rows()].
#'
#' @param iea_data_path The path to an IEA data file or folder. See details.
#' @param countries The countries whose data is to be loaded.
#'                  A vector of 3- (or 4-) letter country codes.
#'                  Only necessary if `iea_data_path` is a folder.
#' @param base_iea_country_filename The base file name for IEA country files.
#'                                  Only necessary if `iea_data_path` is a folder.
#' @param override_df A country concordance data frame.
#' @param dataset The dataset name.
#' @param specify_non_energy_flows A boolean that tells whether to specify non-energy flows.
#'                                 Default is `TRUE`.
#' @param apply_fixes A boolean that tells whether to apply fixes to the IEA data.
#'                    Default is `TRUE`.
#' @param iea_countries The countries to be returned in the outgoing data frame.
#'                      Default is `c(PFUPipelineTools::canonical_countries, wrld = "WRLD") |> unlist()`.
#' @param country The name of the country column in the outgoing data frame.
#'                Default is `IEATools::iea_cols$country`.
#'
#' @return A tidy data frame of IEA extended world energy balance data.
#'
#' @export
load_iea_data <- function(iea_data_path,
                          countries,
                          base_iea_country_filename,
                          override_df,
                          specify_non_energy_flows = TRUE,
                          apply_fixes = TRUE,
                          iea_countries = c(PFUPipelineTools::canonical_countries, wrld = "WRLD") |> unlist(),
                          country = IEATools::iea_cols$country) {

  assertthat::assert_that(file.exists(iea_data_path))
  is_dir <- file.info(iea_data_path)[["isdir"]]
  if (is_dir) {
    # Form file names
    iea_data_path <- file.path(iea_data_path, paste0(base_iea_country_filename, " ", countries, ".csv")) |>
      sapply(FUN = function(this_file) {
        if (file.exists(this_file)) {
          return(this_file)
        }
        return(NULL)
      }) |>
      purrr::keep(function(this_path) {
        !is.null(this_path)
      })
  }
  iea_data_path |>
    IEATools::load_tidy_iea_df(override_df = override_df,
                               specify_non_energy_flows = specify_non_energy_flows,
                               apply_fixes = apply_fixes) |>
    dplyr::filter(.data[[country]] %in% iea_countries)
}


#' Tells whether IEA data are balanced
#'
#' Performs the the energy balance check in a way that is amenable to drake subtargets.
#' Internally, this function uses `IEATools::calc_tidy_iea_df_balances()`.
#' Grouping is doing internal to this function using the value of `grp_vars`.
#'
#' `schema` is a data model (`dm` object) for the CL-PFU database.
#' It can be obtained from calling `schema_from_conn()`.
#' The default is `schema_from_conn(conn = conn)`,
#' which downloads the `dm` object from `conn`.
#' To save time, pre-compute the `dm` object and
#' supply in the `schema` argument.
#'
#' `fk_parent_tables` is a named list of tables,
#' some of which are foreign key (fk) parent tables for `db_table_name`
#' containing the mapping between fk values (usually strings)
#' and fk keys (usually integers).
#' `fk_parent_tables` is treated as a store from which foreign key tables
#' are retrieved by name when needed.
#' An appropriate value for `fk_parent_tables` can be obtained
#' from `get_all_fk_tables()`.
#'
#' @param .iea_data A tidy IEA data frame
#' @param grp_vars The groups that should be checked. Default is
#'                 `c(country, IEATools::iea_cols$method, IEATools::iea_cols$energy_type, IEATools::iea_cols$last_stage, IEATools::iea_cols$product)`.
#' @param conn The database connection.
#' @param schema The data model (`dm` object) for the database in `conn`.
#'               See details.
#' @param fk_parent_tables A named list of all parent tables
#'                         for the foreign keys in `db_table_name`.
#'                         See details.
#'
#' @return a logical stating whether all products are balanced for the country of interest
#'
#' @export
is_balanced <- function(.iea_data,
                        grp_vars = c(IEATools::iea_cols$country,
                                     IEATools::iea_cols$method,
                                     IEATools::iea_cols$energy_type,
                                     IEATools::iea_cols$last_stage,
                                     IEATools::iea_cols$year,
                                     IEATools::iea_cols$product) #,
                        # conn,
                        # schema = schema_from_conn(conn),
                        # fk_parent_tables = get_all_fk_tables(conn = conn, schema = schema)
                        ) {

  .iea_data |>
    # PFUPipelineTools::pl_collect_from_hash(conn = conn,
    #                                        schema = schema,
    #                                        fk_parent_tables = fk_parent_tables) |>
    # Get data from the database
    dplyr::group_by(!!as.name(grp_vars)) |>
    # Check balances
    IEATools::calc_tidy_iea_df_balances() |>
    IEATools::tidy_iea_df_balanced()
}


#' Combine countries and additional exemplars
#'
#' @param couns Countries of interest
#' @param exempls Additional exemplars
#'
#' @return A unique combination of countries and additional exemplars
#'
#' @export
combine_countries_exemplars <- function(couns, exempls) {
  c(couns, exempls) %>%
    unique()
}


#' Balance IEA data
#'
#' Balances the IEA data in a way that is amenable to drake subtargets.
#' Internally, this function uses `IEATools::fix_tidy_iea_df_balances()`.
#' Grouping is done internal to this function using the value of `grp_vars`.
#'
#' `schema` is a data model (`dm` object) for the CL-PFU database.
#' It can be obtained from calling `schema_from_conn()`.
#' The default is `schema_from_conn(conn = conn)`,
#' which downloads the `dm` object from `conn`.
#' To save time, pre-compute the `dm` object and
#' supply in the `schema` argument.
#'
#' `fk_parent_tables` is a named list of tables,
#' some of which are foreign key (fk) parent tables for `db_table_name`
#' containing the mapping between fk values (usually strings)
#' and fk keys (usually integers).
#' `fk_parent_tables` is treated as a store from which foreign key tables
#' are retrieved by name when needed.
#' An appropriate value for `fk_parent_tables` can be obtained
#' from `get_all_fk_tables()`.
#'
#' @param .iea_data A tidy IEA data frame.
#' @param db_table_name The name of the table in `conn` where the result will be stored.
#' @param max_fix The maximum allowable energy imbalance to fix.
#'                Default is `3`.
#' @param balanced_table_name The name of the table in `conn` where
#'                            balanced IEA data should be uploaded.
#' @param grp_vars the groups that should be checked.
#'                 Default is
#'                 `c(IEATools::iea_cols$country, IEATools::iea_cols$method, IEATools::iea_cols$energy_type, IEATools::iea_cols$last_stage, IEATools::iea_cols$product)`.
#'
#' @return A hash of the balanced data frame. See `pl_upsert()`.
#'
#' @export
make_balanced <- function(.iea_data,
                          db_table_name,
                          max_fix = 6,
                          grp_vars = c(IEATools::iea_cols$country,
                                       IEATools::iea_cols$method,
                                       IEATools::iea_cols$energy_type,
                                       IEATools::iea_cols$last_stage,
                                       IEATools::iea_cols$year,
                                       IEATools::iea_cols$product) #,
                          # conn,
                          # schema = schema_from_conn(conn),
                          # fk_parent_tables = get_all_fk_tables(conn = conn, schema = schema)
                          ) {

  .iea_data |>
    dplyr::group_by(!!as.name(grp_vars)) |>
    IEATools::fix_tidy_iea_df_balances(max_fix = max_fix) |>
    dplyr::ungroup()
}


#' Specify the IEA data
#'
#' Specifies the IEA data in a way that is amenable to targets parallelization
#' See `IEATools::specify_all()` for details.
#'
#' @param BalancedIEAData IEA data that have already been balanced.
#' @param db_table_name The name of the specified IEA data table in `conn`.
#'                      Default is "SpecifiedIEAData".
#' @param conn The database connection.
#' @param schema The data model (`dm` object) for the database in `conn`.
#'               See details.
#' @param fk_parent_tables A named list of all parent tables
#'                         for the foreign keys in `db_table_name`.
#'                         See details.
#'
#' @return A data frame of specified IEA data.
#'
#' @export
specify <- function(BalancedIEAData,
                    db_table_name,
                    conn,
                    schema = PFUPipelineTools::schema_from_conn(conn),
                    fk_parent_tables = PFUPipelineTools::get_all_fk_tables(conn = conn, schema = schema)) {
  BalancedIEAData |>
    PFUPipelineTools::pl_collect_from_hash(conn = conn,
                                           schema = schema,
                                           fk_parent_tables = fk_parent_tables) |>
    IEATools::specify_all() |>
    PFUPipelineTools::pl_upsert(in_place = TRUE,
                                db_table_name = db_table_name,
                                conn = conn,
                                schema = schema,
                                fk_parent_tables = fk_parent_tables)
}


#' Filter by country and year; copy to a destination table
#'
#' It is helpful to do an inboard copy and filter of the IEA data.
#' This function filters `source_table` and copies
#' to `dest_table` (after first removing all rows from `dest_table`).
#'
#' @param source_table A string identifying the source table.
#' @param dest_table A string identifying the destination table.
#' @param countries The countries to keep.
#' @param years The years to keep.
#' @param conn The database connection.
#' @param schema A `dm` object for the database.
#' @param country The name of the country column.
#'                Default is `IEATools::iea_cols$country`.
#' @param year The name of the year column.
#'             Default is `IEATools::iea_cols$year`.
#'
#' @return `TRUE` if successful.
filter_all_iea_data <- function(source_table,
                                dest_table,
                                countries,
                                years,
                                conn,
                                schema = schema_from_conn(conn),
                                country = IEATools::iea_cols$country,
                                year = IEATools::iea_cols$year,
                                pk_col = PFUPipelineTools::dm_pk_colnames$pk_col) {

  by_cols <- schema |>
    dm::dm_get_all_pks(table = {{dest_table}}) |>
    magrittr::extract2(pk_col) |>
    unlist()

  dest_tbl <- dplyr::tbl(conn, dest_table) |>
    # Clean out all rows from dest_tbl
    dplyr::filter(FALSE)
  source_tbl <- dplyr::tbl(conn, source_table) |>
    # Filter the source table
    dplyr::filter(.data[[country]] %in% countries, .data[[year]] %in% years)

  dplyr::rows_insert(x = dest_tbl,
                     y = source_tbl,
                     by = by_cols,
                     in_place = FALSE,
                     conflict = "ignore")
  return(TRUE)
}


#' Convert to PSUT matrices
#'
#' Converts tidy IEA data to PSUT matrices in a way that is amenable to drake subtargets.
#' Internally, `IEATools::prep_psut()` does the conversion to matrices.
#'
#' @param specified_iea_data A data frame that has already been specified via `specify()`.
#' @param countries The countries you want to convert to PSUT matrices.
#' @param dataset The name of the dataset to which these data belong.
#' @param db_table_name The name of the specified IEA data table in `conn`.
#' @param index_map A list of data frames to assist with decoding matrices.
#'                  Passed to [decode_matsindf()] when `decode_matsindf` is `TRUE`
#'                  but otherwise not needed.
#' @param rctypes A data frame of row and column types.
#'                Passed to [decode_matsindf()] when `decode_matsindf` is `TRUE`
#'                but otherwise not needed.
#' @param conn The database connection.
#' @param schema The data model (`dm` object) for the database in `conn`.
#'               See details.
#' @param fk_parent_tables A named list of all parent tables
#'                         for the foreign keys in `db_table_name`.
#'                         See details.
#' @param matrix_class The type of matrix to be created.
#'                     Default is "Matrix" for sparse matrices.
#' @param country See `IEATools::iea_cols`.
#' @param dataset_colname See `PFUPipelineTools::dataset_info`.
#'
#' @return A `matsindf`-style data frame.
#'
#' @export
#'
#' @examples
#' IEATools::sample_iea_data_path() %>%
#'   IEATools::load_tidy_iea_df() %>%
#'   make_balanced(countries = c("GHA", "ZAF")) %>%
#'   specify(countries = c("GHA", "ZAF")) %>%
#'   make_iea_psut(countries = c("GHA", "ZAF"))
make_iea_psut <- function(specified_iea_data,
                          countries,
                          dataset,
                          db_table_name,
                          index_map,
                          rctypes,
                          conn,
                          schema = PFUPipelineTools::schema_from_conn(conn),
                          fk_parent_tables = PFUPipelineTools::get_all_fk_tables(conn = conn, schema = schema),
                          matrix_class = "Matrix",
                          country = IEATools::iea_cols$country,
                          dataset_colname = PFUPipelineTools::dataset_info$dataset_colname) {

  specified_iea_data |>
    dplyr::filter(.data[[country]] %in% countries) |>
    PFUPipelineTools::pl_collect_from_hash(index_map = index_map,
                                           rctypes = rctypes,
                                           set_tar_group = FALSE,
                                           conn = conn,
                                           schema = schema,
                                           fk_parent_tables = fk_parent_tables) |>
    dplyr::mutate(
      # Eliminate the dataset column (which contains IEAEWEBYYYY)
      "{dataset_colname}" := NULL
    ) |>
    IEATools::prep_psut(matrix_class = matrix_class) |>
    dplyr::mutate(
      "{dataset_colname}" := dataset
    ) |>
    dplyr::relocate(dplyr::all_of(dataset_colname)) |>
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

