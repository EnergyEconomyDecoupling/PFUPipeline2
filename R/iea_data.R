#' Load a tidy IEA data frame
#'
#' Loads IEA data from `iea_data_path` and pre-processes
#' to make a data frame.#'
#'
#' @param iea_data_path The path to an IEA data file.
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
#' @param db_table_name The name of the table into which the data should be stored
#'                      in the database at `conn`.
#' @param conn The database connection.
#' @param schema The data model (`dm` object) for the database in `conn`.
#'               See details.
#' @param fk_parent_tables A named list of all parent tables
#'                         for the foreign keys in `db_table_name`.
#'                         See details.
#'
#' @return A tidy data frame of IEA extended world energy balance data.
#'
#' @export
load_iea_data <- function(iea_data_path,
                          override_df,
                          dataset,
                          specify_non_energy_flows = TRUE,
                          apply_fixes = TRUE,
                          iea_countries = c(PFUPipelineTools::canonical_countries, wrld = "WRLD") |> unlist(),
                          country = IEATools::iea_cols$country,
                          dataset_colname = PFUPipelineTools::dataset_info$dataset_colname,
                          db_table_name,
                          conn,
                          schema = schema_from_conn(conn = conn),
                          fk_parent_tables = get_all_fk_tables(conn = conn, schema = schema)) {

  iea_data_path |>
    IEATools::load_tidy_iea_df(override_df = override_df,
                               specify_non_energy_flows = specify_non_energy_flows,
                               apply_fixes = apply_fixes) |>
    dplyr::filter(.data[[country]] %in% iea_countries) |>
    dplyr::mutate(
      "{dataset_colname}" := dataset
    ) |>
    dplyr::relocate(dplyr::all_of(dataset_colname)) |>
    PFUPipelineTools::pl_upsert(db_table_name = db_table_name,
                                in_place = TRUE,
                                conn = conn,
                                schema = schema,
                                fk_parent_tables = fk_parent_tables)
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
                                     IEATools::iea_cols$product),
                        conn,
                        schema = schema_from_conn(conn),
                        fk_parent_tables = get_all_fk_tables(conn = conn, schema = schema)) {
  .iea_data |>
    PFUPipelineTools::pl_collect_from_hash(conn = conn,
                                           schema = schema,
                                           fk_parent_tables = fk_parent_tables) |>
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
                                       IEATools::iea_cols$product),
                          conn,
                          schema = schema_from_conn(conn),
                          fk_parent_tables = get_all_fk_tables(conn = conn, schema = schema)) {
  .iea_data |>
    PFUPipelineTools::pl_collect_from_hash(conn = conn,
                                           schema = schema,
                                           fk_parent_tables = fk_parent_tables) |>
    dplyr::group_by(!!as.name(grp_vars)) |>
    IEATools::fix_tidy_iea_df_balances(max_fix = max_fix) |>
    dplyr::ungroup() |>
    PFUPipelineTools::pl_upsert(in_place = TRUE,
                                db_table_name = db_table_name,
                                conn = conn,
                                schema = schema,
                                fk_parent_tables = fk_parent_tables)
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
  browser()
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
