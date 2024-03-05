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
                          dataset_colname = PFUPipelineTools::dataset_info$dataset_colname) {

  iea_data_path |>
    IEATools::load_tidy_iea_df(override_df = override_df,
                               specify_non_energy_flows = specify_non_energy_flows,
                               apply_fixes = apply_fixes) |>
    dplyr::filter(.data[[country]] %in% iea_countries) |>
    dplyr::mutate(
      "{dataset_colname}" := dataset
    )
}


#' Tells whether IEA data are balanced
#'
#' Performs the the energy balance check in a way that is amenable to drake subtargets.
#' Internally, this function uses `IEATools::calc_tidy_iea_df_balances()`.
#' Grouping is doing internal to this function using the value of `grp_vars`.
#'
#' @param .iea_data A tidy IEA data frame
#' @param conn The database connection.
#' @param grp_vars The groups that should be checked. Default is
#'                 `c(country, IEATools::iea_cols$method, IEATools::iea_cols$energy_type, IEATools::iea_cols$last_stage, IEATools::iea_cols$product)`.
#'
#' @return a logical stating whether all products are balanced for the country of interest
#'
#' @export
is_balanced <- function(.iea_data,
                        conn,
                        grp_vars = c(IEATools::iea_cols$country,
                                     IEATools::iea_cols$method,
                                     IEATools::iea_cols$energy_type,
                                     IEATools::iea_cols$last_stage,
                                     IEATools::iea_cols$year,
                                     IEATools::iea_cols$product)) {
  .iea_data |>
    # Get data from the database
    PFUPipelineTools::pl_collect_from_hash(conn = conn) |>
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
#' @param .iea_data A tidy IEA data frame.
#' @param conn The database connection.
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
                          conn,
                          max_fix = 6,
                          balanced_table_name = "BalancedIEAData",
                          grp_vars = c(IEATools::iea_cols$country,
                                       IEATools::iea_cols$method,
                                       IEATools::iea_cols$energy_type,
                                       IEATools::iea_cols$last_stage,
                                       IEATools::iea_cols$year,
                                       IEATools::iea_cols$product)) {
  .iea_data |>
    # Get data from the database
    PFUPipelineTools::pl_collect_from_hash(conn = conn) |>
    dplyr::group_by(!!as.name(grp_vars)) |>
    IEATools::fix_tidy_iea_df_balances(max_fix = max_fix) |>
    dplyr::ungroup() |>
    PFUPipelineTools::pl_upsert(conn = conn,
                                db_table_name = balanced_table_name,
                                in_place = TRUE)
}


#' Specify the IEA data
#'
#' Specifies the IEA data in a way that is amenable to targets parallelization
#' See `IEATools::specify_all()` for details.
#'
#' @param BalancedIEAData IEA data that have already been balanced.
#' @param conn The database connection.
#' @param specified_table_name The name of the specified IEA data table in `conn`.
#'                             Default is "SpecifiedIEAData".
#'
#' @return A data frame of specified IEA data.
#'
#' @export
specify <- function(BalancedIEAData,
                    conn,
                    specified_table_name = "SpecifiedIEAData") {
  BalancedIEAData |>
    PFUPipelineTools::pl_collect_from_hash(conn = conn) |>
    IEATools::specify_all() |>
    PFUPipelineTools::pl_upsert(conn = conn,
                                db_table_name = specified_table_name,
                                in_place = TRUE)
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
