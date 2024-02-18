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
                          country = IEATools::iea_cols$country) {

  iea_data_path |>
    IEATools::load_tidy_iea_df(override_df = override_df,
                               specify_non_energy_flows = specify_non_energy_flows,
                               apply_fixes = apply_fixes) |>
    dplyr::filter(.data[[country]] %in% iea_countries) |>
    dplyr::mutate(
      Dataset = dataset
    )
}


#' Tells whether IEA data are balanced
#'
#' Performs the the energy balance check in a way that is amenable to drake subtargets.
#' Internally, this function uses `IEATools::calc_tidy_iea_df_balances()`.
#' Grouping is doing internal to this function using the value of `grp_vars`.
#'
#' @param .iea_data a tidy IEA data frame
#' @param countries the countries for which balancing should be checked as strings
#' @param country The name of the country column in `IEAData`. Default is `r IEATools::iea_cols$country`.
#' @param grp_vars the groups that should be checked. Default is
#'                 `c(country, IEATools::iea_cols$method, IEATools::iea_cols$energy_type, IEATools::iea_cols$last_stage, IEATools::iea_cols$product)`.
#'
#' @return a logical stating whether all products are balanced for the country of interest
#'
#' @export
is_balanced <- function(.iea_data,
                        countries,
                        country = IEATools::iea_cols$country,
                        grp_vars = c(country,
                                     IEATools::iea_cols$method,
                                     IEATools::iea_cols$energy_type,
                                     IEATools::iea_cols$last_stage,
                                     IEATools::iea_cols$year,
                                     IEATools::iea_cols$product)) {
  table_name <- .iea_data |>
    dplyr::select(PFUPipelineTools::hashed_table_colnames$db_table_name) |>
    unique() |>
    unname() |>
    unlist()

  # Get data from the database
  PFUPipelineTools::pl_download(table_name)
  dplyr::filter(.iea_data, .data[[country]] %in% countries) %>%
    dplyr::group_by(!!as.name(grp_vars)) %>%
    IEATools::calc_tidy_iea_df_balances() %>%
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
