#' Load the phi constants file
#'
#' phi is the exergy-to-energy ratio.
#' During execution of the pipeline,
#' phi values are needed.
#' This function reads a file a phi values
#' and uploads to the database.
#'
#' @param phi_constants_path The path to the phi_constants file.
#' @param dataset The string name of the dataset for this data.
#' @param db_table_name The name of the specified IEA data table in `conn`.
#' @param conn The database connection.
#' @param schema The data model (`dm` object) for the database in `conn`.
#'               See details.
#' @param fk_parent_tables A named list of all parent tables
#'                         for the foreign keys in `db_table_name`.
#'                         See details.
#' @param dataset_colname The name of the dataset column in the output.
#'                        Default is `PFUPipelineTools::dataset_info$dataset_colname`.
#'
#' @return A hashed version of the phi constants table.
#'
#' @export
load_phi_values <- function(phi_constants_path) {
  phi_constants_path |>
    IEATools::load_phi_constants_table()
}


#' Assemble completed phi (exergy-to-energy ratio) tables
#'
#' This function is used in the drake workflow to assemble completed phi (exergy-to-energy ratio) tables
#' given a set of phi tables read from machine data files and a phi constants table.
#' The algorithm gives priority in this order:
#' 1. phi values from the `incomplete_phi_u_table` argument
#' 2. phi values from climatic temperatures
#' 3. phi values from the `phi_constants_table` argument
#'
#' Note that the needed phi values are taken from `completed_efficiency_table`
#' (when not `NULL`).
#' If `completed_efficiency_table` is `NULL`,
#' the needed phi values are taken from `incomplete_phi_u_table`,
#' meaning that any empty (`NA`) phi values are obtained from climatic temperatures or `phi_constants_table`.
#'
#' @param incomplete_phi_u_table A data frame of phi values read from machine efficiency and phi data files.
#'                               This data frame can be "incomplete," i.e., it can be missing
#'                               phi values.
#'                               The phi values from `phi_constants_table` will be used instead.
#' @param phi_constants_table A data frame of constant phi values with reasonable default values for all energy products.
#' @param completed_efficiency_table A data frame containing completed efficiency tables.
#'                                   This data frame identifies all useful products
#'                                   for which we need phi values.
#'                                   Default is `NULL`, meaning that missing (`NA`) values in `incomplete_phi_u_table`
#'                                   should be completed.
#' @param countries A vector of countries for which completed phi tables are to be assembled.
#' @param years The years for which analysis is desired. Default is `NULL`, meaning analyze all years.
#' @param country,year,product See `IEATools::iea_cols`.
#' @param machine,quantity,phi_u,.values,eu_product,eta_fu_source See `IEATools::template_cols`.
#' @param phi_colname,phi_source_colname,is_useful See `IEATools::phi_constants_names`.
#' @param eta_fu_tables,phi_constants See `PFUPipelineTools::phi_sources`.
#'
#' @return A data frame of phi values for every combination of country, year, machine, destination, etc.
#'
#' @export
#'
#' @examples
#' library(dplyr)
#' library(IEATools)
#' library(magrittr)
#' phi_constants_table <- IEATools::load_phi_constants_table()
#' # Load a phi_u_table.
#' phi_table <- IEATools::load_eta_fu_data() |>
#'   # Convert to tidy format.
#'   dplyr::mutate(
#'     "{IEATools::template_cols$maximum_values}" := NULL,
#'     "{IEATools::iea_cols$unit}" := NULL
#'   ) |>
#'   tidyr::pivot_longer(cols = IEATools::year_cols(.),
#'                       names_to = IEATools::iea_cols$year,
#'                       values_to = IEATools::template_cols$.values) |>
#'   # Convert to a table of phi values only
#'   dplyr::filter(.data[[IEATools::template_cols$quantity]] == IEATools::template_cols$phi_u)
#' # Set a value to NA (Charcoal stoves, MTH.100.C, GHA, 1971) in the phi table.
#'   incomplete_phi_table <- phi_table |>
#'     dplyr::mutate(
#'       "{IEATools::template_cols$.values}" := dplyr::case_when(
#'         .data[[IEATools::iea_cols$country]] == "GHA" &
#'         .data[[IEATools::iea_cols$year]] == 1971 &
#'         .data[[IEATools::template_cols$machine]] == "Charcoal stoves" ~ NA_real_,
#'         TRUE ~ .data[[IEATools::template_cols$.values]]
#'       )
#'     )
#' # Run through the assemble_phi_u_tables function
#' completed_phi_u_table <- assemble_phi_u_tables(incomplete_phi_table,
#'                                                phi_constants_table,
#'                                                countries = "GHA")
#' # Show that Charcoal stoves was filled
#' completed_phi_u_table |>
#'   dplyr::filter(.data[[IEATools::template_cols$machine]] == "Charcoal stoves")
assemble_phi_u_tables <- function(incomplete_phi_u_table,
                                  phi_constants_table,
                                  completed_efficiency_table = NULL,
                                  countries,
                                  years = NULL,
                                  country = IEATools::iea_cols$country,
                                  year = IEATools::iea_cols$year,
                                  product = IEATools::iea_cols$product,
                                  machine = IEATools::template_cols$machine,
                                  quantity = IEATools::template_cols$quantity,
                                  phi_u = IEATools::template_cols$phi_u,
                                  .values = IEATools::template_cols$.values,
                                  eu_product = IEATools::template_cols$eu_product,
                                  eta_fu_source = IEATools::template_cols$eta_fu_source,
                                  phi_colname = IEATools::phi_constants_names$phi_colname,
                                  phi_source_colname = IEATools::phi_constants_names$phi_source_colname,
                                  is_useful = IEATools::phi_constants_names$is_useful_colname,
                                  eta_fu_tables = PFUPipelineTools::phi_sources$eta_fu_tables,
                                  phi_constants = PFUPipelineTools::phi_sources$phi_constants) {

  lapply(countries, FUN = function(coun) {

    # Get a data frame of needed phi_u cases.
    # There are two potential sources of the needed phi_u cases.
    # First is a completed_efficiency_table,
    # a data frame which tells us
    # all combinations of country, year, machine, etc.,
    # that make useful energy.
    # Every useful energy carrier needs a phi value.
    # If the completed_efficiency_table is NULL,
    # the second source of information is the incomplete_phi_u_table,
    # which may contain missing (i.e. NA) values.
    if (is.null(completed_efficiency_table)) {
      needed_phi_u_cases <- incomplete_phi_u_table |>
        dplyr::filter(#.data[[country]] == coun,
                      .data[[quantity]] == phi_u) |>
        dplyr::mutate(
          "{.values}" := NULL
        )
    } else {
      needed_phi_u_cases <- completed_efficiency_table |>
        # dplyr::filter(.data[[country]] == coun) |>
        dplyr::mutate(
          # The completed_effiiency_table will have eta_fu for its quantity.
          # We want phi_u
          "{quantity}" := phi_u,
          # Eliminate the phi_u_source column
          "{phi_source_colname}" := NULL,
          # Eliminate the eta_fu_source column. We will add a phi_u_source column later
          "{eta_fu_source}" := NULL,
          # Eliminate the .values column. It contains eta_fu values.
          "{.values}" := NULL
        )
    }

    # Get a data frame of extant phi_u values
    # from the efficiency tables.
    # relevant to the particular analysis
    # for this country.
    # This data frame comes from the incomplete_phi_u_table.
    # Thus, any phi values coming from the efficiency tables or machine data tables
    # will have first priority
    phi_u_from_eta_fu_tables <- incomplete_phi_u_table |>
      dplyr::filter(#.data[[country]] == coun,
                    .data[[quantity]] == phi_u,
                    !is.na(.data[[.values]])) |>
      dplyr::mutate(
        "{phi_source_colname}" := eta_fu_tables
      )
    if (!is.null(completed_efficiency_table)) {
      # incomplete_phi_u_table may have more rows than we need.
      # I.e., it may contain rows for years/countries that do not exist in
      # completed_efficiency_table.
      # Having the extra rows may cause problems later, so filter out the unneeded rows here.
      # But only do this step if completed_efficiency_table is present.
      phi_u_from_eta_fu_tables <- dplyr::semi_join(phi_u_from_eta_fu_tables,
                                                   completed_efficiency_table,
                                                   by = setdiff(names(incomplete_phi_u_table),
                                                                c(.values, quantity)))
    }

    phi_u_table <- phi_u_from_eta_fu_tables

    # Figure out missing phi_u cases by anti joining needed and present
    missing_phi_u_cases <- dplyr::anti_join(needed_phi_u_cases,
                                            phi_u_table,
                                            by = names(needed_phi_u_cases)) |>
      dplyr::mutate(
        # Strip off the .values column, if present.
        "{.values}" := NULL
      )

    # Fill the missing values

    # Third priority will come from the phi_constants data frame.
    phi_u_from_phi_constants <- missing_phi_u_cases |>
      # left_join to pick up the values from phi_constants_table
      dplyr::left_join(phi_constants_table |>
                         # Use only the useful data in phi_constants_table
                         dplyr::filter(.data[[is_useful]]) |>
                         # Strip off the is.useful column, as it is no longer necessary.
                         dplyr::mutate("{is_useful}" := NULL) |>
                         # Rename the Product column to Eu.product to match found_phi_values
                         dplyr::rename("{eu_product}" := dplyr::all_of(product)),
                       by = eu_product) |>
      # At this point, we have the wrong column name.
      # Rename to match the expected column name.
      dplyr::rename(
        "{.values}" := dplyr::all_of(phi_colname)
      ) |>
      # Add that these phi values came from the constants table
      dplyr::mutate(
        "{phi_source_colname}" := phi_constants
      ) |>
      # Eliminate cases that are still missing.
      dplyr::filter(!is.na(.data[[.values]]))

    phi_u_table <- dplyr::bind_rows(phi_u_table, phi_u_from_phi_constants)

    # Calculate remaining missing values
    still_missing <- dplyr::anti_join(needed_phi_u_cases, phi_u_table, by = names(needed_phi_u_cases)) |>
      # Strip off the .values column, if present.
      dplyr::mutate(
        "{.values}" := NULL
      )

    # Ensure that ALL rows have a value in .values
    if (nrow(still_missing) > 0) {
      err_msg <- paste("Not all useful energy carriers have been assigned phi values in assemble_phi_u_tables(). Missing combinations are:",
                       still_missing |>
                         dplyr::select(dplyr::all_of(c(country, year, machine, eu_product))) |>
                         matsindf::df_to_msg())
      stop(err_msg)
    }

    # Now rbind everything together and return
    return(phi_u_table)
  }) |>
    dplyr::bind_rows()
}
