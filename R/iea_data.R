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
#' @param countries The countries to be checked for energy balance.
#' @param grp_vars The groups that should be checked. Default is
#'                 `c(country, IEATools::iea_cols$method, IEATools::iea_cols$energy_type, IEATools::iea_cols$last_stage, IEATools::iea_cols$product)`.
#'
#' @return A logical stating whether all products are balanced for the country of interest
#'
#' @export
is_balanced <- function(.iea_data,
                        countries,
                        grp_vars = c(IEATools::iea_cols$country,
                                     IEATools::iea_cols$method,
                                     IEATools::iea_cols$energy_type,
                                     IEATools::iea_cols$last_stage,
                                     IEATools::iea_cols$year,
                                     IEATools::iea_cols$product)) {

  if (is.null(.iea_data)) {
    # Found now rows of data for this country.
    return(NULL)
  }
  .iea_data |>
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
#' @param countries The countries to be balanced.
#' @param max_fix The maximum allowable energy imbalance to fix.
#'                Default is `3`.
#' @param grp_vars the groups that should be checked.
#'                 Default is
#'                 `c(IEATools::iea_cols$country, IEATools::iea_cols$method, IEATools::iea_cols$energy_type, IEATools::iea_cols$last_stage, IEATools::iea_cols$product)`.
#'
#' @return A ticket to recover the balanced data frame.
#'
#' @export
make_balanced <- function(.iea_data,
                          countries,
                          max_fix = 6,
                          grp_vars = c(IEATools::iea_cols$country,
                                       IEATools::iea_cols$method,
                                       IEATools::iea_cols$energy_type,
                                       IEATools::iea_cols$last_stage,
                                       IEATools::iea_cols$year,
                                       IEATools::iea_cols$product)) {

  if (is.null(.iea_data)) {
    return(NULL)
  }

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
#' @param balanced_iea_data IEA data that have already been balanced.
#' @param countries The countries to specify.
#'
#' @return A ticket to recover a data frame of specified IEA data.
#'
#' @export
specify <- function(balanced_iea_data,
                    countries) {

  if (is.null(balanced_iea_data)) {
    return(NULL)
  }
  balanced_iea_data |>
    IEATools::specify_all()
}


#' Convert to PSUT matrices
#'
#' Converts tidy IEA data to PSUT matrices in a way that is amenable to drake subtargets.
#' Internally, `IEATools::prep_psut()` does the conversion to matrices.
#'
#' @param specified_iea_data A data frame that has already been specified via `specify()`.
#' @param matrix_class The type of matrix to be created.
#'                     Default is "Matrix" for sparse matrices.
#' @param countries The countries to convert to PSUT format.
#'
#' @return A ticket to recover the PSUT data frame.
#'
#' @export
make_iea_psut <- function(specified_iea_data,
                          countries) {

  if (is.null(specified_iea_data)) {
    return(NULL)
  }
  specified_iea_data |>
    IEATools::prep_psut(matrix_class = "Matrix")
}

