download_amw_data <- function(version,
                              temp_location = tempdir(),
                              live_animals_code = "QCL") {

  # Download .zip file containing data for the number of live animals into a specified folder
  fao_amw_data <- FAOSTAT::get_faostat_bulk(code = live_animals_code,
                                            data_folder = temp_location)

  # Save downloaded file
  saveRDS(object = fao_amw_data,
          file = PFUSetup::get_abs_paths(version = version)[["fao_data_path"]])


}



#' Rename muscle work sectors to comport with IEA sectors.
#'
#' The muscle work methodology in the `MWTools` package
#' uses slightly different final demand sector names compared to the IEA
#' and the `IEATools` package..
#' This function converts the `MWTools` sector names to appropriate IEA sector names.
#'
#' @param .df A data frame of muscle work data.
#' @param sector_colname The name of the sector column.
#'                       Default is `MWTools::mw_constants$sector_col`.
#' @param original_sector_names A vector of string sector names that will be replaced.
#' @param new_sector_names A vector of string sector names that will appear in output.
#'
#' @return A data frame with renamed sectors.
#'
#' @export
#'
#' @examples
#' df <- tibble::tribble(~Sector, ~value,
#'                       MWTools::mw_sectors$transport_sector,         10,
#'                       MWTools::mw_sectors$agriculture_broad.sector, 11,
#'                       MWTools::mw_sectors$services_broad.sector,    12,
#'                       MWTools::mw_sectors$industry_broad.sector,    13,
#'                       "bogus",                                      14)
#' df
#' rename_mw_sectors(df)
rename_mw_sectors <- function(.df,
                              sector_colname = MWTools::mw_constants$sector_col,
                              original_sector_names = c(MWTools::mw_sectors$agriculture_broad.sector,
                                                        MWTools::mw_sectors$transport_sector,
                                                        MWTools::mw_sectors$services_broad.sector,
                                                        MWTools::mw_sectors$industry_broad.sector),
                              new_sector_names = c(IEATools::other_flows$agriculture_forestry,
                                                   IEATools::transport_flows$transport_not_elsewhere_specified,
                                                   IEATools::other_flows$commercial_and_public_services,
                                                   IEATools::industry_flows$industry_not_elsewhere_specified)) {

  # Create a named vector to assist with refactoring.
  refactor_vector <- new_sector_names %>%
    magrittr::set_names(original_sector_names)
  .df %>%
    dplyr::mutate(
      "{sector_colname}" := dplyr::recode(.data[[sector_colname]], !!!refactor_vector)
    )
}


#' Load animal muscle work data
#'
#' This function loads animal muscle work data and
#' renames the sectors according to the default arguments to `rename_mw_sectors()`.
#'
#' @param fao_data The FAO data.
#' @param mw_concordance_path The path to the muscle work concordance.
#' @param amw_analysis_data_path The path the animal muscle work data.
#'
#' @return A data frame of animal muscle work data.
#'
#' @export
prep_amw_pfu_data <- function(fao_data,
                              mw_concordance_path,
                              amw_analysis_data_path) {
  fao_data |>
    MWTools::calc_amw_pfu(concordance_path = mw_concordance_path,
                          amw_analysis_data_path = amw_analysis_data_path) |>
    rename_mw_sectors()
}


#' Load human muscle work data
#'
#' This function loads human muscle work data and
#' renames the sectors according to the default arguments to `rename_mw_sectors()`.
#'
#' @param ilo_working_hours_data_path The path to ILO working hours data.
#' @param ilo_employment_data_path The path to the ILO employment data.
#' @param mw_concordance_path The path to the muscle work concordance.
#' @param hmw_analysis_data_path The path the human muscle work data.
#' @param version The version of the databse being created.
#' @param dataset The name of the dataset column.
#'                Default is `PFUPipelineTools::dataset_info$dataset_colname`.
#'
#' @return A data frame of human muscle work data.
#'
#' @export
prep_hmw_pfu_data <- function(ilo_working_hours_data,
                              ilo_employment_data,
                              mw_concordance_path,
                              hmw_analysis_data_path) {

  MWTools::prepareRawILOData(ilo_working_hours_data = ilo_working_hours_data,
                             ilo_employment_data = ilo_employment_data) |>
    MWTools::calc_hmw_pfu(concordance_path = mw_concordance_path,
                          hmw_analysis_data_path = hmw_analysis_data_path)  |>
    rename_mw_sectors()
}

