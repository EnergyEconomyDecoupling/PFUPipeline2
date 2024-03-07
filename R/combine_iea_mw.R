
#' Aggregate ILO and FAO country data into IEA country groupings
#'
#' This function reads an exemplar table containing a countries country code and
#' the country code associated with the territory that the IEA data was recorded
#' in for the years 1960 - 2020. A country as defined by it's territorial
#' boundaries in 2020 may have had it's energy statistics recorded in any number
#' of regions, over any number of years.
#'
#' @param mw_df A data frame containing raw animal muscle work or human muscle
#'              work data. Usually retrieved from the `AMWPFUDataRaw` and
#'              `HMWPFUDataRaw` targets.
#'
#' @param exemplar_table The exemplar table.
#' @param version The version of the databse being created.
#' @param country,year,unit,e_dot See `IEATools::iea_cols`.
#' @param agg_code_col,region_code,exemplar_country See `PFUPipelineTools::exemplar_names`.
#' @param species,stage_col,sector_col See `MWTools::mw_constants`.
#' @param dataset The name of the dataset column.
#'                Default is `PFUPipelineTools::dataset_info$dataset_colname`.
#'
#' @export
aggcountries_mw_to_iea <- function(mw_df,
                                   exemplar_table,
                                   dataset,
                                   country = IEATools::iea_cols$country,
                                   year = IEATools::iea_cols$year,
                                   unit = IEATools::iea_cols$unit,
                                   e_dot = IEATools::iea_cols$e_dot,
                                   agg_code_col = PFUPipelineTools::exemplar_names$agg_code_col,
                                   region_code = PFUPipelineTools::exemplar_names$region_code,
                                   exemplar_country = PFUPipelineTools::exemplar_names$exemplar_country,
                                   species = MWTools::mw_constants$species,
                                   stage_col = MWTools::mw_constants$stage_col,
                                   sector_col = MWTools::mw_constants$sector_col,
                                   dataset_colname = PFUPipelineTools::dataset_info$dataset_colname){

  focused_exemplar_table <- exemplar_table |>
    dplyr::select(-dplyr::all_of(c(region_code, exemplar_country))) |>
    tidyr::pivot_longer(cols = -dplyr::all_of(c(country)),
                        names_to = year,
                        values_to = agg_code_col) |>
    dplyr::mutate("{year}" := as.numeric(.data[[year]]))

  agg_mw_df <- mw_df %>%
    dplyr::left_join(focused_exemplar_table, by = dplyr::all_of(c(country, year))) |>
    dplyr::select(-dplyr::all_of(country)) |>
    dplyr::group_by(.data[[year]],
                    .data[[species]],
                    .data[[stage_col]],
                    .data[[sector_col]],
                    .data[[unit]],
                    .data[[agg_code_col]]) |>
    dplyr::summarise("{e_dot}" := sum(.data[[e_dot]]),
                     .groups = "drop") |>
    dplyr::rename("{country}" := !!agg_code_col) |>
    dplyr::relocate(dplyr::all_of(country)) |>
    dplyr::mutate(
      "{dataset_colname}" := dataset
    ) |>
    dplyr::relocate(dplyr::all_of(dataset_colname))
}
