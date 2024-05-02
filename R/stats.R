calc_db_coverage_stats <- function(completed_allcoation_tables,
                                   completed_efficiency_tables,
                                   country = IEATools::iea_cols$country,
                                   c_source = IEATools::template_cols$c_source,
                                   c_source_category = "CSourceCategory",
                                   own_country = "Own country",
                                   exemplar = "Exemplar",
                                   continent = "Continent",
                                   wrld = PFUPipelineTools::all_countries$wrld,
                                   unknown = "Unknown") {

  stats <- completed_allocation_tables |>
    dplyr::mutate(
      "{c_source_category}" := dplyr::case_when(
        .data[[c_source]] == .data[[country]] ~ own_country,
        .data[[c_source]] %in% c("AFRI", "ASIA", "EURP", "MIDE", "NAMR", "OCEN", "SAMR") ~ continent,
        .data[[c_source]] == wrld ~ wrld,
        .data[[c_source]] %in% PFUPipelineTools::all_countries ~ exemplar,
        TRUE ~ unknown
      )
    )

}
