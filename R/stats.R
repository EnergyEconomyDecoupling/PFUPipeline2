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




# Graph code for the paper


# counts <- dplyr::bind_rows(dplyr::count(dplyr::group_by(stats, Country), CsourceType),
#                            dplyr::count(stats, CsourceType) |>
#                              dplyr::mutate(Country = "World")) |>
#   tidyr::pivot_wider(names_from = CsourceType, values_from = n) |>
#   tidyr::replace_na(replace = list(WRLD = 0, exemplar = 0, own = 0)) |>
#   dplyr::mutate(
#     table = "CompletedAllocationTables"
#   )
#
# h_size <- 2
#
# counts |>
#   dplyr::filter(Country %in% c("GHA", "GBR", "USA", "ZAF")) |>
#   tidyr::pivot_longer(cols = c(WRLD, exemplar, own),
#                       names_to = "var",
#                       values_to = "val") |>
#   dplyr::mutate(
#     var = factor(var, levels = c("own", "exemplar", "WRLD")),
#     x = h_size
#   ) |>
#   ggplot2::ggplot(mapping = ggplot2::aes(x = x, y = val, fill = var)) +
#   ggplot2::geom_col(color = "black") +
#   ggplot2::geom_text(ggplot2::aes(label = val),
#                      position = ggplot2::position_stack(vjust = 0.5),
#                      size = 3) +
#   ggplot2::coord_polar(theta = "y", direction = -1) +
#   ggplot2::xlim(c(0.2, h_size + 0.5)) +
#   ggplot2::facet_wrap(facets = "Country", scales = "free_y") +
#   ggplot2::theme(panel.background = ggplot2::element_rect(fill = "white"),
#                  panel.grid = ggplot2::element_blank(),
#                  axis.title = ggplot2::element_blank(),
#                  axis.ticks = ggplot2::element_blank(),
#                  axis.text = ggplot2::element_blank())
#
