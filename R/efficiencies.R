#' Calculates the aggregated C matrices
#'
#' @param C_mats A data frame containing the C matrices.
#' @param psut_iea A data frame containing IEA data at the final stage, in energy terms, in a PSUT format.
#' @param countries The countries for which this calculation should be done.
#' @param C_EIOU The name of the column containing the C_EIOU matrix in the input data frame.
#' @param C_Y The name of the column containing the C_Y matrix in the input data frame.
#' @param Y The name of the column containing the Y matrix in the input data frame.
#' @param U_EIOU The name of the column containing the U_EIOU matrix in the input data frame.
#' @param C_EIOU_agg The name of the column containing the C_EIOU aggregated matrix in the output data frame.
#' @param C_Y_agg The name of the column containing the C_Y aggregated matrix in the output data frame.
#' @param C_EIOU_Y_agg The name of the column containing the C_EIOU_Y aggregated matrix in the output data frame.
#' @param country,method,energy_type,last_stage,year See `IEATools::iea_cols`.
#'
#' @return A data frame containing the aggregated C matrices for EIOU, Y, and EIOU and Y together.
#'
#' @export
calc_C_mats_agg <- function(C_mats,
                            psut_iea,
                            countries,
                            C_EIOU = "C_EIOU",
                            C_Y = "C_Y",
                            Y = "Y",
                            U_EIOU = "U_EIOU",
                            C_EIOU_agg = "C_EIOU_agg",
                            C_Y_agg = "C_Y_agg",
                            C_EIOU_Y_agg = "C_EIOU_Y_agg",
                            country = IEATools::iea_cols$country,
                            method = IEATools::iea_cols$method,
                            energy_type = IEATools::iea_cols$energy_type,
                            last_stage = IEATools::iea_cols$last_stage,
                            year = IEATools::iea_cols$year) {

  # These assignments eliminate notes in R CMD check
  eiou_vec <- NULL
  y_vec <- NULL
  Alloc_mat_EIOU <- NULL
  Alloc_mat_Y <- NULL
  Alloc_mat_EIOU_Y <- NULL
  f_EIOU <- NULL
  f_Y <- NULL
  f_EIOU_Y <- NULL

  C_mats_agg <- dplyr::left_join(
    C_mats, psut_iea, by = c({country}, {method}, {energy_type}, {last_stage}, {year})
  ) |>
    dplyr::select(tidyselect::any_of(c(country, method, energy_type, last_stage, year, C_EIOU, C_Y, Y, U_EIOU))) |>
    # Calculating all the bunch of vectors and matrices needed now:
    dplyr::mutate(
      # Total use of product p industry EIOU industry i, as a vector. U_EIOU vectorised.
      eiou_vec = matsbyname::vectorize_byname(a = .data[[U_EIOU]], notation = list(RCLabels::arrow_notation)),
      # Total use of product p in final demand sector s, as a vector. Y vectorised.
      y_vec = matsbyname::vectorize_byname(a = .data[[Y]], notation = list(RCLabels::arrow_notation)),
      # Total use of product p in machine m across all EIOU industries aggregated
      Alloc_mat_EIOU = matsbyname::matrixproduct_byname(matsbyname::hatize_byname(eiou_vec, keep = "rownames"),
                                                        .data[[C_EIOU]]) |>
        matsbyname::aggregate_pieces_byname(piece = "noun",
                                            margin = 1,
                                            notation = list(RCLabels::arrow_notation)),
      # Total use of product p in final demand sector s across all final demand sectors aggregated
      Alloc_mat_Y = matsbyname::matrixproduct_byname(matsbyname::hatize_byname(y_vec, keep = "rownames"),
                                                     .data[[C_Y]]) |>
        matsbyname::aggregate_pieces_byname(piece = "noun",
                                            margin = 1,
                                            notation = list(RCLabels::arrow_notation)),
      # Total use of product p in machine m Y and EIOU aggregated
      Alloc_mat_EIOU_Y = matsbyname::sum_byname(Alloc_mat_EIOU, Alloc_mat_Y),
      # Total use of product p in EIOU industries
      f_EIOU = matsbyname::rowsums_byname(Alloc_mat_EIOU),
      # Total use of product p in Y
      f_Y = matsbyname::rowsums_byname(Alloc_mat_Y),
      # Total use of product p in EIOU and Y
      f_EIOU_Y = matsbyname::rowsums_byname(Alloc_mat_EIOU_Y),
      # Share of product p used in each machine m across EIOU. Sum by product yields 1.
      "{C_EIOU_agg}" := matsbyname::matrixproduct_byname(matsbyname::hatinv_byname(f_EIOU, keep = "rownames"),
                                                         Alloc_mat_EIOU),
      # Share of product p used in each machine m across final demand. Sum by product yields 1.
      "{C_Y_agg}" := matsbyname::matrixproduct_byname(matsbyname::hatinv_byname(f_Y, keep = "rownames"),
                                                      Alloc_mat_Y),
      # Share of product p used in each machine m across final demand and EIOU. Sum by product yields 1.
      "{C_EIOU_Y_agg}" := matsbyname::matrixproduct_byname(matsbyname::hatinv_byname(f_EIOU_Y, keep = "rownames"),
                                                           Alloc_mat_EIOU_Y),
    ) |>
    dplyr::select(tidyselect::any_of(c(country, method, energy_type, last_stage, year, C_EIOU_agg, C_Y_agg, C_EIOU_Y_agg)))

  return(C_mats_agg)
}


#' Calculate final-to-useful efficiencies
#'
#' Knowing allocations (`C_mats`), machine efficiencies (`eta_m_vecs`), and
#' exergy-to-energy ratios (`phi_vecs`), it is possible to
#' calculate the final-to-useful efficiencies for all
#' final demand and energy industry own use
#' in an energy conversion chain.
#' This function performs those calculations using
#' `Recca::calc_eta_fu_Y_eiou()`.
#'
#' @param C_mats A data frame containing allocation matrices.
#' @param eta_m_vecs A data frame containing vectors of machine efficiencies, probably the Etafuvecs target.
#' @param phi_vecs A data frame containing vectors of exergy-to-energy ratios, probably the Phivecs target.
#' @param countries The countries for which this analysis should be performed.
#' @param country,last_stage,energy_type,method,year See `IEATools::iea_cols`.
#'
#' @return A data frame of final-to-useful efficiencies by energy sector and energy carrier.
#'
#' @export
calc_fu_Y_EIOU_efficiencies <- function(C_mats,
                                        eta_m_vecs,
                                        phi_vecs,
                                        countries,
                                        country = IEATools::iea_cols$country,
                                        last_stage = IEATools::iea_cols$last_stage,
                                        energy_type = IEATools::iea_cols$energy_type,
                                        method = IEATools::iea_cols$method,
                                        year = IEATools::iea_cols$year,
                                        etafu_colname = "etafu") {

  # Make one large data frame.
  dplyr::full_join(C_mats, eta_m_vecs, by = c(country, last_stage, energy_type, method, year)) |>
    dplyr::full_join(phi_vecs, by = c(country, year)) |>
    # Run the calculations
    Recca::calc_eta_fu_Y_eiou(eta_i = etafu_colname)
}
