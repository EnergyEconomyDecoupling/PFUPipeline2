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

  if (is.null(C_mats) & is.null(psut_iea)) {
    # Nothing to be done.
    return(NULL)
  }

  # These assignments eliminate notes in R CMD check
  eiou_vec <- NULL
  y_vec <- NULL
  Alloc_mat_EIOU <- NULL
  Alloc_mat_Y <- NULL
  Alloc_mat_EIOU_Y <- NULL
  f_EIOU <- NULL
  f_Y <- NULL
  f_EIOU_Y <- NULL

  # C_mats_agg <- dplyr::left_join(
  #   C_mats, psut_iea, by = c({country}, {method}, {energy_type}, {last_stage}, {year})
  # ) |>
  #   dplyr::select(tidyselect::any_of(c(country, method, energy_type, last_stage, year, C_EIOU, C_Y, Y, U_EIOU))) |>
  #   # Calculating all the bunch of vectors and matrices needed now:
  #   dplyr::mutate(
  #     # Total use of product p industry EIOU industry i, as a vector. U_EIOU vectorised.
  #     eiou_vec = matsbyname::vectorize_byname(a = .data[[U_EIOU]], notation = list(RCLabels::arrow_notation)),
  #     # Total use of product p in final demand sector s, as a vector. Y vectorised.
  #     y_vec = matsbyname::vectorize_byname(a = .data[[Y]], notation = list(RCLabels::arrow_notation)),
  #     # Total use of product p in machine m across all EIOU industries aggregated
  #     Alloc_mat_EIOU = matsbyname::matrixproduct_byname(matsbyname::hatize_byname(eiou_vec, keep = "rownames"),
  #                                                       .data[[C_EIOU]]) |>
  #       matsbyname::aggregate_pieces_byname(piece = "noun",
  #                                           margin = 1,
  #                                           notation = list(RCLabels::arrow_notation)),
  #     # Total use of product p in final demand sector s across all final demand sectors aggregated
  #     Alloc_mat_Y = matsbyname::matrixproduct_byname(matsbyname::hatize_byname(y_vec, keep = "rownames"),
  #                                                    .data[[C_Y]]) |>
  #       matsbyname::aggregate_pieces_byname(piece = "noun",
  #                                           margin = 1,
  #                                           notation = list(RCLabels::arrow_notation)),
  #     # Total use of product p in machine m Y and EIOU aggregated
  #     Alloc_mat_EIOU_Y = matsbyname::sum_byname(Alloc_mat_EIOU, Alloc_mat_Y),
  #     # Total use of product p in EIOU industries
  #     f_EIOU = matsbyname::rowsums_byname(Alloc_mat_EIOU),
  #     # Total use of product p in Y
  #     f_Y = matsbyname::rowsums_byname(Alloc_mat_Y),
  #     # Total use of product p in EIOU and Y
  #     f_EIOU_Y = matsbyname::rowsums_byname(Alloc_mat_EIOU_Y),
  #     # Share of product p used in each machine m across EIOU. Sum by product yields 1.
  #     "{C_EIOU_agg}" := matsbyname::matrixproduct_byname(matsbyname::hatinv_byname(f_EIOU, keep = "rownames"),
  #                                                        Alloc_mat_EIOU),
  #     # Share of product p used in each machine m across final demand. Sum by product yields 1.
  #     "{C_Y_agg}" := matsbyname::matrixproduct_byname(matsbyname::hatinv_byname(f_Y, keep = "rownames"),
  #                                                     Alloc_mat_Y),
  #     # Share of product p used in each machine m across final demand and EIOU. Sum by product yields 1.
  #     "{C_EIOU_Y_agg}" := matsbyname::matrixproduct_byname(matsbyname::hatinv_byname(f_EIOU_Y, keep = "rownames"),
  #                                                          Alloc_mat_EIOU_Y),
  #   ) |>
  #   dplyr::select(tidyselect::any_of(c(country, method, energy_type, last_stage, year, C_EIOU_agg, C_Y_agg, C_EIOU_Y_agg)))
  #
  # return(C_mats_agg)

  # Previous code did not correctly account
  # for possible missing matrices.
  # The code below does, as of 22 April 2024.
  # If this works, code above can be deleted
  # after, say, July 2024.

  out <- C_mats_agg <- dplyr::left_join(C_mats, psut_iea,
                                        by = c({country}, {method}, {energy_type}, {last_stage}, {year})) |>
    dplyr::select(tidyselect::any_of(c(country, method, energy_type, last_stage, year, C_EIOU, C_Y, Y, U_EIOU)))

  if (C_EIOU %in% colnames(out)) {
    out <- out |>
      dplyr::mutate(
        # Total use of product p industry EIOU industry i, as a vector. U_EIOU vectorised.
        eiou_vec = matsbyname::vectorize_byname(a = .data[[U_EIOU]], notation = list(RCLabels::arrow_notation)),
        # Total use of product p in machine m across all EIOU industries aggregated
        Alloc_mat_EIOU = matsbyname::matrixproduct_byname(matsbyname::hatize_byname(eiou_vec, keep = "rownames"),
                                                          .data[[C_EIOU]]) |>
          matsbyname::aggregate_pieces_byname(piece = "noun",
                                              margin = 1,
                                              notation = list(RCLabels::arrow_notation)),
        # Total use of product p in EIOU industries
        f_EIOU = matsbyname::rowsums_byname(Alloc_mat_EIOU),
        # Share of product p used in each machine m across EIOU. Sum by product yields 1.
        "{C_EIOU_agg}" := matsbyname::matrixproduct_byname(matsbyname::hatinv_byname(f_EIOU, keep = "rownames"),
                                                           Alloc_mat_EIOU)
      )
  }
  if (C_Y %in% colnames(out)) {
    # Do C_Y
    out <- out |>
      dplyr::mutate(
        # Total use of product p in final demand sector s, as a vector. Y vectorised.
        y_vec = matsbyname::vectorize_byname(a = .data[[Y]], notation = list(RCLabels::arrow_notation)),
        # Total use of product p in final demand sector s across all final demand sectors aggregated
        Alloc_mat_Y = matsbyname::matrixproduct_byname(matsbyname::hatize_byname(y_vec, keep = "rownames"),
                                                       .data[[C_Y]]) |>
          matsbyname::aggregate_pieces_byname(piece = "noun",
                                              margin = 1,
                                              notation = list(RCLabels::arrow_notation)),
        # Total use of product p in Y
        f_Y = matsbyname::rowsums_byname(Alloc_mat_Y),
        # Share of product p used in each machine m across final demand. Sum by product yields 1.
        "{C_Y_agg}" := matsbyname::matrixproduct_byname(matsbyname::hatinv_byname(f_Y, keep = "rownames"),
                                                        Alloc_mat_Y)
      )
  }

  # Work on the combined result
  if ((C_EIOU %in% colnames(out)) & !(C_Y %in% colnames(out))) {
    # Only C_EIOU is present
    out <- out |>
      dplyr::mutate(
        Alloc_mat_EIOU_Y = Alloc_mat_EIOU
      )
  } else if (!(C_EIOU %in% colnames(out)) & (C_Y %in% colnames(out))) {
    # Only C_Y is present
    out <- out |>
      dplyr::mutate(
        Alloc_mat_EIOU_Y = Alloc_mat_Y
      )
  } else if ((C_EIOU %in% colnames(out)) & (C_Y %in% colnames(out))) {
    # Both C_EIOU and C_Y are present
    # Total use of product p in machine m Y and EIOU aggregated
    out <- out |>
      dplyr::mutate(
        Alloc_mat_EIOU_Y = matsbyname::sum_byname(Alloc_mat_EIOU, Alloc_mat_Y)
      )
  }

  # Now operate on the combined EIOU and Y information
  out <- out |>
    dplyr::mutate(
      # Total use of product p in EIOU and Y
      f_EIOU_Y = matsbyname::rowsums_byname(Alloc_mat_EIOU_Y),
      # Share of product p used in each machine m across final demand and EIOU. Sum by product yields 1.
      "{C_EIOU_Y_agg}" := matsbyname::matrixproduct_byname(matsbyname::hatinv_byname(f_EIOU_Y, keep = "rownames"),
                                                           Alloc_mat_EIOU_Y)
    )

  # Clean out unneeded columns and return
  out |>
    dplyr::select(tidyselect::any_of(c(country, method, energy_type, last_stage, year, C_EIOU_agg, C_Y_agg, C_EIOU_Y_agg)))
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
#' @param dataset_colname,valid_from_version,valid_to_version See `PFUPipelineTools::dataset_info`.
#' @param etafu_colname The name of the etafu column.
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
                                        dataset_colname = PFUPipelineTools::dataset_info$dataset_colname,
                                        valid_from_version = PFUPipelineTools::dataset_info$valid_from_version_colname,
                                        valid_to_version = PFUPipelineTools::dataset_info$valid_to_version_colname,
                                        etafu_colname = "etafu") {

  if (is.null(C_mats) & is.null(eta_m_vecs) & is.null(phi_vecs)) {
    # Nothing to be done.
    return(NULL)
  }

  # Check the dataset for C_mats
  ds_C_mats <- C_mats[[dataset_colname]] |>
    unique()
  if (length(ds_C_mats) != 1) {
    stop(paste0("Need only 1 dataset in calc_fu_Y_EIOU_efficiencies(). Found ", length(ds_C_mats), "."))
  }

  # Make one large data frame.
  C_mats |>
  dplyr::full_join(eta_m_vecs |>
                     dplyr::mutate(
                       # Set the dataset to be the same as the dataset of the incoming C_mats
                       # data frame.
                       # eta_m_vecs will likely have a different (and less-specific) dataset.
                       "{dataset_colname}" := ds_C_mats
                     ),
                   by = c(country, last_stage, energy_type, method, year,
                          dataset_colname, valid_from_version, valid_to_version)) |>
    dplyr::full_join(phi_vecs |>
                       dplyr::mutate(
                         # Set the dataset to be the same as the dataset of the incoming psut_final
                         # data frame.
                         # eta_phi_vecs will likely have a different (and less-specific) dataset.
                         "{dataset_colname}" := ds_C_mats
                       ),
                     by = c(country, year,
                                      dataset_colname, valid_from_version, valid_to_version)) |>
    # Run the calculations
    Recca::calc_eta_fu_Y_eiou(eta_i = etafu_colname)
}


#' Calculate machine efficiencies
#'
#' Calculate machine efficiencies from RUVY matrices
#' by calling [Recca::calc_eta_i()] and
#' deleting some columns.
#'
#' @param .psut A data frame of PSUT matrices.
#' @param countries The countries for which this analysis should be conducted.
#' @param R_col,U_col,U_feed_col,U_eiou_col,r_eiou_col,V_col,Y_col,S_units_col See [Recca::psut_cols].
#'
#' @return A data frame with machine efficiencies.
#'
#' @export
calc_eta_i <- function(.psut,
                       countries,
                       R_col = Recca::psut_cols$R,
                       U_col = Recca::psut_cols$U,
                       U_feed_col = Recca::psut_cols$U_feed,
                       U_eiou_col = Recca::psut_cols$U_eiou,
                       r_eiou_col = Recca::psut_cols$r_eiou,
                       V_col = Recca::psut_cols$V,
                       Y_col = Recca::psut_cols$Y,
                       S_units_col = Recca::psut_cols$S_units) {

  if (is.null(.psut)) {
    # Nothing to be done.
    return(NULL)
  }

  .psut |>
    Recca::calc_eta_i() |>
    dplyr::mutate(
      "{R_col}" := NULL,
      "{U_col}" := NULL,
      "{U_feed_col}" := NULL,
      "{U_eiou_col}" := NULL,
      "{r_eiou_col}" := NULL,
      "{V_col}" := NULL,
      "{Y_col}" := NULL,
      "{S_units_col}" := NULL
    )
}


#' Calculate primary, final, and useful aggregates and associated efficiencies
#'
#' Routes to [Recca::calc_agg_eta_pfus()].
#'
#' @param .psut_df The data frame for which efficiencies should be calculated.
#' @param p_industries Primary industries.
#' @param fd_sectors Final demand sectors.
#' @param countries The countries for this analysis.
#'
#' @return A data frame of aggregations and efficiencies.
#'
#' @export
calc_agg_eta_pfu <- function(.psut_df,
                             p_industries,
                             fd_sectors,
                             countries) {

  if (is.null(.psut_df)) {
    # Nothing to do
    return(NULL)
  }

  .psut_df |>
    Recca::calc_agg_eta_pfus(p_industries = p_industries,
                             fd_sectors = fd_sectors)
}
