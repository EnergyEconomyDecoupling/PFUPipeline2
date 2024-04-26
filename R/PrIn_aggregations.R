#' Delete original and rename aggregated columns
#'
#' This is a helpful function that deletes original PSUT columns and
#' renames the aggregated columns to the original names.
#'
#' Columns with names given by `R`, `U`, `V`, `Y`, `r_eiou`, `U_eiou`, `U_feed`, and `S_units` arguments.
#' The suffix is given by the `suffix` argument.
#'
#' This function is a helper function and, thus, is not public.
#'
#' @param .psut_data A data frame of PSUT matrices.
#' @param R,U,V,Y,r_eiou,U_eiou,U_feed,S_units The names of input columns in `.psut_data`.
#' @param suffix The suffix for columns of aggregated matrices.
#'               Default is `Recca::aggregate_cols$aggregated_suffix`.
#'
#' @return A version of `.psut_data` with original PSUT columns deleted and suffixed columns renamed to original names.
rename_suffixed_psut_columns <- function(.psut_data,
                                         suffix = Recca::aggregate_cols$aggregated_suffix,
                                         R = Recca::psut_cols$R,
                                         U = Recca::psut_cols$U,
                                         V = Recca::psut_cols$V,
                                         Y = Recca::psut_cols$Y,
                                         r_eiou = Recca::psut_cols$r_eiou,
                                         U_eiou = Recca::psut_cols$U_eiou,
                                         U_feed = Recca::psut_cols$U_feed,
                                         S_units = Recca::psut_cols$S_units) {
  .psut_data %>%
    # Delete the original columns
    dplyr::mutate(
      "{R}" := NULL,
      "{U}" := NULL,
      "{V}" := NULL,
      "{Y}" := NULL,
      "{r_eiou}" := NULL,
      "{U_eiou}" := NULL,
      "{U_feed}" := NULL,
      "{S_units}" := NULL
    ) %>%
    # Rename the new columns to the original column names
    dplyr::rename(
      "{R}" := paste0(R, suffix),
      "{U}" := paste0(U, suffix),
      "{V}" := paste0(V, suffix),
      "{Y}" := paste0(Y, suffix),
      "{r_eiou}" := paste0(r_eiou, suffix),
      "{U_eiou}" := paste0(U_eiou, suffix),
      "{U_feed}" := paste0(U_feed, suffix),
      "{S_units}" := paste0(S_units, suffix)
    )
}


#' Bundle several aggregation calculations together
#'
#' The efficiency pipeline calculates efficiencies
#' for various despecifications, row aggregations, and column aggregations.
#'
#' This function is an attempt to streamline the calculation pipeline
#' by eliminating the need to repeatedly re-load intermediate targets from disk.
#' It bundles the work of previous targets to
#'
#' - despecify and aggregate both product and industry dimensions of PSUT matrices,
#' - group and aggregate products,
#' - group and aggregate industries,
#' - group and aggregate both products and industries,
#' - calculate primary-to-final efficiencies,
#' - calculate primary-to-useful efficiencies, and
#' - calculate final-to-useful efficiencies.
#'
#'
#' @param .psut_data PSUT matrices in wide-by-matrix format.
#'                   This could be an entire data frame,
#'                   a slice (row) of the data frame, or
#'                   a group of the data frame.
#' @param product_agg_map The product aggregation map.
#' @param industry_agg_map The industry aggregation map.
#' @param p_industries A string vector of primary industries.
#' @param countries,years The countries and years to analyze.
#' @param do_chops A boolean that tells whether to do the chopping of **R** and **Y** matrices.
#' @param pattern_type The matching type for row and column labels.
#'                     Default is "exact".
#' @param piece The piece of row and column labels to be matched.
#'              Default is "noun".
#' @param bracket_notation A row and column notation.
#'                         Default is `RCLabels::bracket_notation`.
#' @param arrow_notation A row and column notation.
#'                       Default is `RCLabels::arrow_notation`.
#' @param prepositions Prepositions to be used in row and column labels.
#'                     Default is `RCLabels::prepositions_list`.
#' @param method The method for doing matrix inversion when chopping the **R** and **Y** matrices.
#'               Default is "SVD" for singular value decomposition.
#' @param tol_invert The tolerance for nearness to 0 in matrix inversion.
#'                   Default is `.Machine$double.eps`.
#' @param product_type,industry_type See `Recca::row_col_types`.
#' @param R,U,U_feed,U_eiou,r_eiou,V,Y,S_units,country,year The names of input columns in `.psut_data`.
#'                                             Default values are from `Recca::psut_cols`.
#' @param .prime The suffix for the columns containing chopped ECC matrices.
#'               Default is "_prime".
#' @param aggregated_suffix The suffix for aggregated column names.
#'                          See `Recca::aggregate_cols`.
#' @param R_aggregated_colname,U_aggregated_colname,V_aggregated_colname,Y_aggregated_colname,r_eiou_aggregated_colname,U_eiou_aggregated_colname,U_feed_aggregated_colname,S_units_aggregated_colname The names of output aggregated columns.
#'                          Defaults are the matrix names with `aggregated_suffix` appended.
#' @param product_aggregation,industry_aggregation,specified,despecified,grouped,product_sector See `PFUAggPipeline::aggregation_df_cols`.
#' @param chopped_mat,chopped_var Column names that indicate which matrix has been chopped, **R** or **Y**.
#'                                Default values are from `PFUAggPipeline::aggregation_df_cols`.
#' @param Y_matname,R_matname Matrix names for the `chopped_mat` and `chopped_var` columns.
#'                            Default values are from `Recca::psut_cols$R` and `Recca::psut_cols$Y`.
#' @param none The string to specify no aggregations.
#'             Default is `PFUAggPipeline::agg_metadata$none`.
#'
#' @return A data frame of efficiencies for the original, despecified, and grouped versions
#'         of `.psut_data`.
#'
#' @export
pr_in_agg_pipeline <- function(.psut_data,
                               product_agg_map,
                               industry_agg_map,
                               p_industries,
                               countries,
                               years,
                               do_chops = FALSE,
                               pattern_type = "exact",
                               piece = "noun",
                               bracket_notation = RCLabels::bracket_notation,
                               arrow_notation = RCLabels::arrow_notation,
                               prepositions = RCLabels::prepositions_list,
                               method = "SVD",
                               tol_invert = .Machine$double.eps,
                               # Row and column types
                               product_type = Recca::row_col_types$product_type,
                               industry_type = Recca::row_col_types$industry_type,
                               # Names of original matrices in .psut_data
                               R = Recca::psut_cols$R,
                               U = Recca::psut_cols$U,
                               V = Recca::psut_cols$V,
                               Y = Recca::psut_cols$Y,
                               r_eiou = Recca::psut_cols$r_eiou,
                               U_eiou = Recca::psut_cols$U_eiou,
                               U_feed = Recca::psut_cols$U_feed,
                               S_units = Recca::psut_cols$S_units,
                               .prime = "_prime",
                               # Country and year columns
                               country = Recca::psut_cols$country,
                               year = Recca::psut_cols$year,
                               # Names for the aggregated matrices after aggregation
                               R_aggregated_colname = paste0(R, aggregated_suffix),
                               U_aggregated_colname = paste0(U, aggregated_suffix),
                               U_feed_aggregated_colname = paste0(U_feed, aggregated_suffix),
                               U_eiou_aggregated_colname = paste0(U_eiou, aggregated_suffix),
                               r_eiou_aggregated_colname = paste0(r_eiou, aggregated_suffix),
                               V_aggregated_colname = paste0(V, aggregated_suffix),
                               Y_aggregated_colname = paste0(Y, aggregated_suffix),
                               S_units_aggregated_colname = paste0(S_units, aggregated_suffix),
                               # The suffix added to the name of the columns of aggregated matrices
                               aggregated_suffix = Recca::aggregate_cols$aggregated_suffix,
                               # Some identifying strings
                               product_aggregation = PFUPipelineTools::aggregation_df_cols$product_aggregation,
                               industry_aggregation = PFUPipelineTools::aggregation_df_cols$industry_aggregation,
                               specified = PFUPipelineTools::aggregation_df_cols$specified,
                               despecified = PFUPipelineTools::aggregation_df_cols$despecified,
                               grouped = PFUPipelineTools::aggregation_df_cols$grouped,
                               # Strings for chopping
                               chopped_mat = PFUPipelineTools::aggregation_df_cols$chopped_mat,
                               chopped_var = PFUPipelineTools::aggregation_df_cols$chopped_var,
                               Y_matname = Recca::psut_cols$Y,
                               R_matname = Recca::psut_cols$R,
                               product_sector = PFUPipelineTools::aggregation_df_cols$product_sector,
                               none = PFUPipelineTools::agg_metadata$none) {

  browser()

  if (is.null(.psut_data)) {
    # Nothing to be done
    return(NULL)
  }

  if (nrow(.psut_data) == 0) {
    # Nothing to be done
    return(NULL)
  }

  # Grab the names of the R and Y columns here,
  # because referring to R and Y in the context of
  # dplyr::mutate() below will grab the
  # column itself,
  # rather than the name of the column.
  R_colname <- R
  Y_colname <- Y

  # Chop the R and Y matrices, if desired
  if (do_chops) {
    PSUT_chop_R <- .psut_data |>
      Recca::chop_R(calc_pfd_aggs = FALSE,
                    piece = "noun",
                    notation = bracket_notation,
                    pattern_type = "literal",
                    unnest = TRUE,
                    method = method,
                    tol_invert = tol_invert,
                    R = R, U = U, V = V, Y = Y, U_feed = U_feed, S_units = S_units,
                    product_sector = product_sector,
                    .prime = .prime) |>
      # Eliminate the original matrices and rename the "_prime" column names.
      # rename_prime_cols() |>
      rename_suffixed_psut_columns(suffix = .prime,
                                   R = R, U = U, V = V, Y = Y,
                                   r_eiou = r_eiou, U_eiou = U_eiou, U_feed = U_feed, S_units = S_units) |>
      dplyr::mutate(
        "{chopped_mat}" := R_colname,
        "{chopped_var}" := .data[[product_sector]],
        "{product_sector}" := NULL
      )
    PSUT_chop_Y <- .psut_data |>
      Recca::chop_Y(calc_pfd_aggs = FALSE,
                    piece = "noun",
                    notation = bracket_notation,
                    pattern_type = "literal",
                    unnest = TRUE,
                    method = method,
                    tol_invert = tol_invert,
                    R = R, U = U, V = V, Y = Y, U_feed = U_feed, S_units = S_units,
                    product_sector = product_sector,
                    .prime = .prime) |>
      # Eliminate the original matrices and rename the "_prime" column names.
      # rename_prime_cols() |>
      rename_suffixed_psut_columns(suffix = .prime,
                                   R = R, U = U, V = V, Y = Y,
                                   r_eiou = r_eiou, U_eiou = U_eiou, U_feed = U_feed, S_units = S_units) |>
      dplyr::mutate(
        "{chopped_mat}" := Y_colname,
        "{chopped_var}" := .data[[product_sector]],
        "{product_sector}" := NULL
      )
  } else {
    PSUT_chop_R <- NULL
    PSUT_chop_Y <- NULL
  }

  # Bind all chopped ECCs together with the original data
  # for later processing.
  PSUT_chop_all <- dplyr::bind_rows(.psut_data |>
                                      dplyr::mutate(
                                        "{chopped_mat}" := none,
                                        "{chopped_var}" := none
                                      ),
                                    PSUT_chop_R ,
                                    PSUT_chop_Y)

  # Despecify and aggregate both Product and Industry dimensions
  PSUT_Chop_all_Ds_InPr <- PSUT_chop_all |>
    Recca::despecified_aggregates(notation = list(bracket_notation, arrow_notation),
                                  R = R, U = U, V = V, Y = Y,
                                  r_eiou = r_eiou, U_eiou = U_eiou, U_feed = U_feed,
                                  S_units = S_units,
                                  R_aggregated_colname = R_aggregated_colname,
                                  U_aggregated_colname = U_aggregated_colname,
                                  V_aggregated_colname = V_aggregated_colname,
                                  Y_aggregated_colname = Y_aggregated_colname,
                                  r_eiou_aggregated_colname = r_eiou_aggregated_colname,
                                  U_eiou_aggregated_colname = U_eiou_aggregated_colname,
                                  U_feed_aggregated_colname = U_feed_aggregated_colname,
                                  S_units_aggregated_colname = S_units_aggregated_colname,
                                  aggregated_suffix = aggregated_suffix) |>
    rename_suffixed_psut_columns(suffix = aggregated_suffix,
                                 R = R, U = U, V = V, Y = Y,
                                 r_eiou = r_eiou, U_eiou = U_eiou, U_feed = U_feed, S_units = S_units) |>
    dplyr::mutate(
      "{product_aggregation}" := despecified,
      "{industry_aggregation}" := despecified
    )

  # Group on Product dimension
  PSUT_Chop_all_Ds_InPr_Gr_Pr <- PSUT_Chop_all_Ds_InPr |>
    Recca::grouped_aggregates(aggregation_map = product_agg_map, margin = product_type, pattern_type = pattern_type,
                              R = R, U = U, V = V, Y = Y, r_eiou = r_eiou, U_eiou = U_eiou, U_feed = U_feed, S_units = S_units,
                              R_aggregated_colname = R_aggregated_colname,
                              U_aggregated_colname = U_aggregated_colname,
                              V_aggregated_colname = V_aggregated_colname,
                              Y_aggregated_colname = Y_aggregated_colname,
                              r_eiou_aggregated_colname = r_eiou_aggregated_colname,
                              U_eiou_aggregated_colname = U_eiou_aggregated_colname,
                              U_feed_aggregated_colname = U_feed_aggregated_colname,
                              S_units_aggregated_colname = S_units_aggregated_colname) |>
    rename_suffixed_psut_columns(suffix = aggregated_suffix,
                                 R = R, U = U, V = V, Y = Y,
                                 r_eiou = r_eiou, U_eiou = U_eiou, U_feed = U_feed, S_units = S_units) |>
    dplyr::mutate(
      "{product_aggregation}" := grouped,
      "{industry_aggregation}" := despecified
    )

  # Group on Industry dimension
  PSUT_Chop_all_Ds_InPr_Gr_In <- PSUT_Chop_all_Ds_InPr |>
    Recca::grouped_aggregates(aggregation_map = industry_agg_map, margin = industry_type, pattern_type = pattern_type,
                              R = R, U = U, V = V, Y = Y, r_eiou = r_eiou, U_eiou = U_eiou, U_feed = U_feed, S_units = S_units,
                              R_aggregated_colname = R_aggregated_colname,
                              U_aggregated_colname = U_aggregated_colname,
                              V_aggregated_colname = V_aggregated_colname,
                              Y_aggregated_colname = Y_aggregated_colname,
                              r_eiou_aggregated_colname = r_eiou_aggregated_colname,
                              U_eiou_aggregated_colname = U_eiou_aggregated_colname,
                              U_feed_aggregated_colname = U_feed_aggregated_colname,
                              S_units_aggregated_colname = S_units_aggregated_colname) |>
    rename_suffixed_psut_columns(suffix = aggregated_suffix,
                                 R = R, U = U, V = V, Y = Y,
                                 r_eiou = r_eiou, U_eiou = U_eiou, U_feed = U_feed, S_units = S_units) |>
    dplyr::mutate(
      "{product_aggregation}" := despecified,
      "{industry_aggregation}" := grouped
    )

  # Group on Product and Industry dimensions
  PSUT_Chop_all_Ds_InPr_Gr_PrIn <- PSUT_Chop_all_Ds_InPr |>
    Recca::grouped_aggregates(aggregation_map = c(product_agg_map, industry_agg_map),
                              margin = c(product_type, industry_type),
                              pattern_type = pattern_type,
                              R = R, U = U, V = V, Y = Y, r_eiou = r_eiou, U_eiou = U_eiou, U_feed = U_feed, S_units = S_units,
                              R_aggregated_colname = R_aggregated_colname,
                              U_aggregated_colname = U_aggregated_colname,
                              V_aggregated_colname = V_aggregated_colname,
                              Y_aggregated_colname = Y_aggregated_colname,
                              r_eiou_aggregated_colname = r_eiou_aggregated_colname,
                              U_eiou_aggregated_colname = U_eiou_aggregated_colname,
                              U_feed_aggregated_colname = U_feed_aggregated_colname,
                              S_units_aggregated_colname = S_units_aggregated_colname) |>
    rename_suffixed_psut_columns(suffix = aggregated_suffix,
                                 R = R, U = U, V = V, Y = Y,
                                 r_eiou = r_eiou, U_eiou = U_eiou, U_feed = U_feed, S_units = S_units) |>
    dplyr::mutate(
      "{product_aggregation}" := grouped,
      "{industry_aggregation}" := grouped
    )

  # Stack all the despecifications and groupings together
  dplyr::bind_rows(PSUT_chop_all |>
                     dplyr::mutate(
                       "{product_aggregation}" := specified,
                       "{industry_aggregation}" := specified,
                     ),
                   PSUT_Chop_all_Ds_InPr,
                   PSUT_Chop_all_Ds_InPr_Gr_Pr,
                   PSUT_Chop_all_Ds_InPr_Gr_In,
                   PSUT_Chop_all_Ds_InPr_Gr_PrIn) |>
    dplyr::relocate(dplyr::all_of(c(chopped_mat, chopped_var, product_aggregation, industry_aggregation)), .before = dplyr::all_of(R))
}
