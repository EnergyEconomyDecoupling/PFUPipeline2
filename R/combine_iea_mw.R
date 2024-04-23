
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
#' @param exemplar_table The exemplar table.
#' @param country,year,unit,e_dot See `IEATools::iea_cols`.
#' @param agg_code_col,region_code,exemplar_country See `PFUPipelineTools::exemplar_names`.
#' @param species,stage_col,sector_col See `MWTools::mw_constants`.
#' @param dataset The name of the dataset column.
#'                Default is `PFUPipelineTools::dataset_info$dataset_colname`.
#'
#' @export
aggcountries_mw_to_iea <- function(mw_df,
                                   exemplar_table,
                                   country = IEATools::iea_cols$country,
                                   year = IEATools::iea_cols$year,
                                   unit = IEATools::iea_cols$unit,
                                   e_dot = IEATools::iea_cols$e_dot,
                                   agg_code_col = PFUPipelineTools::exemplar_names$agg_code_col,
                                   region_code = PFUPipelineTools::exemplar_names$region_code,
                                   exemplar_country = PFUPipelineTools::exemplar_names$exemplar_country,
                                   species = MWTools::mw_constants$species,
                                   stage_col = MWTools::mw_constants$stage_col,
                                   sector_col = MWTools::mw_constants$sector_col) {

  if (is.null(mw_df)) {
    # Nothing to be done.
    return(NULL)
  }

  focused_exemplar_table <- exemplar_table |>
    dplyr::select(-dplyr::all_of(c(region_code, exemplar_country))) |>
    tidyr::pivot_longer(cols = -dplyr::all_of(c(country)),
                        names_to = year,
                        values_to = agg_code_col) |>
    dplyr::mutate(
      "{year}" := as.numeric(.data[[year]])
    )

  agg_mw_df <- mw_df |>
    dplyr::left_join(focused_exemplar_table, by = c(country, year)) |>
    dplyr::select(-dplyr::all_of(country)) |>
    dplyr::group_by(.data[[year]],
                    .data[[species]],
                    .data[[stage_col]],
                    .data[[sector_col]],
                    .data[[unit]],
                    .data[[agg_code_col]]) |>
    dplyr::summarise("{e_dot}" := sum(.data[[e_dot]]),
                     .groups = "drop") |>
    dplyr::rename("{country}" := !!agg_code_col)
}


#' Sum IEA and muscle work ECC matrices
#'
#' To create a combined energy conversion chain (ECC)
#' containing both IEA and muscle work data,
#' PSUT matrices for each ECC are summed.
#' This function sums `R`, `U`, `V`, `Y`, `U_feed`, and `U_eiou` matrices directly.
#' It also re-calculates the `r_eiou` matrix.
#'
#' If either of `iea_psut` or `.mw_psut` are `NULL`,
#' the other is returned.
#'
#' @param .iea_psut An IEA PSUT data frame.
#' @param .mw_psut A muscle work PSUT data frame.
#' @param countries The countries to be analyzed.
#' @param iea_suffix A suffix for IEA columns, used internally.
#'                   Default is "_iea".
#' @param mw_suffix A suffix for muscle work columns, used internally.
#'                  Default is "_mw".
#' @param R The name of the column of `R` matrices. Default is `IEATools::psut_cols$R`.
#' @param U The name of the column of `U` matrices. Default is `IEATools::psut_cols$U`.
#' @param V The name of the column of `V` matrices. Default is `IEATools::psut_cols$V`.
#' @param Y The name of the column of `Y` matrices. Default is `IEATools::psut_cols$Y`.
#' @param U_feed The name of the column of `U_feed` matrices. Default is `IEATools::psut_cols$U_feed`.
#' @param U_eiou The name of the column of `U_eiou` matrices. Default is `IEATools::psut_cols$U_eiou`.
#' @param s_units The name of the column of `s_units` matrices. Default is `IEATools::psut_cols$s_units`.
#' @param country The name of the country column. Default is `IEATools$iea_cols$country`.
#' @param year The name of the year column. Default is `IEATools$iea_cols$year`.
#' @param method The name of the method column. Default is `IEATools$iea_cols$method`.
#' @param energy_type The name of the energy type column. Default is `IEATools$iea_cols$energy_type`.
#' @param last_stage The name of the last_stage column. Default is `IEATools$iea_cols$last_stage`.
#' @param r_eiou The name of the r_eiou column. Default is `IEATools$iea_cols$r_eiou`.
#'
#' @return A data frame of summed matrices.
#'
#' @export
add_iea_mw_psut <- function(.iea_psut = NULL,
                            .mw_psut = NULL,
                            countries,
                            iea_suffix = "_iea",
                            mw_suffix = "_mw",
                            # Input columns
                            R = IEATools::psut_cols$R,
                            U = IEATools::psut_cols$U,
                            V = IEATools::psut_cols$V,
                            Y = IEATools::psut_cols$Y,
                            U_feed = IEATools::psut_cols$U_feed,
                            U_eiou = IEATools::psut_cols$U_eiou,
                            s_units = IEATools::psut_cols$s_units,
                            # Metadata column names
                            country = IEATools::iea_cols$country,
                            year = IEATools::iea_cols$year,
                            method = IEATools::iea_cols$method,
                            energy_type = IEATools::iea_cols$energy_type,
                            last_stage = IEATools::iea_cols$last_stage,
                            # Output column names
                            r_eiou = IEATools::psut_cols$r_eiou) {

  if (is.null(.mw_psut)) {
    return(.iea_psut)
  }
  if (is.null(.iea_psut)) {
    return(.mw_psut)
  }
  # Define new column names.
  R_iea <- paste0(R, iea_suffix)
  U_iea <- paste0(U, iea_suffix)
  U_feed_iea <- paste0(U_feed, iea_suffix)
  U_eiou_iea <- paste0(U_eiou, iea_suffix)
  V_iea <- paste0(V, iea_suffix)
  Y_iea <- paste0(Y, iea_suffix)
  s_units_iea <- paste0(s_units, iea_suffix)
  R_mw <- paste0(R, mw_suffix)
  U_mw <- paste0(U, mw_suffix)
  V_mw <- paste0(V, mw_suffix)
  Y_mw <- paste0(Y, mw_suffix)
  U_feed_mw <- paste0(U_feed, mw_suffix)
  U_eiou_mw <- paste0(U_eiou, mw_suffix)
  s_units_mw <- paste0(s_units, mw_suffix)

  # Rename columns and delete the r_eiou column, as we will recalculate later.
  iea_specific <- .iea_psut %>%
    dplyr::rename(
      "{R_iea}" := dplyr::all_of(R),
      "{U_iea}" := dplyr::all_of(U),
      "{V_iea}" := dplyr::all_of(V),
      "{Y_iea}" := dplyr::all_of(Y),
      "{U_feed_iea}" := dplyr::all_of(U_feed),
      "{U_eiou_iea}" := dplyr::all_of(U_eiou),
      "{s_units_iea}" := dplyr::all_of(s_units)
    ) %>%
    dplyr::mutate(
      "{r_eiou}" := NULL
    )
  mw_specific <- .mw_psut %>%
    dplyr::rename(
      "{R_mw}" := dplyr::all_of(R),
      "{U_mw}" := dplyr::all_of(U),
      "{V_mw}" := dplyr::all_of(V),
      "{Y_mw}" := dplyr::all_of(Y),
      "{U_feed_mw}" := dplyr::all_of(U_feed),
      "{U_eiou_mw}" := dplyr::all_of(U_eiou),
      "{s_units_mw}" := dplyr::all_of(s_units)
    ) %>%
    dplyr::mutate(
      "{r_eiou}" := NULL
    )

  # Join the data frames.
  joined <- dplyr::full_join(iea_specific, mw_specific,
                             by = c(country, year, method, energy_type, last_stage))
  if (nrow(joined) == 0) {
    # We zero-row data frames.
    # Make the columns, but don't do the math (which fails)
    out <- joined |>
      dplyr::mutate(
        "{R}" := list(),
        "{U}" := list(),
        "{V}" := list(),
        "{Y}" := list(),
        "{U_feed}" := list(),
        "{U_eiou}" := list(),
        "{s_units}" := list(),
        "{r_eiou}" := list()
      )
  } else {
    out <- joined |>
      dplyr::mutate(
        # Calculate new columns by summing matrices
        "{R}" := matsbyname::sum_byname(.data[[R_iea]], .data[[R_mw]]),
        "{U}" := matsbyname::sum_byname(.data[[U_iea]], .data[[U_mw]]),
        "{V}" := matsbyname::sum_byname(.data[[V_iea]], .data[[V_mw]]),
        "{Y}" := matsbyname::sum_byname(.data[[Y_iea]], .data[[Y_mw]]),
        "{U_feed}" := matsbyname::sum_byname(.data[[U_feed_iea]], .data[[U_feed_mw]]),
        "{U_eiou}" := matsbyname::sum_byname(.data[[U_eiou_iea]], .data[[U_eiou_mw]]),
        "{s_units}" := matsbyname::sum_byname(.data[[s_units_iea]], .data[[s_units_mw]]),
        "{r_eiou}" := matsbyname::quotient_byname(.data[[U_eiou]], .data[[U]]) |>
          # For cases where U is 0, will get 0/0 = NaN.  Convert to zero.
          matsbyname::replaceNaN_byname(val = 0),
      )
  }
  # Get rid of unneeded columns and return
  out |>
    dplyr::mutate(
      "{R_iea}" := NULL,
      "{R_mw}" := NULL,
      "{U_iea}" := NULL,
      "{U_mw}" := NULL,
      "{V_iea}" := NULL,
      "{V_mw}" := NULL,
      "{Y_iea}" := NULL,
      "{Y_mw}" := NULL,
      "{U_feed_iea}" := NULL,
      "{U_feed_mw}" := NULL,
      "{U_eiou_iea}" := NULL,
      "{U_eiou_mw}" := NULL,
      "{s_units_iea}" := NULL,
      "{s_units_mw}" := NULL
    )
}


#' Build the final PSUT data frame
#'
#' Combines PSUT descriptions based on IEA data exclusively, muscle work data exclusively,
#' and summed IEA and MW data.
#'
#' @param psutiea A PSUT data frame of IEA data. Default is `NULL.`
#' @param psutmw A PSUT data frame of muscle work data. Default is `NULL.`
#' @param psutieamw A PSUT data frame of combined IEA and MW data. Default is `NULL.`
#' @param countries The countries to be calculated.
#' @param country_colname,method_colname,energy_type_colname,last_stage_colname,year_colname Column names.
#'                                                                                           See `IEATools::iea_cols` for defaults.
#' @param ieamw_colname The name of the column that identifies whether data are for the IEA,
#'                      muscle work (MW) or both.
#'                      Default is [PFUPipelineTools::ieamw_cols$ieamw].
#' @param R_colname,U_colname,U_feed_colname,U_eiou_colname,r_eiou_colname,V_colname,Y_colname,S_units_colname Names of matrix columns.
#'                                                                                                             See `IEATools::psut_cols`.
#' @param iea The string that identifies ECC data are from the IEA only.
#'            Default is [PFUPipelineTools::ieamw_cols$iea].
#' @param mw The string that identifies ECC data are for muscle work only.
#'           Default is [PFUPipelineTools::ieamw_cols$mw].
#' @param both The string that identifies ECC data are for both IEA and muscle work.
#'             Default is [PFUPipelineTools::ieamw_cols$both].
#' @return A data frame with `PSUTIEA`, `PSUTMW`, and `PSUTIEAMW` `rbind()`ed together,
#'         and a new column (`IEAMW_colname`) that distinguishes among them.
#'
#' @export
build_psut_dataframe <- function(psutiea = NULL,
                                 psutmw = NULL,
                                 psutieamw = NULL,
                                 countries,
                                 country_colname = IEATools::iea_cols$country,
                                 method_colname = IEATools::iea_cols$method,
                                 energy_type_colname = IEATools::iea_cols$energy_type,
                                 last_stage_colname = IEATools::iea_cols$last_stage,
                                 year_colname = IEATools::iea_cols$year,
                                 ieamw_colname = PFUPipelineTools::ieamw_cols$ieamw,
                                 R_colname = IEATools::psut_cols$R,
                                 U_colname = IEATools::psut_cols$U,
                                 U_feed_colname = IEATools::psut_cols$U_feed,
                                 U_eiou_colname = IEATools::psut_cols$U_eiou,
                                 r_eiou_colname = IEATools::psut_cols$r_eiou,
                                 V_colname = IEATools::psut_cols$V,
                                 Y_colname = IEATools::psut_cols$Y,
                                 S_units_colname = IEATools::psut_cols$s_units,
                                 iea = PFUPipelineTools::ieamw_cols$iea,
                                 mw = PFUPipelineTools::ieamw_cols$mw,
                                 both = PFUPipelineTools::ieamw_cols$both) {

  if (is.null(psutiea) & is.null(psutmw) & is.null(psutieamw)) {
    # Nothing to be done.
    return(NULL)
  }

  # Bind the data frames, with each one having the new IEAMW column.
  if (!is.null(psutiea)) {
    psutiea <- psutiea %>%
      dplyr::mutate(
        "{ieamw_colname}" := iea
      )
  }
  if (!is.null(psutmw)) {
    psutmw <- psutmw %>%
      dplyr::mutate(
        "{ieamw_colname}" := mw
      )
  }
  if (!is.null(psutieamw)) {
    psutieamw <- psutieamw %>%
      dplyr::mutate(
        "{ieamw_colname}" := both
      )
  }
  dplyr::bind_rows(psutiea, psutmw, psutieamw) |>
    # Reorder columns into a sensible sequence.
    dplyr::select(dplyr::all_of(country_colname),
                  dplyr::all_of(method_colname),
                  dplyr::all_of(energy_type_colname),
                  dplyr::all_of(last_stage_colname),
                  dplyr::all_of(year_colname),
                  dplyr::all_of(ieamw_colname),
                  dplyr::all_of(R_colname),
                  dplyr::all_of(U_colname),
                  dplyr::all_of(U_feed_colname),
                  dplyr::all_of(U_eiou_colname),
                  dplyr::all_of(r_eiou_colname),
                  dplyr::all_of(V_colname),
                  dplyr::all_of(Y_colname),
                  dplyr::all_of(S_units_colname))
}
