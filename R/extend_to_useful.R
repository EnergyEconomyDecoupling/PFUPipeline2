#' Move the last stage of the energy conversion chain from final stage to useful stage with details
#'
#' Extends the energy conversion chain from a final energy last stage to useful energy last stage.
#' Details about the conversion from final to useful are retained via matrices
#' **Y_fu_details** and **U_EIOU_fu_details**.
#' The last-stage-useful energy conversion chain PSUT matrices are bound as rows at the bottom
#' of the `psut_final` data frame,
#' albeit with "Useful" instead of "Final" in the `Last.stage` column.
#'
#' @param psut_final A data frame with rows that describe the energy conversion chain with final energy as the last stage.
#' @param C_mats A data frame with allocation matrices, probably the Cmats target.
#' @param eta_phi_vecs A data frame with final-to-useful efficiency and exergy-to-energy ratio vectors.
#' @param countries The countries to be analyzed.
#' @param country,year See IEATools::iea_cols.
#' @param C_Y,C_eiou See IEATools::template_cols.
#'
#' @return A data frame with energy conversion chain matrices with last stage as useful energy.
#'
#' @export
move_to_useful_with_details <- function(psut_final,
                                        C_mats,
                                        eta_phi_vecs,
                                        countries,
                                        country = IEATools::iea_cols$country,
                                        year = IEATools::iea_cols$year,
                                        C_Y = IEATools::template_cols$C_Y,
                                        C_eiou = IEATools::template_cols$C_eiou) {

  # Calculate metadata columns.
  m_cols <- C_mats |>
    IEATools::meta_cols(return_names = TRUE,
                        years_to_keep = year,
                        not_meta = c(C_Y, C_eiou))

  psut_final |>
    # Join the matrices and vectors to the psut_final data frame.
    dplyr::full_join(C_mats, by = m_cols) |>
    dplyr::full_join(eta_phi_vecs, by = m_cols) |>
    # And, finally, extend to the useful stage.
    IEATools::extend_to_useful() |>
    IEATools::stack_final_useful_df(psut_final)
}


#' Remove columns from the PSUTUsefulIEAWithDetails target
#'
#' A simple wrapper function to assist with removing
#' unneeded columns from the `PSUTUsefulIEAWithDetails` target.
#'
#' This function enables mapping over countries.
#'
#' @param psut_useful_iea_with_details The target from which columns should be removed.
#' @param cols_to_remove A string vector of columns names to be removed.
#' @param remove_final A boolean that tells whether to remove Last.stage == "Final" rows.
#'                     Default is `FALSE`.
#' @param countries The countries for which this function should be applied.
#' @param country The name of the `Country` column in `psut_useful_iea_with_details` and `phi_vecs`.
#'                Default is `IEATools::iea_cols$country`.
#' @param year The name of the `Year` column in  `psut_useful_iea_with_details` and `phi_vecs`.
#'             Default is `IEATools::iea_cols$year`.
#' @param last_stage The name of the "Last.stage" column.
#'                   Default is `IEATools::iea_cols$last_stage`.
#' @param final The string defining the final stage.
#'              Default is `IEATools::all_stages$final`.
#'
#' @return A version of `psut_useful_iea_with_details` with `cols_to_remove` removed.
#'
#' @export
remove_cols_from_PSUTUsefulIEAWithDetails <- function(psut_useful_iea_with_details,
                                                      cols_to_remove,
                                                      remove_final = FALSE,
                                                      countries,
                                                      country = IEATools::iea_cols$country,
                                                      year = IEATools::iea_cols$year,
                                                      last_stage = IEATools::iea_cols$last_stage,
                                                      final = IEATools::all_stages$final) {

  out <- psut_useful_iea_with_details |>
    # dplyr::filter(.data[[country]] %in% countries) |>
    dplyr::select(-dplyr::any_of(cols_to_remove))
  if (remove_final) {
    out <- out |>
      dplyr::filter(.data[[last_stage]] != final)
  }
  return(out)
}
