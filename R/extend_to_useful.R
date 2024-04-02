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

  # Download the appropriate data frames from the database using the "ticket"



  # Calculate metadata columns.
  m_cols <- C_mats %>%
    IEATools::meta_cols(return_names = TRUE,
                        years_to_keep = year,
                        not_meta = c(C_Y, C_eiou))
  psut_final_filtered <- psut_final |>
    dplyr::filter(.data[[country]] %in% countries)

  psut_final_filtered %>%
    # Join the matrices and vectors to the psut_final data frame.
    dplyr::full_join(C_mats %>% dplyr::filter(.data[[country]] %in% countries), by = m_cols) %>%
    dplyr::full_join(eta_phi_vecs %>% dplyr::filter(.data[[country]] %in% countries), by = m_cols) %>%
    # And, finally, extend to the useful stage.
    IEATools::extend_to_useful() |>
    IEATools::stack_final_useful_df(psut_final_filtered)
}
