#' Move from all exergy quantities to all energy quantities in energy conversion chains
#'
#' Converts energy conversion chains represented by the matrices
#' in the data frame of `psut_energy` from energy quantities to exergy quantities.
#'
#' The steps in this calculation are to join phi_vecs to psut_energy.
#' Thereafter, we call into the `IEATools` package to do the matrix multiplications.
#'
#' @param psut_energy A wide-by-matrices data frame of energy conversion chain data.
#' @param phi_vecs A data frame of vectors of phi (exergy-to-energy ratios)
#' @param countries The countries for which this task should be performed.
#' @param country See `IEATools::iea_cols`.
#' @param phi_colname See `IEATools::phi_constants`.
#'
#' @return A version of `psut_energy` with additional rows
#'
#' @export
move_to_exergy <- function(psut_energy,
                           phi_vecs,
                           countries,
                           country = IEATools::iea_cols$country,
                           phi_colname = IEATools::phi_constants_names$phi_colname) {
  # Make sure we're operating on the countries of interest.
  psut_energy <- psut_energy %>%
    dplyr::filter(.data[[country]] %in% countries)
  # If the psut_energy data frame has no rows,
  # simply return it.
  # Both the incoming and outgoing data frames have the exact same columns.
  if (nrow(psut_energy) == 0) {
    return(psut_energy)
  }
  # We have a non-zero number of rows, so proceed with the calculations.
  phi_vecs <- phi_vecs %>%
    dplyr::filter(.data[[country]] %in% countries)

  # Get the metadata columns for the phi_vecs data frame.
  meta_cols <- matsindf::everything_except(phi_vecs, phi_colname, .symbols = FALSE)

  # Join the phi vectors to the psut_energy data frame
  df_with_phi <- dplyr::left_join(psut_energy, phi_vecs, by = meta_cols)

  # Calculate exergy versions of the ECC.
  # Need to specify the mat_piece here, because the default value ("all")
  # is not appropriate.
  # We will have cases where the matrix will have specified names like
  # "MP [from Bulk carrier ships]".
  # In this case, we need to match the noun, not the whole string.
  Recca::extend_to_exergy(df_with_phi, mat_piece = "noun", phi_piece = "all")
}
