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

  # If the psut_energy data frame is NULL, just return NULL.
  if (is.null(psut_energy)) {
    return(NULL)
  }
  # If the psut_energy data frame has no rows,
  # simply return it.
  # Both the incoming and outgoing data frames have the exact same columns.
  if (nrow(psut_energy) == 0) {
    return(psut_energy)
  }

  # We have a non-zero number of rows, so proceed with the calculations.

  # Get the metadata columns for the phi_vecs data frame.
  meta_cols <- matsindf::everything_except(phi_vecs, phi_colname, .symbols = FALSE)

  psut_energy |>
    # Join the phi vectors to the psut_energy data frame
    dplyr::left_join(phi_vecs, by = meta_cols) |>
    # Calculate exergy versions of the ECC.
    # Need to specify the mat_piece here, because the default value ("all")
    # is not appropriate.
    # We will have cases where the matrix will have specified names like
    # "MP [from Bulk carrier ships]".
    # In this case, we need to match the noun, not the whole string.
    Recca::extend_to_exergy(mat_piece = "noun", phi_piece = "all")
}


#' Create a data frame of phi vectors for muscle work
#'
#' The `phi_vecs` argument of `move_to_exergy()` is a data frame with
#' "Country", "Year", and "phi" columns,
#' where the "phi" column contains phi vectors of the type created by
#' `MWTools::phi_vec_mw()`.
#' This function creates the required data frame from its parts.
#'
#' @param psut_energy_mw A PSUT data frame containing `country` and `year` columns.
#' @param phi_vec_mw A single vector of muscle work phi values. See `MWTools::phi_vec_mw()`.
#' @param countries The countries to be analyzed. Internally, `psut_energy_mw` is filtered for `countries`.
#' @param country,year Column names. See `MWTools::mw_cols`.
#' @param phi The name of the phi column. Default is "phi".
#'
#' @return A data frame of muscle work phi vectors, suitable for `move_to_exergy()`.
#'
#' @export
calc_phi_vecs_mw <- function(psut_energy_mw,
                             phi_vec_mw,
                             countries,
                             country = MWTools::mw_cols$country,
                             year = MWTools::mw_cols$year,
                             phi = "phi") {

  browser()

  if (is.null(psut_energy_mw)) {
    return(NULL)
  }
  if (nrow(psut_energy_mw) == 0) {
    return(NULL)
  }

  psut_energy_mw |>
    dplyr::select(dplyr::all_of(c(country, year))) |>
    unique() |>
    # Create the outgoing data frame from phi_vec_mw
    dplyr::mutate(
      "{phi}" := list(phi_vec_mw)
    )
}
