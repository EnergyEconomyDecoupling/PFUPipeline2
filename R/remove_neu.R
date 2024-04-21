#' Remove Non-energy use from energy conversion chains
#'
#' It is helpful to remove Non-energy use from ECCs.
#' This function calls `Recca::remove_neu()`.
#'
#' @param .psut_mats A data frame of PSUT matrices.
#' @param countries The countries to be analyzed.
#' @param R_col,U_col,U_feed_col,U_eiou_col,r_eiou_col,V_col,Y_col,S_units_col See [Recca::psut_cols].
#' @param prime_suffix The suffix to add to column names to indicate the without NEU case.
#'
#' @return A version of `.psut_mats` with Non-energy use removed.
#'
#' @export
remove_non_energy_use <- function(.psut_mats,
                                  countries,
                                  R_col = Recca::psut_cols$R,
                                  U_col = Recca::psut_cols$U,
                                  U_feed_col = Recca::psut_cols$U_feed,
                                  U_eiou_col = Recca::psut_cols$U_eiou,
                                  r_eiou_col = Recca::psut_cols$r_eiou,
                                  V_col = Recca::psut_cols$V,
                                  Y_col = Recca::psut_cols$Y,
                                  S_units_col = Recca::psut_cols$S_units,
                                  prime_suffix = "_prime") {

  if (is.null(.psut_mats)) {
    # Nothing to be done.
    return(NULL)
  }

  .psut_mats |>
    Recca::remove_neu(R = R_col,
                      U = U_col,
                      U_feed = U_feed_col,
                      U_eiou = U_eiou_col,
                      r_eiou = r_eiou_col,
                      V = V_col,
                      Y = Y_col,
                      S_units = S_units_col,
                      prime_suffix = prime_suffix) |>
    dplyr::mutate(
      # Eliminate unneeded columns
      "{R_col}" := NULL,
      "{U_col}" := NULL,
      "{U_feed_col}" := NULL,
      "{U_eiou_col}" := NULL,
      "{r_eiou_col}" := NULL,
      "{V_col}" := NULL,
      "{Y_col}" := NULL,
      "{S_units_col}" := NULL
    ) |>
    dplyr::rename(
      # Rename matrices without NEU ("_prime") to regular column names.
      "{R_col}" := paste0(R_col, prime_suffix),
      "{U_col}" := paste0(U_col, prime_suffix),
      "{U_feed_col}" := paste0(U_feed_col, prime_suffix),
      "{U_eiou_col}" := paste0(U_eiou_col, prime_suffix),
      "{r_eiou_col}" := paste0(r_eiou_col, prime_suffix),
      "{V_col}" := paste0(V_col, prime_suffix),
      "{Y_col}" := paste0(Y_col, prime_suffix),
      "{S_units_col}" := paste0(S_units_col, prime_suffix)
    )
}
