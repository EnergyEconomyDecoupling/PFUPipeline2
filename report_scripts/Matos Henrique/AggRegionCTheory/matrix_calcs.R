# This script calculates
# C_Y and C_EIOU matrices
# for all countries and all years and
# compares to allocation matrices
# already in the database.
#
# If this script runs to completion without stopping,
# it demonstrates that we have the correct
# formula/algorithm to calculate C matrices
# from existing data in the database
# for those cases where we know the answer.
#
# That should provide confidence for calculating
# C_Y and C_U_EIOU for aggregated regions
# where we have not allocation matrices.

conn <- PFUPipelineTools::get_scratchmdb_conn()
# conn <- PFUPipelineTools::get_mexerdb_conn(user = "dbcreator")
on.exit(DBI::dbDisconnect(conn))

versn_string <- "v3.0"
years <- 1960:2020

tol <- 1e-10

countries <- PFUPipelineTools::canonical_countries

for (this_country in countries) {

  # Download data from the database
  print("")
  print(paste0("Downloading ", this_country, " Cmats"))
  C_this_country <- PFUPipelineTools::pl_filter_collect(
    db_table_name = "Cmats",
    version_string = versn_string,
    Dataset == "CL-PFU IEA",
    Year %in% years,
    Country == this_country,
    create_matsindf = TRUE,
    collect = TRUE,
    conn = conn)

  print(paste0("Downloading ", this_country, " RUVY matrices"))
  Yf_this_country <- PFUPipelineTools::pl_filter_collect(
    db_table_name = "PSUTReAllChopAllDsAllGrAll",
    version_string = versn_string,
    Dataset == "CL-PFU IEA",
    EnergyType == "E",
    LastStage == "Final",
    IncludesNEU == TRUE,
    ChoppedMat == "None",
    ChoppedVar == "None",
    ProductAggregation == "Specified",
    IndustryAggregation == "Specified",
    Year %in% years,
    Country == this_country,
    create_matsindf = TRUE,
    collect = TRUE,
    conn = conn) |>
    dplyr::select(dplyr::all_of(c("Dataset", "ValidFromVersion", "ValidToVersion", "Country",
                                  "Method", "EnergyType", "LastStage", "IncludesNEU", "Year",
                                  "ChoppedMat", "ChoppedVar", "ProductAggregation",
                                  "IndustryAggregation", "Y", "U_EIOU")))

  print(paste0("Downloading ", this_country, " YfuUEIOUfudetails matrices"))
  YfuUEIOUfudetails_this_country <- PFUPipelineTools::pl_filter_collect(
    db_table_name = "YfuUEIOUfudetails",
    version_string = versn_string,
    Dataset == "CL-PFU IEA",
    EnergyType == "E",
    Year %in% years,
    Country == this_country,
    create_matsindf = TRUE,
    collect = TRUE,
    conn = conn)

  print(paste0("Downloading ", this_country, " etai matrices"))
  etai_this_country <- PFUPipelineTools::pl_filter_collect(
    db_table_name = "Etai",
    version_string = versn_string,
    Dataset == "CL-PFU IEA",
    EnergyType == "E",
    Year %in% years,
    Country == this_country,
    IncludesNEU == TRUE,
    LastStage == "Useful",
    create_matsindf = TRUE,
    collect = TRUE,
    conn = conn)

  for (this_year in years) {

    # Extract matrices.
    # This value of C_Y is for comparison purposes
    C_Y_this_year_db <- C_this_country |>
      dplyr::filter(Year == this_year) |>
      purrr::pluck("C_Y", 1)
    if (is.null(C_Y_this_year_db)) {
      # Nothing to be done.
      # Go on to the next year
      next
    }
    # This value of C_U_EIOU is for comparison purposes
    C_U_EIOU_this_year_db <- C_this_country |>
      dplyr::filter(Year == this_year) |>
      purrr::pluck("C_EIOU", 1)
    # Get Yf and U_EIOU for this country and this year
    Yf_this_year <- Yf_this_country |>
      dplyr::filter(Year == this_year) |>
      purrr::pluck("Y", 1) |>
      matsbyname::select_cols_byname(remove_pattern = "^Exports") |>
      matsbyname::select_cols_byname(remove_pattern = "^Statistical differences") |>
      matsbyname::select_cols_byname(remove_pattern = "^Losses$") |>
      matsbyname::select_cols_byname(remove_pattern = "^Stock changes")
    U_EIOU_this_year <- Yf_this_country |>
      dplyr::filter(Year == this_year) |>
      purrr::pluck("U_EIOU", 1)
    # Get the details matrices for this country and this year
    Yfudetails_this_year <- YfuUEIOUfudetails_this_country |>
      dplyr::filter(Year == this_year) |>
      purrr::pluck("Y_fu_details", 1) |>
      matsbyname::switch_notation_byname(margin = 2,
                                         from = RCLabels::from_notation,
                                         to = RCLabels::arrow_notation,
                                         flip = TRUE)
    UEIOUfudetails_this_year <- YfuUEIOUfudetails_this_country |>
      dplyr::filter(Year == this_year) |>
      purrr::pluck("U_EIOU_fu_details", 1) |>
      matsbyname::switch_notation_byname(margin = 2,
                                         from = RCLabels::from_notation,
                                         to = RCLabels::arrow_notation,
                                         flip = TRUE)
    # Get the machine efficiencies for this country and this year
    etai_this_year <- etai_this_country |>
      dplyr::filter(Year == this_year) |>
      purrr::pluck("eta_i", 1) |>
      matsbyname::setrowtype("Industry -> Product")

    # Calculate vectorized inverses
    # for the Yf and U_EIOU matrices
    Yf_vec_hat_inv <- Yf_this_year |>
      matsbyname::vectorize_byname(notation = RCLabels::arrow_notation) |>
      matsbyname::hatinv_byname(keep = "rownames")
    UEIOUf_vec_hat_inv <- U_EIOU_this_year |>
      matsbyname::vectorize_byname(notation = RCLabels::arrow_notation) |>
      matsbyname::hatinv_byname(keep = "rownames")
    # Calculate inverses for machine efficiencies
    etai_hat_inv <- etai_this_year |>
      matsbyname::hatinv_byname(keep = "rownames")

    # Do the calculation of C_Y and C_U_EIOU
    # These calculations are the reverse of the
    # calculation that provided the useful version of the
    # final demand (Y_u) and energy industry own use matrices (U_EIOU_u).
    # The original calcs were
    #   Y_fu_details = Yf_vec_hat * C_Y * etai_hat
    #   U_EIOU_fu_details = U_EIOU_f_vec_hat * C_U_EIOU * etau_hat
    # So the calculations to find the C matrices are
    #   C_Y = Yf_vec_hat_inv * Yfu_details * etai_hat_inv
    #   C_U_EIOU = U_EIOU_f_vec_inv * U_EIOU_fu_details * etai_hat_inv
    C_Y_calc <- Yf_vec_hat_inv |>
      matsbyname::matrixproduct_byname(Yfudetails_this_year) |>
      matsbyname::matrixproduct_byname(etai_hat_inv) |>
      matsbyname::clean_byname()
    C_U_EIOU_calc <- UEIOUf_vec_hat_inv |>
      matsbyname::matrixproduct_byname(UEIOUfudetails_this_year) |>
      matsbyname::matrixproduct_byname(etai_hat_inv) |>
      matsbyname::clean_byname()

    # Now compare to C_Y and C_U_EIOU in the database
    # by difference.
    # the two compare_* matrices should be zero to within tol
    compare_C_Y <- matsbyname::difference_byname(C_Y_calc, C_Y_this_year_db)
    compare_C_U_EIOU <- matsbyname::difference_byname(C_U_EIOU_calc, C_U_EIOU_this_year_db)
    stopifnot(matsbyname::iszero_byname(compare_C_Y, tol = tol) &
                matsbyname::iszero_byname(compare_C_U_EIOU, tol = tol))
    print(paste0(this_country, ", ", this_year, ", C_Y, C_U_EIOU: ", matsbyname::iszero_byname(compare_C_Y)))
  }
}

print("Success!")
