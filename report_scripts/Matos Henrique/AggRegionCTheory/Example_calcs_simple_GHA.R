# This file contains an R implementation of the new algorithm for
# calculating allocations.
# The focus is on Ghana for testing.
# This is follow-on to the file Example_calcs_simple.R
# which used a very simple made-up example.
# Next step is to try it for Ghana in various years.
# That work is in a different file, namely
# Example_calcs_simple_GHA.R


conn <- PFUPipelineTools::get_scratchmdb_conn()
# conn <- PFUPipelineTools::get_mexerdb_conn(user = "dbcreator")
on.exit(DBI::dbDisconnect(conn))

versn_string <- "v3.0"
tol <- 1e-10
common_coltype <- "Common"

years <- 1971:2020

# Download data from the database
C_gha <- PFUPipelineTools::pl_filter_collect(
  db_table_name = "Cmats",
  version_string = versn_string,
  Dataset == "CL-PFU IEA",
  Year %in% years,
  Country == "GHA",
  create_matsindf = TRUE,
  collect = TRUE,
  conn = conn)

ruvy_gha <- PFUPipelineTools::pl_filter_collect(
  db_table_name = "PSUTReAllChopAllDsAllGrAll",
  version_string = versn_string,
  Dataset == "CL-PFU IEA",
  EnergyType == "E",
  IncludesNEU == "TRUE",
  ProductAggregation == "Specified",
  IndustryAggregation == "Specified",
  Year %in% years,
  Country == "GHA",
  create_matsindf = TRUE,
  collect = TRUE,
  conn = conn)

for (this_year in years) {

  C_gha_this_year <- C_gha |>
    dplyr::filter(Year == this_year)
  ruvy_gha_this_year <- ruvy_gha |>
    dplry::filter(Year == this_year)
  etai_gha_this_year <- etai_gha |>
    dplyr::filter(Year == this_year)



  # Extract various matrices
  Y_f <- ruvy_gha |>
    dplyr::filter(LastStage == "Final") |>
    purrr::pluck("Y", 1) |>
    matsbyname::select_cols_byname(remove_pattern = "^Exports") |>
    matsbyname::select_cols_byname(remove_pattern = "^Statistical differences")

  U_u <- ruvy_gha_this_year |>
    dplyr::filter(LastStage == "Useful") |>
    purrr::pluck("U", 1)

  V_u <- ruvy_gha_this_year |>
    dplyr::filter(LastStage == "Useful") |>
    purrr::pluck("V", 1)

  Y_u <- ruvy_gha_this_year |>
    dplyr::filter(LastStage == "Useful") |>
    purrr::pluck("Y", 1) |>
    matsbyname::select_cols_byname(remove_pattern = "^Exports") |>
    matsbyname::select_cols_byname(remove_pattern = "^Statistical differences") |>
    matsbyname::setrowtype("Product [from Industry]")

  Y_u_rename <- Y_u |>
    matsbyname::switch_notation_byname(margin = 1,
                                       from = RCLabels::from_notation,
                                       to = RCLabels::arrow_notation,
                                       flip = TRUE)

  eta_i <- matsbyname::quotient_byname(
    matsbyname::rowsums_byname(V_u, colname = "Product") |>
      matsbyname::setcoltype("Product"),
    matsbyname::colsums_byname(U_u) |>
      matsbyname::setrowtype("Product") |>
      matsbyname::transpose_byname())

  C_calc <- NULL
  for (this_sector in colnames(Y_f)) {
    for (this_Ef in rownames(Y_f)) {

      if (abs(Y_f[this_Ef, this_sector]) < tol) {
        # There is no energy flow here, so move to the next one.
        next
      }

      U_u_Ef_row <- U_u |>
        matsbyname::select_rows_byname(this_Ef, fixed = TRUE) |>
        matsbyname::clean_byname() |>
        matsbyname::selector_byname(margin = 2, tol = tol) |>
        matsbyname::transpose_byname() |>
        matsbyname::setrowtype("Industry -> Product") |>
        matsbyname::setcoltype(common_coltype)

      Y_u_s_col <- Y_u_rename |>
        matsbyname::select_cols_byname(this_sector) |>
        matsbyname::clean_byname() |>
        matsbyname::selector_byname(margin = 1, tol = tol) |>
        matsbyname::setcoltype(common_coltype)

      # Multiply to find common machines
      common_machines <- matsbyname::hadamardproduct_byname(U_u_Ef_row, Y_u_s_col) |>
        matsbyname::clean_byname() |>
        matsbyname::getrownames_byname()
      if (is.null(common_machines)) {
        # If no common machines exits,
        # skip to the next non-zero entry in the Y_f matrix
        next
      }

      # Select the common machines from Y_u
      Y_u_common <- Y_u_rename |>
        matsbyname::select_rows_byname(retain_pattern = RCLabels::make_or_pattern(common_machines)) |>
        matsbyname::select_cols_byname(retain_pattern = this_sector, fixed = TRUE)

      # Figure out new column names
      new_colnames <- RCLabels::paste_pref_suff(pref = this_Ef,
                                                suff = colnames(Y_u_common),
                                                notation = RCLabels::arrow_notation)

      Y_u_common_new_colnames <- Y_u_common |>
        matsbyname::setcolnames_byname(new_colnames) |>
        matsbyname::setcoltype("Product -> Industry")

      eta_i_trimmed <- Y_u_common_new_colnames |>
        matsbyname::mat_from_store_byname(v = eta_i, margin = 1, margin_v = 1, notation = RCLabels::arrow_notation)

      eta_i_hatinv <- eta_i_trimmed |>
        matsbyname::hatinv_byname(keep = "rownames") |>
        matsbyname::setrowtype("Industry -> Product") |>
        matsbyname::setcoltype("Industry -> Product")

      # Find the final energy associated with these machines
      # by multiplying by hatinv(etai)
      Y_u_common_Ef <- matsbyname::matrixproduct_byname(eta_i_hatinv, Y_u_common_new_colnames)

      # Create the piece of the allocation matrix
      this_C_submat <- Y_u_common_Ef |>
        matsbyname::transpose_byname() |>
        matsbyname::fractionize_byname(margin = 1) |>
        matsbyname::clean_byname()

      C_calc <- matsbyname::sum_byname(C_calc, this_C_submat)
    }
  }

  result <- matsbyname::difference_byname(C_calc, C_gha_this_year$C_Y[[1]]) |>
    matsbyname::iszero_byname()

  print(paste0(year, ": ", result))

}
