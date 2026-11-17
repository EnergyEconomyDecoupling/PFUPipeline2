# This is another run at the problem of computing
# C_Y and C_EIOU matrices
# from RUVY matrices.
# This is needed so that we can calculate
# allocation matrices for aggregated regions.

# The strategy this time will be to very calculate
# each result and try to reverse the process.

conn <- PFUPipelineTools::get_scratchmdb_conn()
# conn <- PFUPipelineTools::get_mexerdb_conn(user = "dbcreator")
on.exit(DBI::dbDisconnect(conn))


versn_string <- "v3.0"
years <- 1971:2020

tol <- 1e-10


# For this example, we'll try Angola, 1984
# because that was a country/year combination that failed.
this_country <- "AGO"
this_year <- 1984


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
ruvy_this_country <- PFUPipelineTools::pl_filter_collect(
  db_table_name = "PSUTReAllChopAllDsAllGrAll",
  version_string = versn_string,
  Dataset == "CL-PFU IEA",
  EnergyType == "E",
  IncludesNEU == "TRUE",
  ProductAggregation == "Specified",
  IndustryAggregation == "Specified",
  Year %in% years,
  Country == this_country,
  create_matsindf = TRUE,
  collect = TRUE,
  conn = conn)

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

# Extract matrices
C_Y_this_year_db <- C_this_country |>
  dplyr::filter(Year == this_year) |>
  purrr::pluck("C_Y", 1)
ruvy_this_year <- ruvy_this_country |>
  dplyr::filter(Year == this_year)

# Extract various RUVY matrices
Y_f_db <- ruvy_this_year |>
  dplyr::filter(LastStage == "Final", Year == this_year) |>
  purrr::pluck("Y", 1) |>
  matsbyname::select_cols_byname(remove_pattern = "^Exports") |>
  matsbyname::select_cols_byname(remove_pattern = "^Statistical differences") |>
  matsbyname::select_cols_byname(remove_pattern = "^Losses$") |>
  matsbyname::select_cols_byname(remove_pattern = "^Stock changes")

Yfudetails_this_year <- YfuUEIOUfudetails_this_country |>
  dplyr::filter(Year == this_year) |>
  purrr::pluck("Y_fu_details", 1)

U_u_db <- ruvy_this_year |>
  dplyr::filter(LastStage == "Useful", Year == this_year) |>
  purrr::pluck("U", 1)

V_u_db <- ruvy_this_year |>
  dplyr::filter(LastStage == "Useful", Year == this_year) |>
  purrr::pluck("V", 1)

Y_u_db <- ruvy_this_year |>
  dplyr::filter(LastStage == "Useful", Year == this_year) |>
  purrr::pluck("Y", 1) |>
  matsbyname::setrowtype("Product [from Industry]")

Y_u_db_compare <- Y_u_db |>
  matsbyname::select_cols_byname(remove_pattern = "^Exports") |>
  matsbyname::select_cols_byname(remove_pattern = "^Statistical differences") |>
  matsbyname::select_cols_byname(remove_pattern = "^Losses$") |>
  matsbyname::select_cols_byname(remove_pattern = "^Stock changes") |>
  matsbyname::clean_byname()

eta_i_calc <- matsbyname::quotient_byname(
  matsbyname::rowsums_byname(V_u_db, colname = "Product") |>
    matsbyname::setcoltype("Product"),
  matsbyname::colsums_byname(U_u_db) |>
    matsbyname::setrowtype("Product") |>
    matsbyname::transpose_byname())




# Calculate Y_u from Y_f using C_Y and efficiencies.
# This proves that we can do a forward calculation.
Y_f_vec <- Y_f_db |>
  matsbyname::vectorize_byname(notation = RCLabels::arrow_notation)
Y_f_vec_hat <- Y_f_vec |>
  matsbyname::hatize_byname()

YFCY <- matsbyname::matrixproduct_byname(Y_f_vec_hat, C_Y_this_year_db)

eta_i_store <- matsbyname::mat_from_store_byname(a = YFCY,
                                                 v = eta_i_calc,
                                                 margin = 2,
                                                 margin_v = 1) |>
  matsbyname::setrowtype("Industry -> Product")
eta_i_store_hat <- eta_i_store |>
  matsbyname::hatize_byname(keep = "rownames")


Y_u_prelim_1_calc <- matsbyname::matrixproduct_byname(YFCY, eta_i_store_hat)
Y_u_prelim_2_calc <- matsbyname::transpose_byname(Y_u_prelim_1_calc)
Y_u_prelim_3_calc <- Y_u_prelim_2_calc |>
  matsbyname::switch_notation_byname(margin = 1,
                                     from = RCLabels::arrow_notation,
                                     to = RCLabels::from_notation, flip = TRUE)
Y_u_calc <- Y_u_prelim_3_calc |>
  matsbyname::aggregate_pieces_byname(margin = 2, piece = "suff", notation = RCLabels::arrow_notation) |>
  matsbyname::setcoltype("Industry") |>
  matsbyname::sort_rows_cols()

compare_Y_u <- matsbyname::difference_byname(Y_u_calc, Y_u_db_compare)
# When this is true, we have obtained the correct answer in the forward direction!
matsbyname::iszero_byname(compare)



# The challenge here is that we have collapsed to the column suffix of
# Y_u_prelim_3_calc to form Y_u_calc.
# Thus, we have lost the information about the final energy source
# of all entries in the Y_u matrix.

# But if we had saved Y_u_prelim_3_calc,
# could we back-calculate C_Y?
# Let's try.



# Calculate some inverses
Y_f_vec_hat_inv <- matsbyname::invert_byname(Y_f_vec_hat)
eta_i_hat_inv <- eta_i_store |>
  matsbyname::hatinv_byname()

Y_f_vec_hat_inv_Yu <- Y_f_vec_hat_inv |>
  matsbyname::matrixproduct_byname(Y_u_prelim_3_calc |>
                                     matsbyname::transpose_byname()) |>
  matsbyname::switch_notation_byname(margin = 2,
                                     from = RCLabels::from_notation,
                                     to = RCLabels::arrow_notation,
                                     flip = TRUE)

C_Y_calc <- matsbyname::matrixproduct_byname(Y_f_vec_hat_inv_Yu, eta_i_hat_inv)

compare_C_Y <- matsbyname::difference_byname(C_Y_calc, C_Y_this_year_db)
matsbyname::iszero_byname(compare_C_Y)
