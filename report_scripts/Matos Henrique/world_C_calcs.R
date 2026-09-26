# This script demonstrates calculating
# Y_u from Y_f, allocations, and efficiencies.
# To verify the calculation works, we compare Y_u calculated
# here to Y_u in the database.
# Then, we show that the inverse is also possible,
# i.e., calculating C (allocation matrices)
# from available information (Y_f and efficiencies)
# for a particular country and year (GHA, 1971).

# Setup ------------------------------------------------------------------------
library(magrittr)

# First, gather information from the database ----------------------------------
conn <- PFUPipelineTools::get_scratchmdb_conn()
on.exit(DBI::dbDisconnect(conn))

# Y matrices
gha1971 <- PFUPipelineTools::pl_filter_collect(version_string = "v3.0",
                                               "PSUTReAllChopAllDsAllGrAll",
                                               Dataset == "CL-PFU IEA",
                                               Country == "GHA",
                                               Year == 1971,
                                               EnergyType == "E",
                                               ProductAggregation == "Specified",
                                               IndustryAggregation == "Specified",
                                               IncludesNEU == FALSE,
                                               collect = TRUE,
                                               conn = conn)
# Alloction (C) matrices
gha1971cmats <- PFUPipelineTools::pl_filter_collect(version_string = "v3.0",
                                                    "Cmats",
                                                    Dataset == "CL-PFU IEA",
                                                    Country == "GHA",
                                                    Year == 1971,
                                                    EnergyType == "E",
                                                    collect = TRUE,
                                                    conn = conn)
# Efficiency vectors
gha1971etavecs <- PFUPipelineTools::pl_filter_collect(version_string = "v3.0",
                                                      "Etafuvecs",
                                                      Dataset == "CL-PFU",
                                                      Country == "GHA",
                                                      Year == 1971,
                                                      EnergyType == "E",
                                                      collect = TRUE,
                                                      conn = conn)
DBI::dbDisconnect(conn)

Y_f_db <- gha1971 |>
  dplyr::filter(LastStage == "Final") |>
  purrr::pluck("Y", 1) |>
  as.matrix()
Y_f_vec_db <- Y_f_db |>
  matsbyname::vectorize_byname(notation = RCLabels::arrow_notation)
Y_f_vec_hat_db <- Y_f_vec_db |>
  matsbyname::hatize_byname(keep = "rownames")
Y_f_vec_hat_inv_db <- Y_f_vec_db |>
  matsbyname::hatinv_byname()
Y_u_db <- gha1971 |>
  dplyr::filter(LastStage == "Useful") |>
  purrr::pluck("Y", 1) |>
  as.matrix()
eta_vec_db <- gha1971etavecs$etafu[[1]] |>
  as.matrix()
eta_vec_hat_db <- eta_vec_db |>
  matsbyname::hatize_byname(keep = "rownames")
eta_vec_hat_inv_db <- eta_vec_db |>
  matsbyname::hatinv_byname()
C_Y_db <- gha1971cmats$C_Y[[1]] |>
  as.matrix()

allocated_final <- matsbyname::matrixproduct_byname(Y_f_vec_hat_db, C_Y_db)

Y_u_calc <- matsbyname::matrixproduct_byname(allocated_final, eta_vec_hat_db) %>%
  matsbyname::setcolnames_byname(RCLabels::switch_notation(colnames(.),
                                                           from = RCLabels::arrow_notation,
                                                           to = RCLabels::from_notation,
                                                           flip = TRUE,
                                                           inf_notation = FALSE)) |>
  matsbyname::aggregate_to_pref_suff_byname(keep = "suff",
                                            margin = 1,
                                            notation = RCLabels::arrow_notation) |>
  matsbyname::transpose_byname() |>
  matsbyname::clean_byname() |>
  matsbyname::sort_rows_cols()

# This is the Y_u we want to test against, one that does NOT have
# final energy going into sectors.
Y_u_db <- Y_u_db |>
  matsbyname::select_rows_byname(remove_pattern =
                                   RCLabels::make_or_pattern(c("Aviation gasoline",
                                                               "Crude oil",
                                                               "Electricity",
                                                               "Fuel oil",
                                                               "Gas/diesel oil excl. biofuels",
                                                               "Kerosene type jet fuel excl. biofuels",
                                                               "Motor gasoline excl. biofuels",
                                                               "Other kerosene")))

# The fact that this test returns TRUE shows that we can
# Calculate Y_u from Y_f, C_Y, and etafu.
# The goal is now to reverse the process
# to calculate C_Y from Y_u, Y_f, and etafu.
matsbyname::equal_byname(Y_u_db, Y_u_calc, tol = 1e-10)


# Calculate C_Y from  Y_u, Y_f, and etafu. -------------------------------------

# Try it with one country.
# We can compare the result against the known C_Y matrix.

Y_u_db_store <- Y_u_db |>
  matsbyname::switch_notation_byname(margin = 1,
                                     from = RCLabels::from_notation,
                                     to = RCLabels::arrow_notation,
                                     flip = TRUE) |>
  matsbyname::transpose_byname()

Y_u_db_expanded <-matsbyname::mat_from_store_byname(
  a = Y_f_vec_hat_inv_db,
  v = Y_u_db_store,
  margin = 2,
  margin_v = 1,
  notation = RCLabels::arrow_notation,
  a_piece = "suff",
  v_piece = "all")

C_Y_calc <- matsbyname::matrixproduct_byname(Y_f_vec_hat_inv_db, Y_u_db_expanded) |>
  matsbyname::matrixproduct_byname(eta_vec_hat_inv)

matsbyname::equal_byname(C_Y_calc, C_Y)



