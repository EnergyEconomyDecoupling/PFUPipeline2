# This script demonstrates calculating
# Y_u from Y_f, allocations, and efficiencies.
# To verify the calculation works, we compare Y_u calculated
# here to Y_u in the database.
# Then, we show that the inverse is also possible,
# i.e., calculating C (allocation matrices)
# from available information (Y_f and efficiencies)
# for a particular country and year (GHA, 1971).

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

Y_f <- gha1971 |>
  dplyr::filter(LastStage == "Final") |>
  purrr::pluck("Y", 1) |>
  as.matrix()
Y_f_vec <- Y_f |>
  matsbyname::vectorize_byname(notation = RCLabels::arrow_notation)
Y_f_vec_hat <- Y_f_vec |>
  matsbyname::hatize_byname(keep = "rownames")
Y_u <- gha1971 |>
  dplyr::filter(LastStage == "Useful") |>
  purrr::pluck("Y", 1) |>
  as.matrix()
eta_vec <- gha1971etavecs$etafu[[1]] |>
  as.matrix()
eta_vec_hat <- eta_vec |>
  matsbyname::hatize_byname(keep = "rownames")
C_Y <- gha1971cmats$C_Y[[1]] |>
  as.matrix()


allocated_final <- matsbyname::matrixproduct_byname(Y_f_vec_hat, C_Y)


Y_u_calculated <- matsbyname::matrixproduct_byname(allocated_final, eta_vec_hat) |>
  matsbyname::transpose_byname() %>%
  matsbyname::setrownames_byname(RCLabels::switch_notation(rownames(.),
                                                           from = RCLabels::arrow_notation,
                                                           to = RCLabels::from_notation,
                                                           flip = TRUE,
                                                           inf_notation = FALSE)) |>
  matsbyname::aggregate_to_pref_suff_byname(keep = "suff",
                                            margin = 2,
                                            notation = RCLabels::arrow_notation) |>
  matsbyname::clean_byname() |>
  matsbyname::sort_rows_cols()

# This is the Y_u we want to test against, one that does NOT have
# final energy going into sectors.
Y_u_test <- Y_u |>
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
# This also means we can reverse the process
# to calculate C_Y from Y_u, Y_f, and etafu.
matsbyname::equal_byname(Y_u_test, Y_u_calculated, tol = 1e-10)


# Calculate C_Y from  Y_u, Y_f, and etafu. -------------------------------------








gha1971 |>
  dplyr::mutate(
    WorksheetNames = paste0(LastStage, "+", EnergyType)
  ) |>
  Recca::write_ecc_to_excel(path = "~/Desktop/GHA1971.xlsx",
                            worksheet_names = "WorksheetNames",
                            overwrite_file = TRUE)




gha1971cmats$C_Y[[1]] |>
  as.matrix() |>
  openxlsx2::write_xlsx(file = "~/Desktop/CY.xlsx", row_names = TRUE)







gha1971etavecs$etafu[[1]] |>
  as.matrix() |>
  openxlsx2::write_xlsx(file = "~/Desktop/eta.xlsx", row_names = TRUE)





