# This file demonstrates calculating C_Y
# from Ghana RUVY data.
# The algorithm is based on calculations shown
# in the file Other kerosene and Fuel oil example.xlsx.
#
# The point here is to do the calculations for the entire
# Ghanaian ECC in 1971.


# conn <- PFUPipelineTools::get_scratchmdb_conn()
conn <- PFUPipelineTools::get_mexerdb_conn(user = "dbcreator")
on.exit(DBI::dbDisconnect(conn))

versn_string <- "v2.0"

C_gha_1971 <- PFUPipelineTools::pl_filter_collect(
  db_table_name = "Cmats",
  version_string = versn_string,
  Dataset == "CL-PFU IEA",
  Year == 1971,
  Country == "GHA",
  create_matsindf = TRUE,
  collect = TRUE,
  conn = conn)

ruvy_gha_1971 <- PFUPipelineTools::pl_filter_collect(
  db_table_name = "PSUTReAllChopAllDsAllGrAll",
  version_string = versn_string,
  Dataset == "CL-PFU IEA",
  EnergyType == "E",
  IncludesNEU == "TRUE",
  ProductAggregation == "Specified",
  IndustryAggregation == "Specified",
  Year == 1971,
  Country == "GHA",
  create_matsindf = TRUE,
  collect = TRUE,
  conn = conn)

etai_gha_1971 <- PFUPipelineTools::pl_filter_collect(
  db_table_name = "Etai",
  version_string = versn_string,
  Dataset == "CL-PFU IEA",
  EnergyType == "E",
  LastStage == "Useful",
  IncludesNEU == "TRUE",
  Year == 1971,
  Country == "GHA",
  create_matsindf = TRUE,
  collect = TRUE,
  conn = conn)

Y_f_vec_hat_inv <- ruvy_gha_1971 |>
  dplyr::filter(LastStage == "Final") |>
  purrr::pluck("Y", 1) |>
  matsbyname::vectorize_byname(notation = RCLabels::arrow_notation) |>
  matsbyname::hatinv_byname(keep = "rownames")

Y_f_special <- ruvy_gha_1971 |>
  dplyr::filter(LastStage == "Final") |>
  purrr::pluck("Y", 1) |>
  matsbyname::fractionize_byname(margin = 2) |>
  matsbyname::vectorize_byname(notation = RCLabels::arrow_notation) |>
  matsbyname::hatize_byname() |>
  matsbyname::aggregate_pieces_byname(piece = "suff",
                                      margin = 2,
                                      notation = RCLabels::arrow_notation)

eta_fu_hat_inv <- etai_gha_1971 |>
  purrr::pluck("eta_i", 1) |>
  matsbyname::hatinv_byname(keep = "rownames") |>
  matsbyname::setrowtype("Industry -> Product")

Y_u_T <- ruvy_gha_1971 |>
  dplyr::filter(LastStage == "Useful") |>
  purrr::pluck("Y", 1) |>
  matsbyname::transpose_byname() |>
  matsbyname::switch_notation_byname(margin = 2,
                                     from = RCLabels::from_notation,
                                     to = RCLabels::arrow_notation,
                                     flip = TRUE) |>
  matsbyname::setcoltype("Industry -> Product")

Y_u_T__eta_fu_hat_inv <- Y_u_T |>
  matsbyname::matrixproduct_byname(eta_fu_hat_inv)

Y_f_special__Y_u_T__eta_fu_hat_inv <- Y_f_special |>
  matsbyname::matrixproduct_byname(Y_u_T__eta_fu_hat_inv)

C_Y <- Y_f_vec_hat_inv |>
  matsbyname::matrixproduct_byname(Y_f_special__Y_u_T__eta_fu_hat_inv) |>
  matsbyname::clean_byname()

matsbyname::rowsums_byname(C_Y)

# Compare to extracted C_Y matrix
C_Y_gha_1971_db <- C_gha_1971 |>
  purrr::pluck("C_Y", 1)




DBI::dbDisconnect(conn)
