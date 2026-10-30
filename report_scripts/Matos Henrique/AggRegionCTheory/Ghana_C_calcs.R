# This file demonstrates calculating C_Y
# from Ghana RUVY data.
# The algorithm is based on calculations shown
# in the file Other kerosene and Fuel oil example.xlsx.
#
# The point here is to do the calculations for the entire
# Ghanaian ECC in 1971.
#
# These example calculations follow the spreadsheet
# named PSB Charcoal example.xlsx.


conn <- PFUPipelineTools::get_scratchmdb_conn()
# conn <- PFUPipelineTools::get_mexerdb_conn(user = "dbcreator")
on.exit(DBI::dbDisconnect(conn))

versn_string <- "v3.0"

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

Y_u_T <- ruvy_gha_1971 |>
  dplyr::filter(LastStage == "Useful") |>
  purrr::pluck("Y", 1) |>
  matsbyname::transpose_byname() |>
  matsbyname::switch_notation_byname(margin = 2,
                                     from = RCLabels::from_notation,
                                     to = RCLabels::arrow_notation,
                                     flip = TRUE) |>
  matsbyname::setcoltype("Industry -> Product")

eta_fu_hat_inv <- etai_gha_1971 |>
  purrr::pluck("eta_i", 1) |>
  matsbyname::hatinv_byname(keep = "rownames") |>
  matsbyname::setrowtype("Industry -> Product") |>
  matsbyname::setcoltype("Industry -> Product")

# U_u_special gives the fraction of each type of final energy
# going into each machine.
# Columns sum to 1.
U_u_special <- ruvy_gha_1971 |>
  dplyr::filter(LastStage == "Useful") |>
  purrr::pluck("U", 1) |>
  matsbyname::fractionize_byname(margin = 2) |>
  matsbyname::vectorize_byname(notation = RCLabels::to_notation) |>
  matsbyname::hatize_byname(keep = "rownames") |>
  matsbyname::aggregate_pieces_byname(piece = "suff",
                                      margin = 2,
                                      notation = RCLabels::to_notation) |>
  matsbyname::setrowtype("Product [to Industry -> Product]") |>
  matsbyname::setcoltype("Industry -> Product")

# This matrix gives the amount of final energy
# used by each machine in each final demand sector.
Y_u_T__eta_fu_hat_inv <- Y_u_T |>
  matsbyname::matrixproduct_byname(eta_fu_hat_inv)

# This matrix give the amount of final energy
# going into each machine (and useful product)
# in rows.
# Columns are the sectors in which the final and
# useful energy are used.
# The format for rownames is
# final [to machine -> useful]
# Column names give sectors in which the
# transformation occurs.
res1 <- U_u_special |>
  matsbyname::matrixproduct_byname(matsbyname::transpose_byname(Y_u_T__eta_fu_hat_inv))

# Define a new notation
underline_notation <- RCLabels::notation_vec(sep = " ___ ")

# Change the rowname notation
# from
# Final [to FU machine -> Useful] ___ Sector
# to
# Final -> Sector ___ FU machine -> Useful

fix_rowname <- function(rname) {
  if (length(rname) > 1) {
    return(sapply(X = rname, FUN = fix_rowname))
  }
  # Split the rowname into constituent parts,
  # sector, final energy carrier,
  # machine, and useful energy carrier.
  prefsuff <- rname |>
    RCLabels::split_pref_suff(notation = underline_notation)
  sector <- prefsuff$suff
  final_machine_useful <- prefsuff$pref
  prefsuff2 <- final_machine_useful |>
    RCLabels::split_pref_suff(notation = RCLabels::to_notation)
  final <- prefsuff2$pref
  machine_useful <- prefsuff2$suff
  prefsuff3 <- machine_useful |>
    RCLabels::split_pref_suff(notation = RCLabels::arrow_notation)
  machine <- prefsuff3$pref
  useful <- prefsuff3$suff
  # Put it all back together
  # in the way we need:
  # final -> sector ___ machine -> useful
  RCLabels::paste_pref_suff(
    pref = RCLabels::paste_pref_suff(pref = final,
                                     suff = sector,
                                     notation = RCLabels::arrow_notation),
    suff = RCLabels::paste_pref_suff(pref = machine,
                                     suff = useful,
                                     notation = RCLabels::arrow_notation),
    notation = underline_notation
  )
}

# This step puts
# final, machine, useful, and sector information all in the row name
# with the form
# final [to machine -> useful] ___ sector.
res2 <- res1 |>
  matsbyname::vectorize_byname(notation = underline_notation)

# This step changes the row names
# into the form we need for the C_Y matrix, namely
# final -> sector ___ machine -> useful.
rownames(res2) <- rownames(res2) |>
  fix_rowname()

res3 <- res2 |>
  matsbyname::setrowtype("Product -> Industry ___ Industry -> Product")

# Convert the vector into a matrix,
# splitting at the "___".
C_Y_calc <- res3 |>
  matsbyname::matricize_byname(notation = underline_notation) |>
  matsbyname::fractionize_byname(margin = 1)

# C_Y_calc should be compared to C_Y_db to see if they are equal.

C_Y_db <- C_gha_1971 |>
  purrr::pluck("C_Y", 1)


C_Y_calc |> as.matrix() |> View()
C_Y_db |> as.matrix() |> View()



# The next code gives TRUE, so they are the same except
# for the Fuel oil -> CPS row.
C_Y_calc |>
  matsbyname::select_rows_byname(remove_pattern = '^Fuel oil -> Commercial and public services$') |>
  matsbyname::difference_byname(C_Y_db) |>
  matsbyname::iszero_byname()

matsbyname::difference_byname(C_Y_calc, C_Y_db) |> as.matrix() |> View()








DBI::dbDisconnect(conn)
