# This script saves Germany natural gas consumption for
# Carey King (University of Texas Energy Institute) and
# Avery Sugg (also University of Texas).
#
# See emails among Avery, Carey, and me from April 2025.
# Also, see emails and discussion with Carey and Avery Suggs
# around 18 Sept 2025.

# This is a data delivery to get things started:
# Germany 2019
# Downstream swim from imported Natural gas
# Also the entire ECC for that year.
# All data are in energy, not exergy.

# Working in scratchMDB, because that's where we have the
# most up-to-date version of the database at this time.
conn <- PFUPipelineTools::get_scratchmdb_conn()
on.exit(DBI::dbDisconnect(conn))

# Extract data for Germany 2019.
# Energy only.
deu_2019 <- PFUPipelineTools::pl_filter_collect("PSUT",
                                                version_string = "v2.1a3",
                                                Dataset == "CL-PFU IEA",
                                                Country == "DEU",
                                                Year == 2019,
                                                EnergyType == "E",
                                                # LastStage == "Useful",
                                                IncludesNEU == TRUE,
                                                conn = conn,
                                                collect = TRUE)
downstream_swim <- deu_2019 |>
  Recca::calc_io_mats(direction = "downstream") |>
  dplyr::mutate(
    R_prime = .data[["R"]] |>
      matsbyname::select_rows_byname(retain_pattern = "Imports [of Natural gas]", fixed = TRUE)
  ) |>
  # Calculate downstream swim matrices
  Recca::new_R_ps() |>
  dplyr::mutate(
    # Eliminate non-prime matrix columns
    R = NULL, U = NULL, V = NULL, Y = NULL, U_feed = NULL, U_EIOU = NULL, r_EIOU = NULL,
    # Eliminate io matrices,
    y = NULL, q = NULL, f = NULL, g = NULL, h = NULL, r = NULL, W = NULL, Z_s = NULL, C_s = NULL,
    D_s = NULL, D_feed_s = NULL, O_s = NULL, B = NULL, G_pxp = NULL, G_ixp = NULL
  ) |>
  dplyr::rename(
    R = R_prime, U = U_prime, V = V_prime, Y = Y_prime,
    U_feed = U_feed_prime, U_EIOU = U_EIOU_prime, r_EIOU = r_EIOU_prime,
  )

for_avery <- dplyr::bind_rows(deu_2019 |>
                                dplyr::mutate(Swim = "none"),
                              downstream_swim |>
                                dplyr::mutate(Swim = "downstream")) |>
  dplyr::mutate(
    WorksheetNames = paste(Year, LastStage, Swim, Country, sep = "_")
  )

for_avery |>
  Recca::write_ecc_to_excel("~/Desktop/for_avery.xlsx",
                            worksheet_names = "WorksheetNames",
                            overwrite_file = TRUE)

DBI::dbDisconnect(conn)
