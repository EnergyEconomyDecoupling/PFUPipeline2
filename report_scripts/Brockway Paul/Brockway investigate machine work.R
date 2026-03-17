# This file investigates questions about
# world aggregate mechanical work.
# The questions were raised by Tiago Domingos
# regarding Figure 7 of the database paper
# P. E. Brockway, M. K. Heun, Z. Marshall, E. Aramendia, P. Steenwyk, T. Relph, M. Widjanarko,
# J. J. Kim, A. Sainju, and J. Irtube. A country-level primary-final-useful (CL-PFU) energy and exergy
# database: Overview of its construction and 1971–2020 world-level efficiency results. Environmental
# Research: Energy, 1(025005):1–29, jun 2024.
#
# --- Matthew Kuperus Heun, 17 March 2026


# Download the world data from the database with all detail and aggregations

conn <- PFUPipelineTools::get_mexerdb_conn(user = "dbcreator")
world_mats <- PFUPipelineTools::pl_filter_collect(db_table_name = "PSUTReAllChopAllDsAllGrAll",
                                                  Country == "World",
                                                  conn = conn,
                                                  collect = TRUE)
world_mats |>
  saveRDS(file = file.path("report_scripts", "Brockway Paul", "world_mats.rds"))


# Can usually start from here
world_mats <- readRDS(file = file.path("report_scripts", "Brockway Paul", "world_mats.rds"))

world_mats |>
  dplyr::filter(Dataset == "CL-PFU IEA+MW") |>
  dplyr::select(-R, -U, -U_EIOU, -U_feed, -r_EIOU, -V, -S_units) |>
  View()
