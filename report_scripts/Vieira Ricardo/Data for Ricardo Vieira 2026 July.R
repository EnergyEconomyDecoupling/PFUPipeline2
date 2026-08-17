# Based on a conversation with Ricardo Vieira at lunch on
# 16 July 2026, he needs
#
# - **Y** matrix with NEU, energy and exergy versions
# - All countries
# - Show both Y matrices and RCV format in a .csv file

conn <- PFUPipelineTools::get_scratchmdb_conn()
on.exit(DBI::dbDisconnect(conn))

countries <- unlist(PFUPipelineTools::canonical_countries)

v_string <- "v3.0"
output_folder <- file.path("~",
                           "OneDrive - University of Leeds",
                           "Fellowship 1960-2015 PFU database research",
                           "Output Data",
                           v_string,
                           "For Ricardo Vieira")
if (!file.exists(output_folder)) {
  dir.create(output_folder)
}

psut_mats_downloaded <- PFUPipelineTools::pl_filter_collect(
  version_string = v_string,
  db_table_name = "PSUTReAllChopAllDsAllGrAll",
  Dataset == "CL-PFU IEA+MW",
  ProductAggregation == "Specified",
  IndustryAggregation == "Specified",
  Country %in% countries,
  IncludesNEU == TRUE,
  matname %in% c("U_EIOU", "Y"),
  create_matsindf = FALSE,
  collect = TRUE,
  conn = conn)

DBI::dbDisconnect(conn)

psut_mats_downloaded |>
  write.csv(file = file.path(output_folder,
                             "Y and U_EIOU matrices for Ricardo Vieira.csv"))








