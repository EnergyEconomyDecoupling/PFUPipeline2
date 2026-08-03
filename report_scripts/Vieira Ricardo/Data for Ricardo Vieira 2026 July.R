# Based on a conversation with Ricardo Vieira at lunch on
# 16 July 2026, he needs
#
# - **Y** matrix with NEU, energy and exergy versions
# - All countries
# - Show both Y matrices and RCV format in a .csv file

conn <- PFUPipelineTools::get_mexerdb_conn(user = "dbcreator")
on.exit(DBI::dbDisconnect(conn))

countries <- unlist(PFUPipelineTools::canonical_countries)

psut_mats_downloaded <- PFUPipelineTools::pl_filter_collect(
  version_string = "v2.0",
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

psut_mats_downloaded |>
  write.csv(file = file.path("~",
                             "Desktop",
                             "For Ricardo Vieira",
                             "Y and U_EIOU matrices for Ricardo Vieira.csv"))








