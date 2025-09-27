# These data are for Sofia Henriques.
# See Sofia's email of 14 July 2025.
#
# I am writing to you to ask you about the possibility to request
# the useful data  from the dataset 1960 until latest year
# or a few countries: Sweden, Portugal, Denmark, Spain and the UK. Is this possible?

conn_params <- list(dbname = "ScratchMDB",
                    user = "dbcreator",
                    host = "mexer.site",
                    port = 6432)
conn <- DBI::dbConnect(drv = RPostgres::Postgres(),
                       dbname = conn_params$dbname,
                       host = conn_params$host,
                       port = conn_params$port,
                       user = conn_params$user)
on.exit(DBI::dbDisconnect(conn))

downloaded <- PFUPipelineTools::pl_filter_collect(
  db_table_name = "PSUTReAllChopAllDsAllGrAll",
  version_string = "v2.1a2",
  Dataset == "CL-PFU IEA",
  Country %in% c("SWE", "PRT", "DNK", "ESP", "GBR"),
  matname %in% c("Y", "U_EIOU"),
  LastStage == "Useful",
  ProductAggregation == "Specified",
  IndustryAggregation == "Specified",
  conn = conn,
  create_matsindf = FALSE,
  collect = TRUE)

DBI::dbDisconnect(conn)

downloaded |>
  dplyr::rename(Carrier = "i", Sector = "j") |>
  dplyr::arrange(Country, EnergyType, IncludesNEU, matname, Year) |>
  openxlsx2::write_xlsx(file = file.path("~",
                                         "Desktop",
                                         "For Sofia",
                                         "Useful enxergy from SWE PRT DNK ESP GBR for Sofia.xlsx"))




