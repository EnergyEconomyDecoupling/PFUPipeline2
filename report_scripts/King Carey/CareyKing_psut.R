# This script saves PSUT data from the database.
# Carey King (University of Texas Energy Institute)
# requested these data on 2 December 2024.
#
# ---Matthew Kuperus Heun, 4 December 2024

conn_params <- list(dbname = "MexerDB",
                    user = "dbcreator",
                    host = "eviz.cs.calvin.edu",
                    port = 6432)
conn <- DBI::dbConnect(drv = RPostgres::Postgres(),
                       dbname = conn_params$dbname,
                       host = conn_params$host,
                       port = conn_params$port,
                       user = conn_params$user)
on.exit(DBI::dbDisconnect(conn))

psut <- PFUPipelineTools::pl_filter_collect("PSUT",
                                            countries = NULL,
                                            conn = conn,
                                            collect = TRUE,
                                            matrix_class = "matrix") |>
  dplyr::arrange(Year, LastStage, EnergyType)

saveRDS(psut, "~/Desktop/psut.rds")

psut_usa <- psut |>
  dplyr::filter(Country == "USA")

saveRDS(psut_usa, "~/Desktop/psut_usa.rds")

