# These data are for Gregor Semieniuk.
# He wrote (on 31 January 2025):
# What I’m after is an 1971-last year
# annual total primary, final, and if available
# useful energy consumption for all countries in your data.
# I am not interested in sectoral or by fuel disaggregation.

conn_params <- list(dbname = "MexerDB",
                    user = "dbcreator",
                    host = "mexer.site",
                    port = 6432)
conn <- DBI::dbConnect(drv = RPostgres::Postgres(),
                       dbname = conn_params$dbname,
                       host = conn_params$host,
                       port = conn_params$port,
                       user = conn_params$user)
on.exit(DBI::dbDisconnect(conn))


agg_eta_pfu <- PFUPipelineTools::pl_filter_collect("AggEtaPFU",
                                                   countries = NULL,
                                                   conn = conn,
                                                   collect = TRUE,
                                                   matrix_class = "matrix") |>
  dplyr::filter(ProductAggregation == "Grouped",
                IndustryAggregation == "Grouped",
                LastStage == "Useful") |>
  dplyr::arrange(Dataset, Country, Year, EnergyType, GrossNet)

agg_eta_pfu |>
  write.csv("~/Desktop/For Gregor/AggEtaPFU.csv")



