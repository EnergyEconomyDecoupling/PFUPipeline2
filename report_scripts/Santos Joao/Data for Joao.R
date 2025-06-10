# These data are for Joao Santos.
# See Joao's email of 28 May 2025
#
# From what I understood of our email exchange,
# the data in the repository includes the NEU, correct?
# So I would think that, for France (and Austria as well)
# we have been using data with NEU, and
# comparing it with André's data without the NEU
# (in the case of France, specifically).
# So, we would like to get the data from the repository
# without the NEU for France and Austria.
#
# Or, to make things easier and if not more work for you,
# could you send us the data for both France and Austria
# with and without the NEU?
# Both exergy and energy would be useful to us.
#
# Regarding data file, .csv would be fine.

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

agg_eta_pfu <- PFUPipelineTools::pl_filter_collect(db_table_name = "AggEtaPFU",
                                                   Country %in% c("FRA", "AUT"),
                                                   conn = conn,
                                                   collect = TRUE,
                                                   matrix_class = "matrix") |>
  dplyr::filter(LastStage == "Useful",
                ProductAggregation == "Specified",
                IndustryAggregation == "Specified") |>
  dplyr::arrange(Dataset,
                 Country,
                 Year,
                 LastStage,
                 EnergyType,
                 IncludesNEU,
                 ProductAggregation,
                 IndustryAggregation,
                 GrossNet)
write.csv(agg_eta_pfu, file = "~/Desktop/For Joao/agg_eta_pfu.csv", row.names = FALSE)
