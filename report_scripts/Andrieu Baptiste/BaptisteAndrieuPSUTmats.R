# This script saves PSUT matrices .rds files for
# Baptiste Andrieu (Cambridge University).
# Baptiste requested these data at the Exergy Economics
# workshop in Leeds, England in June 2025 and
# in a followup email from 12 June 2025.
#
# ---Matthew Kuperus Heun, 26 June 2025

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


PSUT_FRA_2020 <- PFUPipelineTools::pl_filter_collect("PSUTReAllChopAllDsAllGrAll",
                                                     Country == "FRA",
                                                     Year == 2020,
                                                     ProductAggregation == "Despecified",
                                                     IndustryAggregation == "Despecified",
                                                     conn = conn,
                                                     collect = TRUE,
                                                     matrix_class = "matrix") |>
  dplyr::mutate(
    Dataset = factor(Dataset, levels = c("CL-PFU IEA", "CL-PFU MW", "CL-PFU IEA+MW"))
  ) |>
  dplyr::arrange(Country, Year, EnergyType, LastStage, IncludesNEU, Dataset)

saveRDS(PSUT_FRA_2020, "~/Desktop/psut_fra_2020_for_baptiste.rds")


# To read this file in R, use code like the following:
foo <- readRDS("~/Desktop/psut_fra_2020_for_baptiste.rds")
# To look at the matrices, use code like the following in the Console of RStudio:
foo$R[[1]] |> View()
