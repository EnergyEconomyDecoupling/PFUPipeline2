# Following a Teams call with Henrique Matos, Ricardo Pinto, and Tania Sousa
# on 14 April 2026,
# this script downloads allocation and efficiency data
# for a few countries to assist Henrique's MS work.
#
# --- MKH, 15 April 2026

# Following a Teams call with Henrique Matos, Ricardo Pinto, and Tania Sousa
# on 14 May 2026,
# I'm changing the script to supply data data for all years.
# Specifically, I commented the Year %in% years portion of the filter.
#
# ---MKH, 14 May 2026

version_string <- "v3.0"

conn <- PFUPipelineTools::get_scratchmdb_conn()
on.exit(DBI::dbDisconnect(conn))

ember_dir <- file.path("~",
                       "OneDrive - University of Leeds",
                       "Fellowship 1960-2015 PFU database research",
                       "Output Data",
                       version_string,
                       "For Ember")


res <- PFUPipelineTools::pl_filter_collect(
  db_table_name = "PSUTReAllChopAllDsAllGrAll",
  version_string = version_string,
  Dataset == "CL-PFU IEA+MW",
  Year %in% 2010:2020,
  LastStage == "Useful",
  EnergyType == "E",
  IncludesNEU == TRUE,
  ProductAggregation == "Specified",
  IndustryAggregation == "Specified",
  Country == "World",
  create_matsindf = FALSE,
  collect = TRUE,
  conn = conn
)

res |>
  dplyr::rename(`value [TJ]` = value) |>
  write.csv(file = file.path(ember_dir, "CL-PFU_world_2010_2020.csv"),
            row.names = FALSE)




DBI::dbDisconnect(conn)
