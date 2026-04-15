# Following a Teams call with Henrique Matos, Ricardo Pinto, and Tania Sousa
# on 14 April 2026,
# this script downloads allocation and efficiency data
# for a few countries to assist Henrique's MS work.
#
# --- MKH, 15 April 2026

conn <- PFUPipelineTools::get_mexerdb_conn(user = "dbcreator")
on.exit(DBI::dbDisconnect(conn))

henrique_dir <- file.path("~", "Desktop", "For Henrique")

countries <- c("PRT", "WRLD")
years <- 1971:1980

iat <- PFUPipelineTools::pl_filter_collect(
  db_table_name = "IncompleteAllocationTables",
  version_string = "v2.0",
  Dataset == "CL-PFU IEA",
  Year %in% years,
  Country %in% countries,
  create_matsindf = FALSE,
  collect = TRUE,
  conn = conn) |>
  dplyr::arrange(Country, LedgerSide, FlowAggregationPoint, Destination,
                 EfProduct, Machine, EuProduct, Year)
iat |>
  write.csv(file = file.path(henrique_dir, "iat.csv"), row.names = FALSE)

cat <- PFUPipelineTools::pl_filter_collect(
  db_table_name = "CompletedAllocationTables",
  version_string = "v2.0",
  Dataset == "CL-PFU IEA",
  Year %in% years,
  Country %in% countries,
  create_matsindf = FALSE,
  collect = TRUE,
  conn = conn) |>
  dplyr::arrange(Country, LedgerSide, FlowAggregationPoint, Destination,
                 EfProduct, Machine, EuProduct, Year)
cat |>
  write.csv(file = file.path(henrique_dir, "cat.csv"), row.names = FALSE)

md <- PFUPipelineTools::pl_filter_collect(
  db_table_name = "MachineData",
  version_string = "v2.0",
  Dataset == "CL-PFU IEA",
  Year %in% years,
  Country %in% countries,
  create_matsindf = FALSE,
  collect = TRUE,
  conn = conn) |>
  dplyr::arrange(Country, Machine, EuProduct, Year)
md |>
  write.csv(file = file.path(henrique_dir, "md.csv"), row.names = FALSE)

cet <- PFUPipelineTools::pl_filter_collect(
  db_table_name = "CompletedEfficiencyTables",
  version_string = "v2.0",
  Dataset == "CL-PFU IEA",
  Year %in% years,
  Country %in% countries,
  create_matsindf = FALSE,
  collect = TRUE,
  conn = conn) |>
  dplyr::arrange(Country, Machine, EuProduct, Year)
cet |>
  write.csv(file = file.path(henrique_dir, "cet.csv"), row.names = FALSE)



