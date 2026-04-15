conn <- PFUPipelineTools::get_mexerdb_conn(user = "dbcreator")
on.exit(DBI::dbDisconnect(conn))

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
  conn = conn)

cat <- PFUPipelineTools::pl_filter_collect(
  db_table_name = "CompletedAllocationTables",
  version_string = "v2.0",
  Dataset == "CL-PFU IEA",
  Year %in% years,
  Country %in% countries,
  create_matsindf = FALSE,
  collect = TRUE,
  conn = conn)

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



