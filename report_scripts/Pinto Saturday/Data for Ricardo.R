# Based on a Teams call with Joao Santos and Joao Goncalves
# on 16 Jan 2026, they would like
#
# - **Y** matrix with NEU, energy and exergy versions
# - Start with USA for the first attempt, 1960-1961
# - Show both Y matrices and RCV format
# - Also send row and column sums.

# Based on emails from Joao Santos on 2 and 3 February 2026,
# he requests data in row-col-val format.
#
# "After close inspection,
# I think I could do the work I want with the kind of data shown
# in the rowcolvalues Excel sheet.
# The question is, is it really easy for you to come up
# with the corresponding data for multiple years/countries?
# I'm interested in as many as possible."
#
# Further, he requests the following countries:
# AUT, BEL, DNK, FIN, FRA, DEU, GRC, IRL, ITA, LUX, NLD, PRT, ESP, SWE, GBR.
# Basically, Europe so he can compare to Andre's original paper.
#
# On 20 May 2026, I'm working on the calculations Joao requested
# in an email dated 10 April 2026


conn <- PFUPipelineTools::get_scratchmdb_conn()
on.exit(DBI::dbDisconnect(conn))

countries <- "World"

v_string <- "v3.0"
output_folder <- file.path("~",
                           "OneDrive - University of Leeds",
                           "Fellowship 1960-2015 PFU database research",
                           "Output Data",
                           v_string,
                           "For Ricardo Pinto")
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
  matname %in% c("U", "V"),
  create_matsindf = FALSE,
  collect = TRUE,
  conn = conn)

DBI::dbDisconnect(conn)


psut_mats_downloaded |>
  write.csv(file = file.path(output_folder,
                             "U and V matrices for Ricardo.csv"),
            row.names = FALSE)
