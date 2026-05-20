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


conn <- PFUPipelineTools::get_mexerdb_conn(user = "dbcreator")
on.exit(DBI::dbDisconnect(conn))

# countries <- c("AUT", "BEL", "DNK", "FIN", "FRA", "DEU", "GRC",
#                "IRL", "ITA", "LUX", "NLD", "PRT", "ESP", "SWE", "GBR")

countries <- c("AUT", "BEL")

years <- 1971:1972

psut_mats_downloaded <- PFUPipelineTools::pl_filter_collect(
  version_string = "v2.0",
  db_table_name = "PSUTReAllChopAllDsAllGrAll",
  Dataset == "CL-PFU IEA+MW",
  ProductAggregation == "Specified",
  IndustryAggregation == "Specified",
  Country %in% countries,
  Year %in% years,
  IncludesNEU == TRUE,
  matname %in% c("U_EIOU", "Y"),
  create_matsindf = TRUE,
  collect = TRUE,
  conn = conn)

c_mats_downloaded <- PFUPipelineTools::pl_filter_collect(
  version_string = "v2.0",
  db_table_name = "Cmats",
  Dataset == "CL-PFU IEA",
  Country %in% countries,
  Year %in% years,
  matname %in% c("C_EIOU", "C_Y"),
  create_matsindf = TRUE,
  collect = TRUE,
  conn = conn)


DBI::dbDisconnect(conn)



matvecs <- psut_mats_downloaded |>
  dplyr::filter(LastStage == "Final", EnergyType == "E") |>
  dplyr::full_join(c_mats_downloaded,
                   by = c("ValidFromVersion", "ValidToVersion",
                          "Country", "Method", "EnergyType", "LastStage", "Year")) |>
  dplyr::select(-dplyr::starts_with("Dataset")) |>
  tidyr::pivot_longer(cols = c(U_EIOU, Y, C_EIOU, C_Y),
                      names_to = "matnames",
                      values_to = "matvals") |>
  dplyr::mutate(
    matvecs = matsbyname::vectorize_byname(matvals, notation = list(RCLabels::arrow_notation))
  )






psut_mats_rcv <- psut_mats_downloaded |>
  dplyr::arrange(Country, Year, LastStage, EnergyType) |>
  tidyr::pivot_longer(cols = c(U_EIOU, Y),
                      names_to = "matnames",
                      values_to = "matvals") |>
  matsindf::expand_to_tidy(drop = 0)


# Y and U_EIOU matrices in row col val format
psut_mats_rcv |>
  openxlsx::write.xlsx("~/Desktop/rowcolvalues for Joaos.xlsx")






