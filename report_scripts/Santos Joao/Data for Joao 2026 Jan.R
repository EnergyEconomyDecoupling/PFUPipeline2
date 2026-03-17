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

conn <- PFUPipelineTools::get_mexerdb_conn(user = "dbcreator")
on.exit(DBI::dbDisconnect(conn))

countries <- c("AUT", "BEL", "DNK", "FIN", "FRA", "DEU", "GRC",
               "IRL", "ITA", "LUX", "NLD", "PRT", "ESP", "SWE", "GBR")




# psut_mats_downloaded <- PFUPipelineTools::pl_filter_collect(db_table_name = "PSUTReAllChopAllDsAllGrAll",
#                                                             Dataset == "CL-PFU IEA+MW",
#                                                             ProductAggregation == "Specified",
#                                                             IndustryAggregation == "Specified",
#                                                             Country %in% countries,
#                                                             IncludesNEU == TRUE,
#                                                             matname == "Y",
#                                                             create_matsindf = FALSE,
#                                                             collect = TRUE,
#                                                             conn = conn)

psut_mats_downloaded <- PFUPipelineTools::pl_filter_collect(db_table_name = "PSUTReAllChopAllDsAllGrAll",
                                                            Dataset == "CL-PFU IEA+MW",
                                                            ProductAggregation == "Specified",
                                                            IndustryAggregation == "Specified",
                                                            Country %in% countries,
                                                            IncludesNEU == TRUE,
                                                            matname == "Y",
                                                            create_matsindf = FALSE,
                                                            collect = TRUE,
                                                            conn = conn)


DBI::dbDisconnect(conn)

psut_mats <- psut_mats_downloaded |>
  dplyr::arrange(Country, Year, LastStage, EnergyType) # |>
  # dplyr::mutate(
  #   WorksheetNames = paste(Country, Year, LastStage, EnergyType, sep = "-")
  # )

# ECC and XCC matrices
# psut_mats |>
#   Recca::write_ecc_to_excel(path = "~/Desktop/PSUT mats for Joaos.xlsx", worksheet_names = "WorksheetNames")

# Y matrices in row col val format
psut_mats |>
  openxlsx::write.xlsx("~/Desktop/rowcolvalues for Joaos.xlsx")
# psut_mats |>
#   write.csv("~/Desktop/rowcolvalues for Joaos.csv")

# psut_mats |>
#   dplyr::mutate(
#     # Eliminate unneeded matrix columns
#     R = NULL,
#     U = NULL,
#     V = NULL,
#     U_feed = NULL,
#     U_EIOU = NULL,
#     r_EIOU = NULL,
#     S_units = NULL
#   ) |>
#   tidyr::pivot_longer(cols = Y, names_to = "matnames", values_to = "matvals") |>
#   matsindf::expand_to_tidy(drop = 0) |>
#   dplyr::mutate(
#     rowtypes = NULL,
#     coltypes = NULL
#   ) |>
#   dplyr::rename(
#     values = "matvals"
#   ) |>
#   openxlsx::write.xlsx("~/Desktop/rowcolvalues for Joaos.xlsx")


# Row and column sums of the Y matrix
# psut_mats |>
#   dplyr::mutate(
#     # Eliminate unneeded matrix columns
#     R = NULL,
#     U = NULL,
#     V = NULL,
#     U_feed = NULL,
#     U_EIOU = NULL,
#     r_EIOU = NULL,
#     S_units = NULL,
#     # Calculate the row and column sums
#     rowsums = Y |>
#       matsbyname::rowsums_byname(colname = "rowsums"),
#     colsums = Y |>
#       matsbyname::colsums_byname(rowname = "colsums"),
#     # Get rid of the Y matrices, as we no longer need them
#     Y = NULL
#   ) |>
#   tidyr::pivot_longer(cols = c(rowsums, colsums), names_to = "matnames", values_to = "matvals") |>
#   matsindf::expand_to_tidy(drop = 0) |>
#   dplyr::rename(
#     Value = "matvals",
#     SumType = "matnames"
#   ) |>
#   dplyr::mutate(
#     RowColName = dplyr::case_when(
#       SumType == "rowsums" ~ rownames,
#       SumType == "colsums" ~ colnames
#     ),
#     rownames = NULL,
#     colnames = NULL,
#     rowtypes = NULL,
#     coltypes = NULL
#   ) |>
#   dplyr::relocate(Value, .after = dplyr::last_col()) |>
#   openxlsx::write.xlsx("~/Desktop/rowcolsums for Joaos.xlsx")




