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

phi_vecs <- PFUPipelineTools::pl_filter_collect(
  version_string = "v2.0",
  db_table_name = "Phivecs",
  Dataset == "CL-PFU",
  Country %in% countries,
  Year %in% years,
  create_matsindf = TRUE,
  collect = TRUE,
  conn = conn)


DBI::dbDisconnect(conn)



allocated_final_energy <- psut_mats_downloaded |>
  # Focus on LastStage Final and energy
  dplyr::filter(LastStage == "Final", EnergyType == "E") |>
  tidyr::pivot_longer(cols = c(U_EIOU, Y),
                      names_to = "matnames",
                      values_to = "matvals") |>
  dplyr::mutate(
    # Make a vector out of the U_EIOU and Y matrices
    matvecs = matsbyname::vectorize_byname(matvals, notation = list(RCLabels::arrow_notation)) |>
      # Put that vector on a diagonal.
      matsbyname::hatize_byname(),
    matvals = NULL # No longer needed.
  ) |>
  tidyr::pivot_wider(names_from = "matnames", values_from = "matvecs") |>
  # Add the allocation matrices to the data frame
  dplyr::full_join(c_mats_downloaded,
                   by = c("ValidFromVersion", "ValidToVersion",
                          "Country", "Method", "EnergyType", "LastStage", "Year")) |>
  dplyr::select(-dplyr::starts_with("Dataset")) |>
  dplyr::mutate(
    AllocatedY = matsbyname::matrixproduct_byname(Y, C_Y),
    AllocatedEIOU = matsbyname::matrixproduct_byname(U_EIOU, C_EIOU)
  )

allocated_final_exergy <- allocated_final_energy |>
  dplyr::full_join(phi_vecs, by = c("ValidFromVersion", "ValidToVersion", "Country", "Year")) |>
  Recca::extend_to_exergy(mat_piece = "noun", phi_piece = "all", tol = tol)



allocated_final_energy |>
  tidyr::pivot_longer(cols = c(U_EIOU, Y, C_EIOU, C_Y,
                               AllocatedY, AllocatedEIOU),
                      names_to = "matnames",
                      values_to = "matvals") |>
  matsindf::expand_to_tidy(drop = 0) |>
  openxlsx2::write_xlsx("~/Desktop/For Joao/Allocated final energy for Joao.xlsx")







psut_mats_rcv <- psut_mats_downloaded |>
  dplyr::arrange(Country, Year, LastStage, EnergyType) |>
  tidyr::pivot_longer(cols = c(U_EIOU, Y),
                      names_to = "matnames",
                      values_to = "matvals") |>
  matsindf::expand_to_tidy(drop = 0)


# Y and U_EIOU matrices in row col val format
psut_mats_rcv |>
  openxlsx::write.xlsx("~/Desktop/rowcolvalues for Joaos.xlsx")






