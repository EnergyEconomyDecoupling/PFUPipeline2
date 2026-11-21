# This script calculates
# C_Y and C_EIOU matrices
# for all countries and all years and
# compares to allocation matrices
# already in the database.
#
# If this script runs to completion without stopping,
# it demonstrates that we have the correct
# formula/algorithm to calculate C matrices
# from existing data in the database
# for those cases where we know the answer.
#
# That should provide confidence for calculating
# C_Y and C_U_EIOU for aggregated regions
# where we have not allocation matrices.

conn <- PFUPipelineTools::get_scratchmdb_conn()
# conn <- PFUPipelineTools::get_mexerdb_conn(user = "dbcreator")
on.exit(DBI::dbDisconnect(conn))

versn_string <- "v3.0"
years <- 1960:2020

tol <- 1e-10

countries <- PFUPipelineTools::canonical_countries

for (this_country in countries) {

  # Download data from the database
  print("")
  print(paste0("Downloading ", this_country, " Cmats"))
  C_this_country <- PFUPipelineTools::pl_filter_collect(
    db_table_name = "Cmats",
    version_string = versn_string,
    Dataset == "CL-PFU IEA",
    Year %in% years,
    Country == this_country,
    create_matsindf = TRUE,
    collect = TRUE,
    conn = conn)

  print(paste0("Downloading ", this_country, " RUVY matrices"))
  Yf_this_country <- PFUPipelineTools::pl_filter_collect(
    db_table_name = "PSUTReAllChopAllDsAllGrAll",
    version_string = versn_string,
    Dataset == "CL-PFU IEA",
    EnergyType == "E",
    LastStage == "Final",
    IncludesNEU == TRUE,
    ChoppedMat == "None",
    ChoppedVar == "None",
    ProductAggregation == "Specified",
    IndustryAggregation == "Specified",
    Year %in% years,
    Country == this_country,
    create_matsindf = TRUE,
    collect = TRUE,
    conn = conn) |>
    dplyr::select(dplyr::all_of(c("Dataset", "ValidFromVersion", "ValidToVersion", "Country",
                                  "Method", "EnergyType", "LastStage", "IncludesNEU", "Year",
                                  "ChoppedMat", "ChoppedVar", "ProductAggregation",
                                  "IndustryAggregation", "Y", "U_EIOU")))

  print(paste0("Downloading ", this_country, " YfuUEIOUfudetails matrices"))
  YfuUEIOUfudetails_this_country <- PFUPipelineTools::pl_filter_collect(
    db_table_name = "YfuUEIOUfudetails",
    version_string = versn_string,
    Dataset == "CL-PFU IEA",
    EnergyType == "E",
    Year %in% years,
    Country == this_country,
    create_matsindf = TRUE,
    collect = TRUE,
    conn = conn)

  print(paste0("Downloading ", this_country, " etai matrices"))
  etai_this_country <- PFUPipelineTools::pl_filter_collect(
    db_table_name = "Etai",
    version_string = versn_string,
    Dataset == "CL-PFU IEA",
    EnergyType == "E",
    Year %in% years,
    Country == this_country,
    IncludesNEU == TRUE,
    LastStage == "Useful",
    create_matsindf = TRUE,
    collect = TRUE,
    conn = conn)

  print(paste0("Testing ", this_country))

  # Make a single data frame

  df_this_country <- Yf_this_country |>
    dplyr::rename(
      Y___Final = Y,
      U_EIOU___Final = U_EIOU
    ) |>
    dplyr::mutate(
      LastStage = "FU"
    ) |>
    dplyr::left_join(YfuUEIOUfudetails_this_country |>
                       dplyr::mutate(LastStage = "FU"),
                     by = c("Dataset", "ValidFromVersion", "ValidToVersion",
                            "Country", "Method", "EnergyType", "LastStage", "Year")) |>
    dplyr::left_join(etai_this_country |>
                       dplyr::mutate(LastStage = "FU"),
                     by = c("Dataset", "ValidFromVersion", "ValidToVersion",
                            "Country", "Method", "EnergyType", "LastStage", "IncludesNEU", "Year")) |>
    dplyr::left_join(C_this_country |>
                       dplyr::mutate(
                         LastStage = "FU") |>
                       dplyr::rename(C_Y_orig = dplyr::any_of("C_Y"),
                                     C_EIOU_orig = dplyr::any_of("C_EIOU")),
                     by = c("Dataset", "ValidFromVersion", "ValidToVersion",
                            "Country", "Method", "EnergyType", "LastStage", "Year")) |>
    Recca::calc_C_Y_C_EIOU()
  # Check results, but only if the matrices exist.
  if (("C_Y" %in% colnames(df_this_country)) & ("C_Y_orig" %in% colnames(df_this_country))) {
    df_this_country <- df_this_country |>
      dplyr::mutate(
        EqualC_Y = matsbyname::difference_byname(C_Y, C_Y_orig) |>
          matsbyname::iszero_byname(tol = tol)
      )
    stopifnot(all(unlist(df_this_country$EqualC_Y)))
  }
  if (("C_EIOU" %in% colnames(df_this_country)) & ("C_EIOU_orig" %in% colnames(df_this_country))) {
    df_this_country <- df_this_country |>
      dplyr::mutate(
        EqualC_EIOU = matsbyname::difference_byname(C_EIOU, C_EIOU_orig) |>
          matsbyname::iszero_byname(tol = tol)
      )
    stopifnot(all(unlist(df_this_country$EqualC_EIOU)))
  }
  print(paste0(this_country, ": worked!"))
}

print("Success!")
DBI::dbDisconnect(conn)
