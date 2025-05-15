
#' Load FAO data
#'
#' Steps to download FAO crop and livestock data:
#'
#' * Navigate to https://www.fao.org/faostat/
#' * Click the "Bulk Download" button
#' * Look in the downloaded folders for the file Trade_CropsLivestock_E_All_Data_(Normalized).zip
#' * Unzip Trade_CropsLivestock_E_All_Data_(Normalized).zip
#' * Move the file SUA_Crops_Livestock_E_All_Data_(Normalized).csv into this folder
#'   (Fellowship 1960-2015 PFU database research/InputData/vx.x/FAOSTAT)
#'
#' Alternatively,
#' * copy-paste this URL into a browser:
#'   https://fenixservices.fao.org/faostat/static/bulkdownloads/SUA_Crops_Livestock_E_All_Data_(Normalized).zip
#' * Move the file SUA_Crops_Livestock_E_All_Data_(Normalized).csv into this folder
#'   (Fellowship 1960-2015 PFU database research/InputData/vx.x/FAOSTAT)
#'
#' @param fao_data_path The string path to the FAO data.
#'
#' @return A tidy data frame
#'
#' @export
load_fao_data <- function(fao_data_path)  {
  # data.table::fread(file = fao_data_path) |>
  #   MWTools::tidy_fao_live_animals()
  # internal_file <- basename(fao_data_path) |>
  #   tools::file_path_sans_ext() |>
  #   paste0(".csv")
  # data.table::fread(file = unzip(fao_data_path, internal_file)) |>
  #   tibble::as_tibble()

  readRDS(fao_data_path)
}
