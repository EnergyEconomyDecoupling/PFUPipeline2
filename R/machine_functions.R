#' Get all file paths to machine efficiency files
#'
#' The machine efficiency files contain a FIN_ETA sheet
#' that stores all efficiencies..
#'
#' @param filepath A file path to the folder containing all machine folders.
#' @param efficiency_tab_name See `PFUPipeline::machine_constants`.
#' @param hidden_excel_file_prefix The prefix for hidden Excel files.
#'                                 These files appear when an Excel file is open
#'                                 and should be ignored.
#'
#' @return A list of the file paths to machine excel files containing
#'         FIN_ETA front sheets, and therefore usable data.
#'
#' @export
get_eta_filepaths <- function(filepath,
                              efficiency_tab_name = "FIN_ETA",
                              hidden_excel_file_prefix = "~$") {

  if (!file.exists(filepath)) {
    return(list())
  }

  # Lists path to each machine folder
  machine_paths <- list.dirs(path = filepath, recursive = FALSE)

  # Lists each machine excel file in machine_paths
  machine_filepaths <- list.files(machine_paths, recursive = FALSE, full.names = TRUE, pattern = ".xlsx") %>%
    as.list()

  # Keep only those machine_filepaths that point to a file
  # that contains a FIN_ETA tab.
  lapply(machine_filepaths, FUN = function(fp) {
    if (basename(fp) %>% startsWith(hidden_excel_file_prefix)) {
      return(NULL)
    }

    if(efficiency_tab_name %in% readxl::excel_sheets(fp)) {
      return(fp)
    } else {
      return(NULL)
    }
  }) %>%
    purrr::compact()
}


#' Create a data frame containing machine Eta.fu and Phi.u values.
#'
#' This function reads the files in `eta_fin_paths`
#' and creates a data frame of important efficiency and other variables.
#'
#' Note that `eta_fin_paths` should typically be a list of
#' file paths, each a character string.
#' But `eta_fin_paths` can be a single character string (not a list),
#' in which case it will be interpreted as a directory
#' containing files that have Eta.fu and Phi.u values.
#' When `eta_fin_paths` is a single character string (not a list),
#' the directory will be interrogated for files,
#' a list of file paths constructed, and
#' all files read.
#' `get_eta_filepaths()` is called internally before
#' reading the files and creating the data frames.
#'
#' @param eta_fin_paths A list of the file paths to machine excel files containing
#'                      FIN_ETA front sheets, and therefore usable data.
#'                      Created by calling the `get_eta_filepaths()` function.
#' @param version A string containing the version of the database you are creating.
#' @param efficiency_tab_name See `PFUPipeline::machine_constants`.
#' @param year See `IEATools::iea_cols`.
#' @param .values See `IEATools::template_cols`.
#' @param dataset_colname The name of the dataset column in the output file.
#'                        Default is "Dataset"
#' @param hidden_excel_file_prefix The prefix for hidden Excel files.
#'                                 These files appear when an Excel file is open
#'                                 and should be ignored.
#'                                 Default is "~$".
#'
#' @return A data frame containing all Eta.fu and Phi.u values present
#'         in all Machine excel files, with the following column names:
#'         "Country", "Energy.type", "Last.stage", "Method", "Machine",
#'         "Eu.product", "Quantity", "Year", "Value".
#'
#' @export
read_all_eta_files <- function(eta_fin_paths,
                               version,
                               efficiency_tab_name = "FIN_ETA",
                               country = IEATools::iea_cols$country,
                               energy_type = IEATools::iea_cols$energy_type,
                               last_stage = IEATools::iea_cols$last_stage,
                               method = IEATools::iea_cols$method,
                               machine = IEATools::template_cols$machine,
                               eu_product = IEATools::template_cols$eu_product,
                               quantity = IEATools::template_cols$quantity,
                               year = IEATools::iea_cols$year,
                               .values = IEATools::template_cols$.values,
                               dataset_colname = "Dataset",
                               hidden_excel_file_prefix = "~$") {

  # Check if eta_fin_paths is a directory. If so, call get_eta_filepaths() before loading the files.
  if (!is.list(eta_fin_paths)) {
    # Assume this is a directory which contains eta files.
    # Let's load all the paths.
    eta_fin_paths <- get_eta_filepaths(eta_fin_paths)
  }

  # Creates empty tibble to store etas data in
  etas <- tibble::tibble()

  # Loops through each country in eta_fin_paths to add Eta.fu and Phi.u values
  # to etas tibble
  for (path in eta_fin_paths) {

    # Reads raw data, but not if the file starts with hidden_file_prefix
    if (basename(path) %>% startsWith(hidden_excel_file_prefix)) {
      # Break out of this iteration of the for loop.
      # Effectively, "next" means that we skip this "path" and
      # go to the next one.
      next
    }
    raw_etas <- readxl::read_excel(path = path, sheet = efficiency_tab_name)

    # Figure out year columns.
    year_columns <- IEATools::year_cols(raw_etas, return_names = TRUE)

    # Pivots year columns into a "Year" column and a "Value" column
    raw_etas <- raw_etas %>%
      tidyr::pivot_longer(cols = dplyr::all_of(year_columns),
                          names_to = year,
                          values_to = .values) |>
      dplyr::mutate(
        # Add the version column
        "{dataset_colname}" := version
      ) |>
      # Move it to the left
      dplyr::relocate(dplyr::all_of(dataset_colname))

    # Sets column classes
    raw_etas[[country]] <- as.character(raw_etas[[country]])
    raw_etas[[energy_type]] <- as.character(raw_etas[[energy_type]])
    raw_etas[[last_stage]] <- as.character(raw_etas[[last_stage]])
    raw_etas[[method]] <- as.character(raw_etas[[method]])
    raw_etas[[machine]] <- as.character(raw_etas[[machine]])
    raw_etas[[eu_product]] <- as.character(raw_etas[[eu_product]])
    raw_etas[[quantity]] <- as.character(raw_etas[[quantity]])
    raw_etas[[year]] <- as.numeric(raw_etas[[year]])
    raw_etas[[.values]] <- as.numeric(raw_etas[[.values]])

    # Binds values from individual excel FIN_ETA sheet into etas tibble.
    etas <- etas %>%
      dplyr::bind_rows(raw_etas)
  }

  return(etas)

}
