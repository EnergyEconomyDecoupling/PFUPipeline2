#' Get all file paths to machine efficiency files
#'
#' The machine efficiency files contain a FIN_ETA sheet
#' that stores all efficiencies..
#'
#' @param filepath A file path to the folder containing all machine folders.
#' @param efficiency_tab_name See `PFUPipelineTools::machine_constants`.
#' @param hidden_excel_file_prefix The prefix for hidden Excel files.
#'                                 These files appear when an Excel file is open
#'                                 and should be ignored.
#'
#' @return A list of the file paths to machine excel files containing
#'         FIN_ETA front sheets, and therefore usable data.
#'
#' @export
get_eta_filepaths <- function(filepath,
                              efficiency_tab_name = PFUPipelineTools::machine_constants$efficiency_tab_name,
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
#' @param efficiency_tab_name See `PFUPipelineTools::machine_constants`.
#' @param year See `IEATools::iea_cols`.
#' @param .values See `IEATools::template_cols`.
#' @param dataset_colname The name of the dataset column in the output file.
#'                        Default is `PFUPipelineTools::dataset_info$dataset_colname`.
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
                               dataset,
                               efficiency_tab_name = PFUPipelineTools::machine_constants$efficiency_tab_name,
                               country = IEATools::iea_cols$country,
                               energy_type = IEATools::iea_cols$energy_type,
                               last_stage = IEATools::iea_cols$last_stage,
                               method = IEATools::iea_cols$method,
                               machine = IEATools::template_cols$machine,
                               eu_product = IEATools::template_cols$eu_product,
                               quantity = IEATools::template_cols$quantity,
                               year = IEATools::iea_cols$year,
                               .values = IEATools::template_cols$.values,
                               dataset_colname = PFUPipelineTools::dataset_info$dataset_colname,
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
        # Add the dataset column
        "{dataset_colname}" := dataset
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


#' Assemble completed final-to-useful efficiency tables
#'
#' This function is used in a drake workflow to assemble completed final-to-useful efficiency tables
#' given a set of incomplete efficiency tables.
#' Information from exemplar countries is used to complete incomplete final-to-useful efficiency tables.
#' See examples for how to construct `exemplar_lists`.
#'
#' Note that this function can accept tidy or wide by year data frames.
#' The return value is a tidy data frame.
#'
#' Note that the `.values` argument applies for both
#' `incomplete_eta_fu_tables` and
#' `completed_fu_allocation_tables`.
#' Callers should ensure that value columns in both
#' data frames (`incomplete_eta_fu_tables` and `completed_fu_allocation_tables`)
#' are named identically and that name is passed into the
#' `.values` argument.
#'
#' Note that the `which_quantity` argument is an accident of history.
#' At one time, this function also assembled tables
#' of `phi.u` (useful exergy-to-energy ratio) values.
#' At present, the function only assembles `eta.fu` (final-to-useful efficiency) tables,
#' so the only valid value for `which_quantity` is `IEATools::template_cols$eta_fu`.
#'
#' @param incomplete_eta_fu_tables An incomplete data frame of final-to-useful efficiencies for all Machines in `completed_fu_allocation_tables`.
#' @param exemplar_lists A data frame containing `country` and `year` columns along with a column of ordered vectors of strings
#'                       telling which countries should be considered exemplars for the country and year of this row.
#' @param completed_fu_allocation_tables A data frame containing completed final-to-useful allocation data,
#'                                       typically the result of calling `assemble_fu_allocation_tables`.
#' @param version The version of the database being created.
#' @param countries A vector of countries for which completed final-to-useful allocation tables are to be assembled.
#' @param years The years for which analysis is desired. Default is `NULL`, meaning analyze all years.
#' @param which_quantity A vector of quantities to be completed in the eta_FU table.
#'                       Default is `c(IEATools::template_cols$eta_fu, IEATools::template_cols$phi_u)`.
#'                       Must be one or both of the default values.
#' @param country,method,energy_type,last_stage,year,unit,e_dot See `IEATools::iea_cols`.
#' @param machine,eu_product,eta_fu,phi_u,c_source,eta_fu_source,e_dot_machine,e_dot_machine_perc,quantity,maximum_values,e_dot_perc,.values See `IEATools::template_cols`.
#' @param exemplars,exemplar_tables,alloc_data,incomplete_eta_tables,complete_eta_tables See `PFUPipelineTools::exemplar_names`.
#' @param dataset See `PFUPipelineTools::dataset_info`.
#'
#' @return A tidy data frame containing completed final-to-useful efficiency tables.
#'
#' @export
#'
#' @examples
#' # Make some incomplete efficiency tables for GHA by removing Wood cookstoves.
#' # Information from the exemplar, ZAF, will supply efficiency for Wood cookstoves for GHA.
#' incomplete_eta_fu_tables <- IEATools::load_eta_fu_data() %>%
#'   dplyr::filter(! (Country == "GHA" & Machine == "Wood cookstoves"))
#' # The rows for Wood cookstoves are missing.
#' incomplete_eta_fu_tables %>%
#'   dplyr::filter(Country == "GHA", Machine == "Wood cookstoves")
#' # Set up exemplar list
#' el <- tibble::tribble(
#'   ~Country, ~Year, ~Exemplars,
#'   "GHA", 1971, c("ZAF"),
#'   "GHA", 2000, c("ZAF"))
#' # Load FU allocation data.
#' # An efficiency is needed for each machine in FU allocation data.
#' fu_allocation_data <- IEATools::load_fu_allocation_data()
#' # Assemble complete allocation tables
#' completed <- assemble_eta_fu_tables(incomplete_eta_fu_tables = incomplete_eta_fu_tables,
#'                                     exemplar_lists = el,
#'                                     completed_fu_allocation_tables = fu_allocation_data,
#'                                     countries = "GHA")
#' # The missing rows have been picked up from the exemplar country, ZAF.
#' completed %>%
#'   dplyr::filter(Country == "GHA", Machine == "Wood cookstoves")
assemble_eta_fu_tables <- function(incomplete_eta_fu_tables,
                                   exemplar_lists,
                                   completed_fu_allocation_tables,
                                   dataset,
                                   countries,
                                   years = NULL,
                                   which_quantity = c(IEATools::template_cols$eta_fu),
                                   country = IEATools::iea_cols$country,
                                   method = IEATools::iea_cols$method,
                                   energy_type = IEATools::iea_cols$energy_type,
                                   last_stage = IEATools::iea_cols$last_stage,
                                   unit = IEATools::iea_cols$unit,
                                   year = IEATools::iea_cols$year,
                                   e_dot = IEATools::iea_cols$e_dot,

                                   machine = IEATools::template_cols$machine,
                                   eu_product = IEATools::template_cols$eu_product,
                                   eta_fu = IEATools::template_cols$eta_fu,
                                   phi_u = IEATools::template_cols$phi_u,
                                   c_source = IEATools::template_cols$c_source,
                                   eta_fu_source = IEATools::template_cols$eta_fu_source,
                                   e_dot_machine = IEATools::template_cols$e_dot_machine,
                                   e_dot_machine_perc = IEATools::template_cols$e_dot_machine_perc,
                                   quantity = IEATools::template_cols$quantity,
                                   maximum_values = IEATools::template_cols$maximum_values,
                                   e_dot_perc = IEATools::template_cols$e_dot_perc,

                                   exemplars = PFUPipelineTools::exemplar_names$exemplars,
                                   exemplar_tables = PFUPipelineTools::exemplar_names$exemplar_tables,
                                   alloc_data = PFUPipelineTools::exemplar_names$alloc_data,
                                   incomplete_eta_tables = PFUPipelineTools::exemplar_names$incomplete_eta_table,
                                   complete_eta_tables = PFUPipelineTools::exemplar_names$complete_eta_table,

                                   .values = IEATools::template_cols$.values,
                                   dataset_colname = PFUPipelineTools::dataset_info$dataset_colname) {

  which_quantity <- match.arg(which_quantity, several.ok = FALSE)

  # The FU allocation tables and the incomplete efficiency tables are easier to deal with when they are tidy.
  tidy_incomplete_eta_fu_tables <- IEATools::tidy_eta_fu_table(incomplete_eta_fu_tables,
                                                               year = year,
                                                               e_dot_machine = e_dot_machine,
                                                               e_dot_machine_perc = e_dot_machine_perc,
                                                               quantity = quantity,
                                                               maximum_values = maximum_values,
                                                               .values = .values) |>
    dplyr::filter(.data[[year]] %in% years) |>
    dplyr::mutate(
      # Eliminate the dataset column for now.
      "{dataset_colname}" := NULL
    ) |>
    PFUPipelineTools::tar_ungroup()

  tidy_allocation_tables <- IEATools::tidy_fu_allocation_table(completed_fu_allocation_tables,
                                                               year = year,
                                                               e_dot = e_dot,
                                                               e_dot_perc = e_dot_perc,
                                                               quantity = quantity,
                                                               maximum_values = maximum_values,
                                                               .values = .values) |>
    dplyr::filter(.data[[year]] %in% years) |>
    dplyr::mutate(
      # Eliminate the dataset column for now.
      "{dataset}" := NULL
    ) |>
    PFUPipelineTools::tar_ungroup()

  completed_tables_by_year <- lapply(countries, FUN = function(coun) {
    coun_exemplar_strings <- exemplar_lists %>%
      dplyr::filter(.data[[country]] == coun)

    # For each combination of Country and Year (the rows of coun_exemplar_strings),
    # assemble a list of country allocation tables.
    coun_exemplar_strings_and_tables <- coun_exemplar_strings %>%
      dplyr::mutate(
        # Create a list column containing lists of exemplar tables
        # corresponding to the countries in the Exemplars column.
        "{exemplar_tables}" := Map(get_one_exemplar_table_list,
                                   # Need to wrap this in a list so the WHOLE table is sent via Map to get_one_exemplar_table_list
                                   tidy_incomplete_tables = list(tidy_incomplete_eta_fu_tables),
                                   exemplar_strings = .data[[exemplars]],
                                   yr = .data[[year]],
                                   country_colname = country,
                                   year_colname = year),
        # Add a column containing an FU allocation data frame for the country and year of each row
        "{alloc_data}" := Map(get_one_df_by_coun_and_yr,
                              .df = list(tidy_allocation_tables),
                              coun = .data[[country]],
                              yr = .data[[year]],
                              country_colname = country,
                              year_colname = year),
        # Add a column containing incomplete fu eta tables for each row (i.e., for each combination of country and year).
        "{incomplete_eta_tables}" := Map(get_one_df_by_coun_and_yr,
                                         .df = list(tidy_incomplete_eta_fu_tables),
                                         coun = .data[[country]],
                                         yr = .data[[year]],
                                         country_colname = country,
                                         year_colname = year),
        # Add a column containing completed fu efficiency tables for each row (i.e., for each combination of country and year).
        # Note that the data frames in this column contain the SOURCE of information for each efficiency
        "{complete_eta_tables}" := Map(IEATools::complete_eta_fu_table,
                                       eta_fu_table = .data[[incomplete_eta_tables]],
                                       exemplar_eta_fu_tables = .data[[exemplar_tables]],
                                       fu_allocation_table = .data[[alloc_data]],
                                       which_quantity = list(which_quantity),

                                       country = country,
                                       method = method,
                                       energy_type = energy_type,
                                       last_stage = last_stage,
                                       e_dot = e_dot,
                                       unit = unit,
                                       year = year,
                                       machine = machine,
                                       eu_product = eu_product,
                                       e_dot_perc = e_dot_perc,
                                       e_dot_machine = e_dot_machine,
                                       e_dot_machine_perc = e_dot_machine_perc,
                                       eta_fu = eta_fu,
                                       phi_u = phi_u,
                                       quantity = quantity,
                                       maximum_values = maximum_values,
                                       c_source = c_source,
                                       eta_fu_source = eta_fu_source,
                                       .values = .values)
      )
  }) %>%
    dplyr::bind_rows()

  # The only information we need to return is the completed efficiency tables.
  # Expand (un-nest) only the completed efficiency table column to give one data frame of all the FU efficiencies
  # for all years and all countries.
  completed_tables_by_year %>%
    dplyr::select(dplyr::all_of(complete_eta_tables)) %>%
    tidyr::unnest(cols = dplyr::all_of(complete_eta_tables)) |>
    dplyr::mutate(
      "{dataset_colname}" := dataset
    ) |>
    dplyr::relocate(dplyr::all_of(dataset_colname))
}
