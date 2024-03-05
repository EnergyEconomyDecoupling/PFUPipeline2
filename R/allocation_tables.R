#' Load FU allocation tables
#'
#' This function reads all final-to-useful allocation data
#' in files in the `fu_analysis_folder` that start with the country abbreviations
#' given in `countries`.
#'
#' By default, it is assumed that each country's final-to-useful analysis file will be in a subfolder
#' of `fu_analysis_path`.
#' Set `use_subfolders` to `FALSE` to change the default behavior.
#'
#' If final-to-useful allocation data are not available, this function
#' automatically creates an empty final-to-useful allocation template and writes it to disk.
#' Then, this function reads the empty file.
#' This behavior can be modified by setting argument `generate_missing_fu_allocation_template` to `FALSE`.
#'
#' @param fu_analysis_folder The folder from which final-to-useful analyses will be loaded.
#' @param specified_iea_data A data frame of specified IEA data for `countries`.
#' @param countries The countries for which allocation tables should be loaded.
#' @param file_suffix The suffix for the FU analysis files. Default is "`r IEATools::fu_analysis_file_info$fu_analysis_file_suffix`".
#' @param use_subfolders Tells whether to look for files in subfolders named by `countries`. Default is `TRUE`.
#' @param generate_missing_fu_allocation_template Tells whether to generate a missing final-to-useful allocation template from `specified_iea_data`. Default is `TRUE`.
#' @param conn The database connection.
#' @param schema The data model (`dm` object) for the database in `conn`.
#'               See details.
#' @param fk_parent_tables A named list of all parent tables
#'                         for the foreign keys in `db_table_name`.
#'                         See details.
#' @param fu_allocations_tab_name The name of the tab for final-to-useful allocations in the Excel file containing final-to-useful allocation data. Default is "`r IEATools::fu_analysis_file_info$fu_allocation_tab_name`".
#' @param country The string name of the country column.
#'                Default is `IEATools::iea_cols$country`.
#' @param table_name_colname The name of a column in `specified_iea_data` that gives
#'                           the table in the database in which specified IEA data are to be found.
#'                           Default is `PFUPipelineTools::hashed_table_colnames$db_table_name`.
#'
#' @export
#'
#' @return A data frame of tidy FU Allocation tables read by `IEATools::load_fu_allocation_data()`.
#'         If no FU Allocation data are found and `generate_missing_fu_allocation_template` is `TRUE`,
#'         an empty template written to disk and the empty template is returned.
#'         If no FU Allocation data are found and `generate_missing_fu_allocation_template` is `FALSE`,
#'         `NULL` is returned.
load_fu_allocation_tables <- function(fu_analysis_folder,
                                      specified_iea_data,
                                      countries,
                                      file_suffix = IEATools::fu_analysis_file_info$fu_analysis_file_suffix,
                                      use_subfolders = TRUE,
                                      generate_missing_fu_allocation_template = TRUE,
                                      version = paste0("CLPFU", clpfu_version),
                                      dataset_colname = PFUPipelineTools::dataset_info$dataset_colname,
                                      conn,
                                      schema = schema_from_conn(conn = conn),
                                      fk_parent_tables = get_all_fk_tables(conn = conn, schema = schema),
                                      fu_allocations_tab_name = IEATools::fu_analysis_file_info$fu_allocation_tab_name,
                                      country_colname = IEATools::iea_cols$country,
                                      table_name_colname = PFUPipelineTools::hashed_table_colnames$db_table_name) {
  out <- lapply(countries, FUN = function(coun) {
    folder <- ifelse(use_subfolders, file.path(fu_analysis_folder, coun), fu_analysis_folder)
    fpath <- file.path(folder, paste0(coun, file_suffix))
    fexists <- file.exists(fpath)
    if (!fexists & !generate_missing_fu_allocation_template) {
      return(NULL)
    }
    if (!fexists & generate_missing_fu_allocation_template) {
      # Create and write the template

      table_name <- specified_iea_data |>
        magrittr::extract2(table_name_colname) |>
        unique()
browser()
      assertthat::assert_that(length(table_name) == 1)
      iea_data <- PFUPipelineTools::pl_filter_collect(table_name,
                                                      # Find a way to insert country_colname here!
                                                      # This doesn't seem to work.
                                                      # .data[[country]] == coun,
                                                      Country %in% coun,
                                                      conn = conn,
                                                      schema = schema,
                                                      fk_parent_tables = fk_parent_tables)

      # Writing the allocation table is pointless if we don't have any IEA
      # data for that country.
      # So only write a template file if we have a non-zero number
      # of rows in the IEA data.
      if (nrow(iea_data) > 0) {
        # Make sure we have the folder we need
        dir.create(folder, showWarnings = FALSE)
        # Now write the template
        IEATools::fu_allocation_template(iea_data) %>%
          IEATools::write_fu_allocation_template(fpath)
      }
    }
    # Read the FU allocation data from fpath, if it exists.
    fexists <- file.exists(fpath)
    if (fexists) {
      return(IEATools::load_fu_allocation_data(fpath, fu_allocations_tab_name = fu_allocations_tab_name))
    } else {
      return(NULL)
    }
  }) %>%
    dplyr::bind_rows()
  if (nrow(out) == 0) {
    return(NULL)
  }
  out <- IEATools::tidy_fu_allocation_table(out) |>
    dplyr::mutate(
      "{dataset_colname}" := version
    ) |>
    dplyr::relocate(dplyr::all_of(dataset_colname))
  return(out)
}


#' Assemble completed final-to-useful allocation tables
#'
#' This function is used in a `targets` workflow to assemble completed final-to-useful allocation tables
#' given a set of incomplete allocation tables.
#'
#' Note that this function can accept tidy or wide by year data frames.
#' The return value is a tidy data frame.
#' Information from exemplar countries is used to complete incomplete final-to-useful efficiency tables.
#' See examples for how to construct `exemplar_lists`.
#'
#' @param tidy_incomplete_allocation_tables A data frame containing (potentially) incomplete final-to-useful allocation tables.
#' @param exemplar_lists A data frame containing `country` and `year` columns along with a column of ordered vectors of strings
#'                       telling which countries should be considered exemplars for the country and year of this row.
#' @param specified_iea_data_hash A has of the data frame containing specified IEA data.
#' @param countries A vector of countries for which completed final-to-useful allocation tables are to be assembled.
#' @param years The years for which analysis is desired.
#' @param conn The connection to a database; needed to collect the specified IEA data.
#' @param schema The database schema (a `dm` object).
#'               Default calls `PFUPipelineTools::schema_from_conn()`, but
#'               you can supply a pre-computed schema for speed.
#' @param fk_parent_tables Foreign key parent tables to assist decoding foreign keys.
#'                         Default calls `PFUPipelineTools::get_all_fk_tables()`.
#' @param country,year See `IEATools::iea_cols`.
#' @param exemplars,exemplar_tables,iea_data,incomplete_alloc_tables,complete_alloc_tables See `PFUPipeline::exemplar_names`.
#' @param table_name_colname The name of a column in `specified_iea_data` that gives
#'                           the table in the database in which specified IEA data are to be found.
#'                           Default is `PFUPipelineTools::hashed_table_colnames$db_table_name`.
#'
#' @return A tidy data frame containing completed final-to-useful allocation tables.
#'
#' @export
#'
#' @examples
#' # Load final-to-useful allocation tables, but eliminate one category of consumption,
#' # Residential consumption of Primary solid biofuels,
#' # which will be filled by the exemplar for GHA, ZAF.
#' incomplete_fu_allocation_tables <- IEATools::load_fu_allocation_data() %>%
#'   dplyr::filter(! (Country == "GHA" & Ef.product == "Primary solid biofuels" &
#'     Destination == "Residential"))
#' # Show that those rows are gone.
#' incomplete_fu_allocation_tables %>%
#'   dplyr::filter(Country == "GHA" & Ef.product == "Primary solid biofuels" &
#'     Destination == "Residential")
#' # But the missing rows of GHA are present in allocation data for ZAF.
#' incomplete_fu_allocation_tables %>%
#'   dplyr::filter(Country == "ZAF" & Ef.product == "Primary solid biofuels" &
#'     Destination == "Residential")
#' # Set up exemplar list
#' el <- tibble::tribble(
#'   ~Country, ~Year, ~Exemplars,
#'   "GHA", 1971, c("ZAF"),
#'   "GHA", 2000, c("ZAF"))
#' el
#' # Load IEA data
#' iea_data <- IEATools::load_tidy_iea_df() %>%
#'   IEATools::specify_all()
#' # Assemble complete allocation tables
#' completed <- assemble_fu_allocation_tables(incomplete_allocation_tables =
#'                                              incomplete_fu_allocation_tables,
#'                                            exemplar_lists = el,
#'                                            specified_iea_data = iea_data,
#'                                            countries = "GHA")
#' # Missing data for GHA has been picked up from ZAF.
#' completed %>%
#'   dplyr::filter(Country == "GHA" & EfProduct == "Primary solid biofuels" &
#'     Destination == "Residential")
assemble_fu_allocation_tables <- function(tidy_incomplete_allocation_tables,
                                          exemplar_lists,
                                          specified_iea_data,
                                          conn,
                                          schema = PFUPipelineTools::schema_from_conn(conn = conn),
                                          fk_parent_tables = PFUPipelineTools::get_all_fk_tables(conn = conn, schema = schema),
                                          country = IEATools::iea_cols$country,
                                          year = IEATools::iea_cols$year,
                                          exemplars = PFUPipeline::exemplar_names$exemplars,
                                          exemplar_tables = PFUPipeline::exemplar_names$exemplar_tables,
                                          iea_data = PFUPipeline::exemplar_names$iea_data,
                                          incomplete_alloc_tables = PFUPipeline::exemplar_names$incomplete_alloc_table,
                                          complete_alloc_tables = PFUPipeline::exemplar_names$complete_alloc_table,
                                          table_name_colname = PFUPipelineTools::hashed_table_colnames$db_table_name) {

browser()

  # Gather information about countries and years for pulling specified IEA data from the database.
  countries <- tidy_incomplete_allocation_tables |>
    magrittr::extract2(country) |>
    unique()
  years <- tidy_incomplete_allocation_tables |>
    magrittr::extract2(year) |>
    unique()
  table_name <- specified_iea_data |>
    magrittr::extract2(table_name_colname) |>
    unique()
  assertthat::assert_that(length(table_name) == 1)
  specified_iea_data <- PFUPipelineTools::pl_filter_collect(table_name,
                                                            # Find a way to insert country here!
                                                            # This doesn't seem to work.
                                                            # .data[[country]] %in% countries,
                                                            Country %in% countries,
                                                            # Find a way to insert country here!
                                                            # This doesn't seem to work.
                                                            # .data[[year]] %in% years,
                                                            Year %in% years,
                                                            conn = conn,
                                                            schema = schema,
                                                            fk_parent_tables = fk_parent_tables)

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
                                   tidy_incomplete_tables = list(tidy_incomplete_allocation_tables),
                                   exemplar_strings = .data[[exemplars]],
                                   yr = .data[[year]],
                                   country_colname = country,
                                   year_colname = year),
        # Add a column containing an IEA data frame for the country and year of each row
        "{iea_data}" := Map(get_one_df_by_coun_and_yr,
                            .df = list(specified_iea_data),
                            coun = .data[[country]],
                            yr = .data[[year]],
                            country_colname = country,
                            year_colname = year),
        # Add a column containing incomplete fu allocation tables for each row (i.e., for each combination of country and year).
        "{incomplete_alloc_tables}" := Map(get_one_df_by_coun_and_yr,
                                           .df = list(tidy_incomplete_allocation_tables),
                                           coun = .data[[country]],
                                           yr = .data[[year]],
                                           country_colname = country,
                                           year_colname = year),
        # Add a column containing completed fu allocation tables for each row (i.e., for each combination of country and year).
        # Note that the data frames in this column contain the SOURCE of information for each allocation.
        "{complete_alloc_tables}" := Map(IEATools::complete_fu_allocation_table,
                                         fu_allocation_table = .data[[incomplete_alloc_tables]],
                                         country_to_complete = coun,
                                         exemplar_fu_allocation_tables = .data[[exemplar_tables]],
                                         tidy_specified_iea_data = .data[[iea_data]])
      )
  }) %>%
    dplyr::bind_rows()

  # The only information we need to return is the completed allocation tables.
  # Expand (unnest) only the completed allocation table column to give one data frame of all the FU allocations
  # for all years and all countries.
  # completed_tables_by_year %>%
  #   dplyr::select(complete_alloc_tables) %>%
  #   tidyr::unnest(cols = .data[[complete_alloc_tables]])
  out <- completed_tables_by_year %>%
    dplyr::select(complete_alloc_tables) %>%
    tidyr::unnest(cols = .data[[complete_alloc_tables]])
  assertthat::assert_that(!(complete_alloc_tables %in% names(out)),
                          msg = paste(paste0(countries, collapse = ", "),
                                      "do (does) not have allocation information in PFUPipeline::assemble_fu_allocation_tables()"))
  return(out)
}


get_one_df_by_coun_and_yr <- function(.df, coun, yr, country_colname, year_colname) {
  .df %>%
    dplyr::filter(.data[[country_colname]] %in% coun, .data[[year_colname]] %in% yr)
}


get_one_exemplar_table_list <- function(tidy_incomplete_tables,
                                        exemplar_strings, yr, country_colname, year_colname) {
  lapply(exemplar_strings, function(exemplar_coun) {
    tidy_incomplete_tables %>%
      dplyr::filter(.data[[country_colname]] == exemplar_coun, .data[[year_colname]] == yr)
  })
}
