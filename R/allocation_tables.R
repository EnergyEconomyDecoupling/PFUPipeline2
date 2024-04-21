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
#' @param specified_iea_data A ticket to recover a data frame of specified IEA data for `countries`.
#' @param countries The countries for which allocation tables should be loaded.
#' @param file_suffix The suffix for the FU analysis files. Default is "`r IEATools::fu_analysis_file_info$fu_analysis_file_suffix`".
#' @param use_subfolders Tells whether to look for files in subfolders named by `countries`. Default is `TRUE`.
#' @param generate_missing_fu_allocation_template Tells whether to generate a missing final-to-useful allocation template from `specified_iea_data`. Default is `TRUE`.
#'
#' @export
#'
#' @return A ticket to recover a
#'         data frame of tidy FU Allocation tables read by `IEATools::load_fu_allocation_data()`.
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
                                      fu_allocations_tab_name = IEATools::fu_analysis_file_info$fu_allocation_tab_name) {

  out <- lapply(countries, FUN = function(coun) {

    folder <- ifelse(use_subfolders, file.path(fu_analysis_folder, coun), fu_analysis_folder)
    fpath <- file.path(folder, paste0(coun, file_suffix))
    fexists <- file.exists(fpath)
    if (!fexists & !generate_missing_fu_allocation_template) {
      return(NULL)
    }
    if (!fexists & generate_missing_fu_allocation_template) {
      # Create and write the template
      # If there is no iea_data for coun, simply return NULL.
      if (is.null(specified_iea_data)) {
        return(NULL)
      }
      # Writing the allocation table is pointless if we don't have any IEA
      # data for that country.
      # So only write a template file if we have a non-zero number
      # of rows in the IEA data.
      if (nrow(specified_iea_data) > 0) {
        # Make sure we have the folder we need
        dir.create(folder, showWarnings = FALSE)
        # Now write the template
        IEATools::fu_allocation_template(specified_iea_data) |>
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
  }) |>
    dplyr::bind_rows()
  if (nrow(out) == 0) {
    return(NULL)
  }
  out |>
    IEATools::tidy_fu_allocation_table()
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
#' @param incomplete_allocation_tables
#' @param exemplar_lists A data frame containing `country` and `year` columns along with a column of ordered vectors of strings
#'                       telling which countries should be considered exemplars for the country and year of this row.
#' @param specified_iea_data Specified IEA data.
#' @param countries A vector of countries for which completed final-to-useful allocation tables are to be assembled.
#' @param years The years for which analysis is desired.
#' @param dataset The name of the dataset to which these data belong.
#' @param db_table_name The name of the specified IEA data table in `conn`.
#' @param conn The database connection.
#' @param schema The data model (`dm` object) for the database in `conn`.
#'               See details.
#' @param fk_parent_tables A named list of all parent tables
#'                         for the foreign keys in `db_table_name`.
#'                         See details.
#' @param country,year See `IEATools::iea_cols`.
#' @param exemplars,exemplar_tables,iea_data,incomplete_alloc_tables,complete_alloc_tables See `IEATools::exemplar_names`.
#' @param dataset_colname See `PFUPipelineTools::dataset_info`.
#'
#' @return A tidy data frame containing completed final-to-useful allocation tables.
#'
#' @export
#'
#' @examples
#' # Load final-to-useful allocation tables, but eliminate one category of consumption,
#' # Residential consumption of Primary solid biofuels,
#' # which will be filled by the exemplar for GHA, ZAF.
#' incomplete_fu_allocation_tables <- IEATools::load_fu_allocation_data() |>
#'   dplyr::filter(! (Country == "GHA" & Ef.product == "Primary solid biofuels" &
#'     Destination == "Residential"))
#' # Show that those rows are gone.
#' incomplete_fu_allocation_tables |>
#'   dplyr::filter(Country == "GHA" & Ef.product == "Primary solid biofuels" &
#'     Destination == "Residential")
#' # But the missing rows of GHA are present in allocation data for ZAF.
#' incomplete_fu_allocation_tables |>
#'   dplyr::filter(Country == "ZAF" & Ef.product == "Primary solid biofuels" &
#'     Destination == "Residential")
#' # Set up exemplar list
#' el <- tibble::tribble(
#'   ~Country, ~Year, ~Exemplars,
#'   "GHA", 1971, c("ZAF"),
#'   "GHA", 2000, c("ZAF"))
#' el
#' # Load IEA data
#' iea_data <- IEATools::load_tidy_iea_df() |>
#'   IEATools::specify_all()
#' # Assemble complete allocation tables
#' completed <- assemble_fu_allocation_tables(incomplete_allocation_tables =
#'                                              incomplete_fu_allocation_tables,
#'                                            exemplar_lists = el,
#'                                            specified_iea_data = iea_data,
#'                                            countries = "GHA")
#' # Missing data for GHA has been picked up from ZAF.
#' completed |>
#'   dplyr::filter(Country == "GHA" & EfProduct == "Primary solid biofuels" &
#'     Destination == "Residential")
assemble_fu_allocation_tables <- function(incomplete_allocation_tables,
                                          exemplar_lists,
                                          specified_iea_data,
                                          countries,
                                          years,
                                          dataset,
                                          db_table_name,
                                          conn,
                                          schema = PFUPipelineTools::schema_from_conn(conn),
                                          fk_parent_tables = PFUPipelineTools::get_all_fk_tables(conn = conn, schema = schema),
                                          country = IEATools::iea_cols$country,
                                          year = IEATools::iea_cols$year,
                                          exemplars = PFUPipelineTools::exemplar_names$exemplars,
                                          exemplar_tables = PFUPipelineTools::exemplar_names$exemplar_tables,
                                          iea_data = PFUPipelineTools::exemplar_names$iea_data,
                                          incomplete_alloc_tables = PFUPipelineTools::exemplar_names$incomplete_alloc_table,
                                          complete_alloc_tables = PFUPipelineTools::exemplar_names$complete_alloc_table,
                                          dataset_colname = PFUPipelineTools::dataset_info$dataset_colname) {

  browser()

  if (is.null(specified_iea_data)) {
    # No reason to assemble allocation data,
    # because there is no IEA data to be allocated.
    return(NULL)
  }

  tidy_incomplete_allocation_tables <- incomplete_allocation_tables
  if (is.null(tidy_incomplete_allocation_tables)) {
    incomplete_allocation_tables_name <-
      incomplete_allocation_tables[[PFUPipelineTools::hashed_table_colnames$db_table_name]] |>
      unique()
    # Get a zero-row version of the table
    tidy_incomplete_allocation_tables <- schema |>
      dm::dm_get_tables() |>
      magrittr::extract2(incomplete_allocation_tables_name) |>
      # Empty out the rows
      dplyr::filter(FALSE) |>
      # This decode step is necessary,
      # despite the data frame being empty of rows,
      # to convert data types in the columns.
      PFUPipelineTools::decode_fks(db_table_name = incomplete_allocation_tables_name,
                                   conn = conn,
                                   schema = schema,
                                   fk_parent_tables = fk_parent_tables)
  }
  tidy_incomplete_allocation_tables <- tidy_incomplete_allocation_tables |>
    dplyr::mutate(
      # Eliminate the dataset column for now.
      "{dataset_colname}" := NULL
    ) |>
    PFUPipelineTools::tar_ungroup()

  specified_iea_data <- specified_iea_data |>
    dplyr::mutate(
      # Eliminate the dataset column, because it conflicts with the dataset name for the
      # incomplete_allocation_tables.
      "{dataset_colname}" := NULL
    ) |>
    PFUPipelineTools::tar_ungroup()

  completed_tables_by_year <- lapply(countries, FUN = function(coun) {
    coun_exemplar_strings <- exemplar_lists |>
      dplyr::filter(.data[[country]] == coun, .data[[year]] %in% years)

    # For each combination of Country and Year (the rows of coun_exemplar_strings),
    # assemble a list of country allocation tables.
    coun_exemplar_strings_and_tables <- coun_exemplar_strings |>
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
  }) |>
    dplyr::bind_rows()

  # The only information we need to return is the completed allocation tables.
  # Expand (unnest) only the completed allocation table column to give one data frame of all the FU allocations
  # for all years and all countries.
  # completed_tables_by_year |>
  #   dplyr::select(complete_alloc_tables) |>
  #   tidyr::unnest(cols = .data[[complete_alloc_tables]])
  out <- completed_tables_by_year |>
    dplyr::select(dplyr::all_of(complete_alloc_tables)) |>
    tidyr::unnest(cols = dplyr::all_of(complete_alloc_tables)) |>
    dplyr::mutate(
      # Add back the dataset column
      "{dataset_colname}" := dataset
    ) |>
    # Move the dataset column to the left.
    dplyr::relocate(dplyr::all_of(dataset_colname))
  assertthat::assert_that(!(complete_alloc_tables %in% names(out)),
                          msg = paste(paste0(countries, collapse = ", "),
                                      "do (does) not have allocation information in PFUPipeline2::assemble_fu_allocation_tables()"))
  return(out)
}


get_one_df_by_coun_and_yr <- function(.df, coun, yr, country_colname, year_colname) {
  .df |>
    dplyr::filter(.data[[country_colname]] %in% coun, .data[[year_colname]] %in% yr)
}


get_one_exemplar_table_list <- function(tidy_incomplete_tables,
                                        exemplar_strings, yr, country_colname, year_colname) {
  lapply(exemplar_strings, function(exemplar_coun) {
    tidy_incomplete_tables |>
      dplyr::filter(.data[[country_colname]] == exemplar_coun, .data[[year_colname]] == yr)
  })
}


#' Add allocation matrices to a data frame
#'
#' This function adds allocation matrices (**C_Y** and **C_EIOU**) to the previously-created
#' `CompletedAllocationTables` target.
#'
#' @param completed_allocation_tables The completed allocation tables from which allocation (**C**) matrices should be created.
#'                                    This data frame is most likely to be the `CompletedAllocationTables` target.
#' @param countries The countries for which **C** matrices should be formed.
#' @param index_map The index map for the matrices, used for uploading to the database.
#' @param matrix_class The type of matrix that should be produced.
#'                     One of "matrix" (the default and not sparse) or "Matrix" (which may be sparse).
#' @param dataset The name of the dataset to which these data belong.
#' @param db_table_name The name of the specified IEA data table in `conn`.
#' @param conn The database connection.
#' @param schema The data model (`dm` object) for the database in `conn`.
#'               See details.
#' @param fk_parent_tables A named list of all parent tables
#'                         for the foreign keys in `db_table_name`.
#'                         See details.
#' @param country,year See `IEATools::iea_cols`.
#' @param c_source,.values,C_Y,C_EIOU See `IEATools::template_cols`.
#' @param industry_type,product_type See `IEATools::row_col_types`.
#' @param dataset_colname See `PFUPipelineTools::dataset_info`.
#'
#' @return A data frame with `C_Y` and `C_EIOU` columns containing allocation matrices.
#'
#' @export
calc_C_mats <- function(completed_allocation_tables,
                        countries,
                        matrix_class = "Matrix",
                        c_source = IEATools::template_cols$c_source,
                        .values = IEATools::template_cols$.values,
                        C_Y = IEATools::template_cols$C_Y,
                        C_EIOU  = IEATools::template_cols$C_eiou) {

  if (is.null(completed_allocation_tables)) {
    # No reason to calculate allocation matrices.
    return(NULL)
  }

  completed_allocation_tables |>
    dplyr::mutate(
      # Eliminate the c_source column (if it exists) before sending
      # the completed_allocation_tables into form_C_mats().
      # The c_source column applies to individual C values, and we're making matrices out of them.
      # In other words, form_C_mats() doesn't know what to do with that column.
      "{c_source}" := NULL
    ) |>
    # Need to form C matrices from completed_allocation_tables.
    # Use the IEATools::form_C_mats() function for this task.
    # The function accepts a tidy data frame in addition to wide-by-year data frames.
    IEATools::form_C_mats(matvals = .values, matrix_class = matrix_class)
}
