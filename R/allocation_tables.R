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
#' @return A data frame of FU Allocation tables read by `IEATools::load_fu_allocation_data()`.
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
      assertthat::assert_that(length(table) == 1)
      iea_data <- PFUPipelineTools::pl_filter_collect(table_name,









                                                      # Find a way to insert country_colname here!
                                                      Country == coun,













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
  return(out)
}
