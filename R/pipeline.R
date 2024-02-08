#' Create the targets pipeline for the CL-PFU database
#'
#' This is a type of target factory whose arguments
#' specify the details of a targets workflow to be constructed
#'
#' `conn_params` should be a named list containing some information for the connection.
#'
#' * dbname: a string identifying the name of the database
#' * user: the username of the person connecting
#' * host: the hostname or IP address of the database management system (DBMS)
#' * port: the port for DBMS connections
#'
#' Passwords should _not_ be included in `conn_params` and will be ignored.
#' Rather, passwords should be set in the user's `.pgpass` file.
#'
#' @param input_data_version A string indicating which input data should be used.
#' @param output_version A string indicating the output version.
#' @param conn_params A named list containing connection parameters to the database.
#'                    See details.
#' @param countries A vector of countries to be analyzed.
#' @param years A vector of years to be analyzed.
#' @param do_chops A boolean that tells whether to perform slices on the
#'                 resource matrix and final demand matrix.
#'
#' @return A list representing the `targets` pipeline.
#'
#' @export
get_pipeline <- function(input_data_version,
                         output_version,
                         conn_params,
                         countries,
                         years,
                         do_chops) {

  setup <- PFUSetup::get_abs_paths(version = input_data_version)

  # Avoid warnings on target names
  Schema <- NULL

  # Return the targets pipeline
  list(
    targets::tar_target(
      Schema,
      PFUPipelineTools::load_schema_table(version = input_data_version))

  )
}
