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
                         do_chops ,
                         schema_file_path) {

  # Avoid warnings on target names
  SchemaFilePath <- NULL; Schema <- NULL; DM <- NULL

  # Return the targets pipeline
  list(

    # Schema file set as a target to track changes
    targets::tar_target_raw(
      "SchemaFilePath",
      schema_file_path,
      format = "file"
    ),

    # Load the schema file
    targets::tar_target(
      Schema,
      PFUPipelineTools::load_schema_table(schema_path = SchemaFilePath)
    ),

    # Create a data model
    targets::tar_target(
      DM,
      PFUPipelineTools::schema_dm(Schema)
    ),

    # Upload the data model to the database
    targets::tar_target(
      UploadDM,
      PFUPipeline2::upload_schema(DM, conn, drop_tables = TRUE)
    )




  ) |>
    # Ensure each target has access to the database,
    # using the hint found at https://github.com/ropensci/targets/discussions/1164.
    tarchetypes::tar_hook_before(hook = {
      conn <- DBI::dbConnect(drv = RPostgres::Postgres(),
                             dbname = conn_params$dbname,
                             host = conn_params$host,
                             port = conn_params$port,
                             user = conn_params$user)
      on.exit(DBI::dbDisconnect(conn))
    })

}
