
# This is the targets pipeline

list(

  # Schema file set as a target to track changes
  targets::tar_target_raw(
    "SchemaFilePath",
    PFUSetup::get_abs_paths(version = input_data_version)[["schema_path"]],
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
    PFUPipelineTools::upload_schema_and_simple_tables(DM, conn, drop_tables = TRUE)
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
