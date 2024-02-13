
# This is the targets pipeline

list(

  # Schema and data model ------------------------------------------------------

  ## SchemaFilePath
  targets::tar_target_raw(
    "SchemaFilePath",
    pfu_setup_paths[["schema_path"]],
    format = "file"
  ),

  ## SchemaTable
  # Load the schema table from the schema file.
  targets::tar_target(
    SchemaTable,
    PFUPipelineTools::load_schema_table(schema_path = SchemaFilePath)
  ),

  ## SimpleFKTables
  # Store a list of foreign key tables available in SchemaFilePath.
  # Additions will be made later.
  targets::tar_target(
    SimpleFKTables,
    PFUPipelineTools::load_simple_tables(simple_tables_path = SchemaFilePath)
  ),

  ## DM
  # Create the data model (dm object) from the SchemaTable.
  targets::tar_target(
    DM,
    PFUPipelineTools::schema_dm(SchemaTable)
  ),

  ## UploadDM
  # Upload the data model and SimpleFKTables to the database.
  targets::tar_target(
    UploadDM,
    PFUPipelineTools::pl_upload_schema_and_simple_tables(schema = DM,
                                                         simple_tables = SimpleFKTables,
                                                         conn = conn,
                                                         drop_db_tables = TRUE)
  ),


  # Country concordance --------------------------------------------------------

  ## CountryConcordancePath
  targets::tar_target_raw(
    "CountryConcordancePath",
    pfu_setup_paths[["country_concordance_path"]],
    format = "file"
  ),

  ## CountryConcordanceTable
  targets::tar_target(
    CountryConcordanceTable,
    load_country_concordance_table(country_concordance_path = CountryConcordancePath)
  ),


  # IEA data -------------------------------------------------------------------

  ## IEADataPath
  targets::tar_target_raw(
    "IEADataPath",
    pfu_setup_paths[["iea_data_path"]],
    format = "file"
  ),

  ## AllIEAData
  targets::tar_target(
    AllIEAData,
    IEATools::load_tidy_iea_df(IEADataPath,
                               override_df = CountryConcordanceTable,
                               specify_non_energy_flows = TRUE,
                               apply_fixes = TRUE))

  ## Upload AllIEAData


) |>


  # Hook before for conn -------------------------------------------------------

  tarchetypes::tar_hook_before(hook = {
    # Ensure each target has access to the database,
    # using the hint found at https://github.com/ropensci/targets/discussions/1164.
    conn <- DBI::dbConnect(drv = RPostgres::Postgres(),
                           dbname = conn_params$dbname,
                           host = conn_params$host,
                           port = conn_params$port,
                           user = conn_params$user)
    on.exit(DBI::dbDisconnect(conn))
  })
