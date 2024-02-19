
# This is the targets pipeline

list(

  # Initialize -----------------------------------------------------------------
  targets::tar_target_raw("Countries", list(countries)),
  targets::tar_target_raw("AdditionalExemplarCountries", list(additional_exemplar_countries)),
  targets::tar_target_raw("SpecifyNonEnergyFlows", list(specify_non_energy_flows)),
  targets::tar_target_raw("ApplyFixes", list(apply_fixes)),

  targets::tar_target(
    AllocAndEffCountries,
    combine_countries_exemplars(Countries, AdditionalExemplarCountries)),


  # Schema and data model ------------------------------------------------------

  ## SchemaFilePath
  targets::tar_target_raw(
    "SchemaFilePath",
    clpfu_setup_paths[["schema_path"]],
    format = "file"),

  ## SchemaTable
  # Load the schema table from the schema file.
  targets::tar_target(
    SchemaTable,
    PFUPipelineTools::load_schema_table(schema_path = SchemaFilePath)),

  ## SimpleFKTables
  # Store a list of foreign key tables available in SchemaFilePath.
  # Additions will be made later.
  targets::tar_target(
    SimpleFKTables,
    PFUPipelineTools::load_fk_tables(simple_tables_path = SchemaFilePath)),

  ## DM
  # Create the data model (dm object) from the SchemaTable.
  targets::tar_target(
    DM,
    PFUPipelineTools::schema_dm(SchemaTable)),

  ## UploadDM
  # Upload the data model and SimpleFKTables to the database.
  targets::tar_target(
    UploadDM,
    PFUPipelineTools::pl_upload_schema_and_simple_tables(schema = DM,
                                                         simple_tables = SimpleFKTables,
                                                         conn = conn,
                                                         drop_db_tables = TRUE)),


  # Country concordance --------------------------------------------------------

  ## CountryConcordancePath
  targets::tar_target_raw(
    "CountryConcordancePath",
    clpfu_setup_paths[["country_concordance_path"]],
    format = "file"),

  ## CountryConcordanceTable
  targets::tar_target(
    CountryConcordanceTable,
    load_country_concordance_table(country_concordance_path = CountryConcordancePath)),


  # IEA data -------------------------------------------------------------------

  ## IEADataPath
  targets::tar_target_raw(
    "IEADataPath",
    clpfu_setup_paths[["iea_data_path"]],
    format = "file"),

  ## AllIEADataLocal
  targets::tar_target(
  # targets::tar_target(
    AllIEADataLocal,
    IEADataPath |>
      load_iea_data(override_df = CountryConcordanceTable,
                    dataset = iea_dataset,
                    specify_non_energy_flows = SpecifyNonEnergyFlows,
                    apply_fixes = ApplyFixes)),

  ## Upload AllIEAData
  targets::tar_target(
    AllIEAData,
    AllIEADataLocal |>
      PFUPipelineTools::pl_upsert(db_table_name = "AllIEAData",
                                  conn = conn,
                                  in_place = TRUE,
                                  schema = DM,
                                  fk_parent_tables = SimpleFKTables)),

  ## IEADataLocal
  tarchetypes::tar_group_by(
    IEADataLocal,
    AllIEADataLocal |>
      dplyr::filter(Country %in% countries),
    Country),

  ## IEAData
  targets::tar_target(
    IEAData,
    IEADataLocal |>
      PFUPipelineTools::pl_upsert(db_table_name = "IEAData",
                                  conn = conn,
                                  in_place = TRUE,
                                  schema = DM,
                                  fk_parent_tables = SimpleFKTables),
    pattern = map(IEADataLocal))

  ## Check IEA data balance
  # targets::tar_target(
  #   BalancedBeforeIEA,
  #   IEAData |>
  #     is_balanced(conn = conn, schema = DM, fk_parent_tables = SimpleFKTables),
  #   pattern = map(IEAData))


) |>


  # conn tar_hook_before targets -----------------------------------------------

  tarchetypes::tar_hook_before(
    hook = {
      # Ensure each target has access to the database,
      # using the hint found at https://github.com/ropensci/targets/discussions/1164.
      conn <- DBI::dbConnect(drv = RPostgres::Postgres(),
                             dbname = conn_params$dbname,
                             host = conn_params$host,
                             port = conn_params$port,
                             user = conn_params$user)
      on.exit(DBI::dbDisconnect(conn))
    })
