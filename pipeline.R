
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

  ## SetDMAndFKTables
  #  Create and upload the schema from SchemaFilePath
  targets::tar_target(
    SetDMAndFKTables,
    set_dm_fk_tables(SchemaFilePath, conn = conn)),

  ## DM
  #  Extract the data model
  targets::tar_target(
    DM,
    SetDMAndFKTables[["dm"]]),

  ## SimpleFKTables
  #  Extract the foreign key tables
  targets::tar_target(
    SimpleFKTables,
    SetDMAndFKTables[["simple_fk_tables"]]),


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

  # Dependencies among IEA targets:
  #
  #                    AllIEAData             IEAData -----> BalancedIEAData -----> SpecifiedIEAData
  #                     ^                      ^
  #                     |                      |
  #                     |                      |
  # IEADataPath -----> AllIEADataLocal -----> IEADataLocal

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

  ## AllIEAData
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
      dplyr::filter(Country %in% AllocAndEffCountries, Year %in% years),
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
    pattern = map(IEADataLocal)),

  ## BalancedBeforeIEA
  targets::tar_target(
    BalancedBeforeIEA,
    IEAData |>
      is_balanced(conn = conn),
    pattern = map(IEAData)),

  ## BalancedIEAData
  targets::tar_target(
    BalancedIEAData,
    IEAData |>
      make_balanced(conn = conn),
    pattern = map(IEAData)),

  ## BalancedAfterIEA
  targets::tar_target(
    BalancedAfterIEA,
    BalancedIEAData |>
      is_balanced(conn = conn),
    pattern = map(BalancedIEAData)),

  ## OKToProceedIEA
  targets::tar_target(
    OKToProceedIEA,
    ifelse(is.null(stopifnot(all(BalancedAfterIEA))), yes = TRUE, no = FALSE)),

  ## SpecifiedIEAData
  targets::tar_target(
    SpecifiedIEAData,
    BalancedIEAData |>
      specify(conn = conn),
    pattern = map(BalancedIEAData)),


  # Machine data (efficiencies) ------------------------------------------------

  # Dependencies among MachineData targets:
  #
  #                        AllMachineData             MachineData
  #                         ^                          ^
  #                         |                          |
  #                         |                          |
  # MachineDataPath -----> AllMachineDataLocal -----> MachineDataLocal

  ## MachineDataPath
  targets::tar_target_raw(
    "MachineDataPath",
    clpfu_setup_paths[["machine_data_folder"]],
    format = "file"),

  ## AllMachineDataLocal
  targets::tar_target(
    AllMachineDataLocal,
    read_all_eta_files(eta_fin_paths = get_eta_filepaths(MachineDataPath),
                       version = paste0("CLPFU", clpfu_version))),

  ## AllMachineData
  targets::tar_target(
    AllMachineData,
    AllMachineDataLocal |>
      PFUPipelineTools::pl_upsert(db_table_name = "AllMachineData",
                                  conn = conn,
                                  in_place = TRUE,
                                  schema = DM,
                                  fk_parent_tables = SimpleFKTables)),

  ## MachineDataLocal
  tarchetypes::tar_group_by(
    MachineDataLocal,
    AllMachineDataLocal |>
      dplyr::filter(Country %in% AllocAndEffCountries, Year %in% years),
    Country),

  ## MachineData
  targets::tar_target(
    MachineData,
    MachineDataLocal |>
      PFUPipelineTools::pl_upsert(db_table_name = "MachineData",
                                  conn = conn,
                                  in_place = TRUE,
                                  schema = DM,
                                  fk_parent_tables = SimpleFKTables),
    pattern = map(MachineDataLocal)),


  # Allocation tables ----------------------------------------------------------

  # IncompleteAllocationTables
  targets::tar_target(
    IncompleteAllocationTables,
    load_fu_allocation_tables(FUAnalysisFolder,
                              specified_iea_data = SpecifiedIEAData,
                              countries = AllocAndEffCountries))






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
