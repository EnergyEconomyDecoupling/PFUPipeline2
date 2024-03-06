
# This is the targets pipeline

list(

  # Initialize -----------------------------------------------------------------
  targets::tar_target_raw("Countries", list(countries)),
  targets::tar_target_raw("Years", list(years)),
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
  #                    AllIEAData             IEAData             BalancedIEAData             SpecifiedIEAData
  #                     ^                      ^                   ^                           ^
  #                     |                      |                   |                           |
  #                     |                      |                   |                           |
  # IEADataPath -----> AllIEADataLocal -----> IEADataLocal -----> BalancedIEADataLocal -----> SpecifiedIEADataLocal

  ## IEADataPath
  targets::tar_target_raw(
    "IEADataPath",
    clpfu_setup_paths[["iea_data_path"]],
    format = "file"),

  ## AllIEADataLocal
  targets::tar_target(
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
    IEADataLocal |>
      is_balanced(),
    pattern = map(IEADataLocal)),

  ## BalancedIEADataLocal
  targets::tar_target(
    BalancedIEADataLocal,
    IEADataLocal |>
      make_balanced(),
    pattern = map(IEADataLocal)),

  ## BalancedIEAData
  targets::tar_target(
    BalancedIEAData,
    BalancedIEADataLocal |>
      PFUPipelineTools::pl_upsert(db_table_name = "BalancedIEAData",
                                  conn = conn,
                                  in_place = TRUE,
                                  schema = DM,
                                  fk_parent_tables = SimpleFKTables),
    pattern = map(BalancedIEADataLocal)),

  ## BalancedAfterIEA
  targets::tar_target(
    BalancedAfterIEA,
    BalancedIEADataLocal |>
      is_balanced(),
    pattern = map(BalancedIEADataLocal)),

  ## OKToProceedIEA
  targets::tar_target(
    OKToProceedIEA,
    ifelse(is.null(stopifnot(all(BalancedAfterIEA))), yes = TRUE, no = FALSE)),

  ## SpecifiedIEADataLocal
  targets::tar_target(
    SpecifiedIEADataLocal,
    BalancedIEADataLocal |>
      specify(),
    pattern = map(BalancedIEADataLocal)),

  ## SpecifiedIEAData
  targets::tar_target(
    SpecifiedIEAData,
    SpecifiedIEADataLocal |>
      PFUPipelineTools::pl_upsert(db_table_name = "SpecifiedIEAData",
                                  conn = conn,
                                  in_place = TRUE,
                                  schema = DM,
                                  fk_parent_tables = SimpleFKTables),
    pattern = map(SpecifiedIEADataLocal)),


  # Exemplar lists -------------------------------------------------------------
  # For FU machine allocations and FU machine efficiencies

  ## ExamplarTablePath
  targets::tar_target_raw(
    "ExemplarTablePath",
    clpfu_setup_paths[["exemplar_table_path"]],
    format = "file"),

  ## ExemplarLists
  targets::tar_target(
    ExemplarLists,
    ExemplarTablePath |>
      load_exemplar_table(countries = AllocAndEffCountries,
                          years = Years) |>
      exemplar_lists(AllocAndEffCountries),
    pattern = map(AllocAndEffCountries)),


  # Allocation tables ----------------------------------------------------------

  # Dependencies among AllocationTable targets:
  #
  #                         IncompleteAllocationTables             CompletedAllocationTables
  #                          ^                                      ^
  #                          |                                      |
  #                          |                                      |
  # FUAnalysisFolder -----> IncompleteAllocationTablesLocal -----> CompletedAllocationTablesLocal

  ## FUAnalysisFolder
  targets::tar_target_raw(
    "FUAnalysisFolder",
    clpfu_setup_paths$fu_allocation_folder,
    format = "file"),

  ## IncompleteAllocationTablesLocal
  tarchetypes::tar_group_by(
    IncompleteAllocationTablesLocal,
    load_fu_allocation_tables(FUAnalysisFolder,
                              specified_iea_data = SpecifiedIEAData,
                              countries = AllocAndEffCountries,
                              conn = conn,
                              schema = DM,
                              fk_parent_tables = SimpleFKTables),
    Country),

  targets::tar_target(
    IncompleteAllocationTables,
    IncompleteAllocationTablesLocal |>
      PFUPipelineTools::pl_upsert(db_table_name = "IncompleteAllocationTables",
                                  conn = conn,
                                  in_place = TRUE,
                                  schema = DM,
                                  fk_parent_tables = SimpleFKTables),
    pattern = map(IncompleteAllocationTablesLocal)
  ),

  ## CompletedAllocationTablesLocal
  targets::tar_target(
    CompletedAllocationTablesLocal,
    IncompleteAllocationTablesLocal |>
      assemble_fu_allocation_tables(exemplar_lists = ExemplarLists,
                                    specified_iea_data = SpecifiedIEADataLocal |>
                                      PFUPipelineTools::tar_ungroup(),
                                    version = paste0("CLPFU", clpfu_version),
                                    countries = Countries,
                                    years = Years),
      pattern = map(Countries)),

  ## CompletedAllocationTables
  targets::tar_target(
    CompletedAllocationTables,
    CompletedAllocationTablesLocal |>
      PFUPipelineTools::pl_upsert(db_table_name = "CompletedAllocationTables",
                                  conn = conn,
                                  in_place = TRUE,
                                  schema = DM,
                                  fk_parent_tables = SimpleFKTables),
    pattern = map(CompletedAllocationTablesLocal)
  ),


  # Machine data (Efficiencies) ------------------------------------------------

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
    pattern = map(MachineDataLocal))

  # ## CompletedEfficiencyTables
  # targets::tar_target(
  #   CompletedEfficiencyTables,
  #   assemble_eta_fu_tables(incomplete_eta_fu_tables = MachineData,
  #                          exemplar_lists = ExemplarLists,
  #                          completed_fu_allocation_tables = CompletedAllocationTables,
  #                          countries = Countries,
  #                          years = Years,
  #                          which_quantity = IEATools::template_cols$eta_fu),
  #   pattern = quote(map(Countries)))



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
