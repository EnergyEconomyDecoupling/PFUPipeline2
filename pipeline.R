
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


  # ExemplarTable --------------------------------------------------------------

  ## ExemplarTablePath
  targets::tar_target_raw(
    "ExemplarTablePath",
    clpfu_setup_paths[["exemplar_table_path"]],
    format = "file"),

  ## ExemplarTable
  targets::tar_target(
    ExemplarTable,
    readxl::read_excel(path = ExemplarTablePath)),

  ## ExemplarLists
  targets::tar_target(
    ExemplarLists,
    prep_exemplar_table(exemplar_table = ExemplarTable,
                        countries = AllocAndEffCountries,
                        years = Years) |>
      exemplar_lists(AllocAndEffCountries)),


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
    load_iea_data(iea_data_path = IEADataPath,
                  override_df = CountryConcordanceTable,
                  dataset = iea_dataset,
                  specify_non_energy_flows = SpecifyNonEnergyFlows,
                  apply_fixes = ApplyFixes)),

  ## AllIEAData
  targets::tar_target(
    AllIEAData,
    PFUPipelineTools::pl_upsert(AllIEADataLocal,
                                db_table_name = "AllIEAData",
                                conn = conn,
                                in_place = TRUE,
                                schema = DM,
                                fk_parent_tables = SimpleFKTables)),

  ## IEADataLocal
  tarchetypes::tar_group_by(
    IEADataLocal,
    dplyr::filter(AllIEADataLocal,
                  Country %in% AllocAndEffCountries,
                  Year %in% years),
    Country),

  ## IEAData
  targets::tar_target(
    IEAData,
    PFUPipelineTools::pl_upsert(IEADataLocal,
                                db_table_name = "IEAData",
                                conn = conn,
                                in_place = TRUE,
                                schema = DM,
                                fk_parent_tables = SimpleFKTables),
    pattern = map(IEADataLocal)),

  ## BalancedBeforeIEA
  targets::tar_target(
    BalancedBeforeIEA,
    is_balanced(IEADataLocal),
    pattern = map(IEADataLocal)),

  ## BalancedIEADataLocal
  targets::tar_target(
    BalancedIEADataLocal,
    make_balanced(IEADataLocal),
    pattern = map(IEADataLocal)),

  ## BalancedIEAData
  targets::tar_target(
    BalancedIEAData,
    PFUPipelineTools::pl_upsert(BalancedIEADataLocal,
                                db_table_name = "BalancedIEAData",
                                conn = conn,
                                in_place = TRUE,
                                schema = DM,
                                fk_parent_tables = SimpleFKTables),
    pattern = map(BalancedIEADataLocal)),

  ## BalancedAfterIEA
  targets::tar_target(
    BalancedAfterIEA,
    is_balanced(BalancedIEADataLocal),
    pattern = map(BalancedIEADataLocal)),

  ## OKToProceedIEA
  targets::tar_target(
    OKToProceedIEA,
    ifelse(is.null(stopifnot(all(BalancedAfterIEA))), yes = TRUE, no = FALSE)),

  ## SpecifiedIEADataLocal
  targets::tar_target(
    SpecifiedIEADataLocal,
    specify(BalancedIEADataLocal),
    pattern = map(BalancedIEADataLocal)),

  ## SpecifiedIEAData
  targets::tar_target(
    SpecifiedIEAData,
    PFUPipelineTools::pl_upsert(SpecifiedIEADataLocal,
                                db_table_name = "SpecifiedIEAData",
                                conn = conn,
                                in_place = TRUE,
                                schema = DM,
                                fk_parent_tables = SimpleFKTables),
    pattern = map(SpecifiedIEADataLocal)),


  # Animal muscle work data ----------------------------------------------------

  ## MWConcordancePath
  targets::tar_target_raw(
    "MWConcordancePath",
    clpfu_setup_paths[["mw_concordance_path"]],
    format = "file"),

  ## AMWAnalysisDataPath
  targets::tar_target_raw(
    "AMWAnalysisDataPath",
    clpfu_setup_paths[["amw_analysis_data_path"]],
    format = "file"),

  ## FAODataLocal
  targets::tar_target(
    FAODataLocal,
    FAOSTAT::get_faostat_bulk(code = "QCL", # Live animals code
                              data_folder = tempdir())),

  ## AMWPFUDataRawLocal
  targets::tar_target(
    AMWPFUDataRawLocal,
    prep_amw_pfu_data(fao_data = FAODataLocal,
                      mw_concordance_path = MWConcordancePath,
                      amw_analysis_data_path = AMWAnalysisDataPath) |>
      dplyr::filter(Country %in% AllocAndEffCountries,
                    Year %in% years)),

  ## AMWPFUDataLocal
  targets::tar_target(
    AMWPFUDataLocal,
    aggcountries_mw_to_iea(mw_df = AMWPFUDataRawLocal,
                           exemplar_table = ExemplarTable,
                           dataset = clpfu_dataset)),

  ## AMWPFUData
  targets::tar_target(
    AMWPFUData,
    PFUPipelineTools::pl_upsert(AMWPFUDataLocal,
                                db_table_name = "AMWPFUData",
                                conn = conn,
                                in_place = TRUE,
                                schema = DM,
                                fk_parent_tables = SimpleFKTables)),


  # Human muscle work data -----------------------------------------------------

  ## HMWAnalysisDataPath
  targets::tar_target_raw(
    "HMWAnalysisDataPath",
    clpfu_setup_paths[["hmw_analysis_data_path"]],
    format = "file"),

  ## ILOWorkingHoursDataLocal
  targets::tar_target(
    ILOWorkingHoursDataLocal,
    Rilostat::get_ilostat(id = "HOW_TEMP_SEX_ECO_NB_A", # Working hours code
                          quiet = TRUE) |>
      Rilostat::label_ilostat(code = c("ref_area"))),

  ## ILOEmploymentDataLocal
  targets::tar_target(
    ILOEmploymentDataLocal,
    Rilostat::get_ilostat(id = "EMP_TEMP_SEX_ECO_NB_A", # Employment code
                          quiet = TRUE) |>
      Rilostat::label_ilostat(code = c("ref_area"))),

  ## HMWPFUDataRaw
  targets::tar_target(
    HMWPFUDataRawLocal,
    prep_hmw_pfu_data(ilo_working_hours_data = ILOWorkingHoursDataLocal,
                      ilo_employment_data = ILOEmploymentDataLocal,
                      mw_concordance_path = MWConcordancePath,
                      hmw_analysis_data_path = HMWAnalysisDataPath) |>
      dplyr::filter(Country %in% AllocAndEffCountries,
                    Year %in% years)),

  ## HMWPFUDataLocal
  targets::tar_target(
    HMWPFUDataLocal,
    aggcountries_mw_to_iea(mw_df = HMWPFUDataRawLocal,
                           exemplar_table = ExemplarTable,
                           dataset = clpfu_dataset)),

  ## HMWPFUData
  targets::tar_target(
    HMWPFUData,
    PFUPipelineTools::pl_upsert(HMWPFUDataLocal,
                                db_table_name = "HMWPFUData",
                                conn = conn,
                                in_place = TRUE,
                                schema = DM,
                                fk_parent_tables = SimpleFKTables)),


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
    load_fu_allocation_tables(fu_analysis_folder = FUAnalysisFolder,
                              specified_iea_data = SpecifiedIEAData,
                              countries = AllocAndEffCountries,
                              conn = conn,
                              schema = DM,
                              fk_parent_tables = SimpleFKTables),
    Country),

  targets::tar_target(
    IncompleteAllocationTables,
    PFUPipelineTools::pl_upsert(IncompleteAllocationTablesLocal,
                                db_table_name = "IncompleteAllocationTables",
                                conn = conn,
                                in_place = TRUE,
                                schema = DM,
                                fk_parent_tables = SimpleFKTables),
    pattern = map(IncompleteAllocationTablesLocal)
  ),

  ## CompletedAllocationTablesLocal
  targets::tar_target(
    CompletedAllocationTablesLocal,
    assemble_fu_allocation_tables(incomplete_allocation_tables = IncompleteAllocationTablesLocal,
                                  exemplar_lists = ExemplarLists,
                                  specified_iea_data = SpecifiedIEADataLocal |>
                                    PFUPipelineTools::tar_ungroup(),
                                  dataset = clpfu_dataset,
                                  countries = Countries,
                                  years = Years),
    pattern = map(Countries)),

  ## CompletedAllocationTables
  targets::tar_target(
    CompletedAllocationTables,
    PFUPipelineTools::pl_upsert(CompletedAllocationTablesLocal,
                                db_table_name = "CompletedAllocationTables",
                                conn = conn,
                                in_place = TRUE,
                                schema = DM,
                                fk_parent_tables = SimpleFKTables),
    pattern = map(CompletedAllocationTablesLocal)
  ),


  # Efficiency tables ----------------------------------------------------------

  # Dependencies among MachineData targets:
  #
  #                        AllMachineData             MachineData             CompletedEfficiencyTables
  #                         ^                          ^                       ^
  #                         |                          |                       |
  #                         |                          |                       |
  # MachineDataPath -----> AllMachineDataLocal -----> MachineDataLocal -----> CompletedEfficiencyTablesLocal

  ## MachineDataPath
  targets::tar_target_raw(
    "MachineDataPath",
    clpfu_setup_paths[["machine_data_folder"]],
    format = "file"),

  ## AllMachineDataLocal
  targets::tar_target(
    AllMachineDataLocal,
    read_all_eta_files(eta_fin_paths = get_eta_filepaths(MachineDataPath),
                       dataset = clpfu_dataset)),

  ## AllMachineData
  targets::tar_target(
    AllMachineData,
    PFUPipelineTools::pl_upsert(AllMachineDataLocal,
                                db_table_name = "AllMachineData",
                                conn = conn,
                                in_place = TRUE,
                                schema = DM,
                                fk_parent_tables = SimpleFKTables)),

  ## MachineDataLocal
  tarchetypes::tar_group_by(
    MachineDataLocal,
    dplyr::filter(AllMachineDataLocal,
                  Country %in% AllocAndEffCountries,
                  Year %in% years),
    Country),

  ## MachineData
  targets::tar_target(
    MachineData,
    PFUPipelineTools::pl_upsert(MachineDataLocal,
                                db_table_name = "MachineData",
                                conn = conn,
                                in_place = TRUE,
                                schema = DM,
                                fk_parent_tables = SimpleFKTables),
    pattern = map(MachineDataLocal)),

  ## CompletedEfficiencyTablesLocal
  targets::tar_target(
    CompletedEfficiencyTablesLocal,
    assemble_eta_fu_tables(incomplete_eta_fu_tables = MachineDataLocal,
                           exemplar_lists = ExemplarLists,
                           completed_fu_allocation_tables = CompletedAllocationTablesLocal,
                           dataset = clpfu_dataset,
                           countries = Countries,
                           years = Years,
                           which_quantity = IEATools::template_cols$eta_fu),
    pattern = map(Countries)),

  ## CompletedEfficiencyTables
  targets::tar_target(
    CompletedEfficiencyTables,
    PFUPipelineTools::pl_upsert(CompletedEfficiencyTablesLocal,
                                db_table_name = "CompletedEfficiencyTables",
                                conn = conn,
                                in_place = TRUE,
                                schema = DM,
                                fk_parent_tables = SimpleFKTables))




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
