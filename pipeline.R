
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

  ## FKTables
  #  Extract the foreign key tables
  targets::tar_target(
    FKTables,
    SetDMAndFKTables[["fk_tables"]]),

  ## matnameRCType
  #  Get the mapping between matrix names and row/col types
  #  for later downloading matsindf data frames
  targets::tar_target(
    MatnameRCType,
    PFUPipelineTools::pl_filter_collect("matnameRCType",
                                        conn = conn,
                                        collect = TRUE,
                                        schema = DM,
                                        fk_parent_tables = FKTables)),

  ## IndexMap
  #  Get the map for Industry, Product, and Other indices for matrices
  targets::tar_target(
    IndexMap,
    create_index_map(conn = conn,
                     schema = DM,
                     fk_parent_tables = FKTables)),


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
  # AllIEAData ---> IEAData ---> BalancedIEAData ---> SpecifiedIEAData
  #  ^               |            |
  #  |               |            |--|
  #  |               |               |
  #  |               v               v
  # IEADataPath  BalancedBeforeIEA  BalancedAfterIEA ---> OKToProceedIEA

  ## IEADataPath
  targets::tar_target_raw(
    "IEADataPath",
    clpfu_setup_paths[["iea_data_path"]],
    format = "file"),

  ## AllIEAData
  targets::tar_target(
    AllIEAData,
    load_iea_data(iea_data_path = IEADataPath,
                  override_df = CountryConcordanceTable,
                  dataset = iea_dataset,
                  specify_non_energy_flows = SpecifyNonEnergyFlows,
                  apply_fixes = ApplyFixes,
                  db_table_name = db_table_name,
                  conn = conn,
                  schema = DM,
                  fk_parent_tables = FKTables)),

  ## IEAData
  tarchetypes::tar_group_by(
    IEAData,
    PFUPipelineTools::inboard_filter_copy(source = "AllIEAData",
                                          dest = db_table_name,
                                          countries = Countries,
                                          years = Years,
                                          empty_dest = TRUE,
                                          in_place = TRUE,
                                          dependencies = AllIEAData,
                                          conn = conn),
    Country),

  ## BalancedBeforeIEA
  targets::tar_target(
    BalancedBeforeIEA,
    is_balanced(IEAData,
                conn = conn,
                schema = DM,
                fk_parent_tables = FKTables),
    pattern = map(IEAData)),

  ## BalancedIEAData
  targets::tar_target(
    BalancedIEAData,
    make_balanced(IEAData,
                  db_table_name = db_table_name,
                  conn = conn,
                  schema = DM,
                  fk_parent_tables = FKTables),
    pattern = map(IEAData)),

  ## BalancedAfterIEA
  targets::tar_target(
    BalancedAfterIEA,
    is_balanced(BalancedIEAData,
                conn = conn,
                schema = DM,
                fk_parent_tables = FKTables),
    pattern = map(BalancedIEAData)),

  ## OKToProceedIEA
  targets::tar_target(
    OKToProceedIEA,
    ifelse(is.null(stopifnot(all(BalancedAfterIEA))), yes = TRUE, no = FALSE)),

  ## SpecifiedIEAData
  targets::tar_target(
    SpecifiedIEAData,
    specify(BalancedIEAData,
            db_table_name = db_table_name,
            conn = conn,
            schema = DM,
            fk_parent_tables = FKTables),
    pattern = map(BalancedIEAData)),


  # Animal muscle work data ----------------------------------------------------

  ## MWConcordancePath
  targets::tar_target_raw(
    "MWConcordancePath",
    clpfu_setup_paths[["mw_concordance_path"]],
    format = "file"),

  # Dependencies among AMW targets

  #                          AMWPFUDataRaw ---> AMWPFUData
  #                           ^
  #                           |
  #                           |
  # AMWAnalysisDataPath ---> FAODataLocal

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

  ## AMWPFUDataRaw
  targets::tar_target(
    AMWPFUDataRaw,
    prep_amw_pfu_data(fao_data = FAODataLocal,
                      mw_concordance_path = MWConcordancePath,
                      amw_analysis_data_path = AMWAnalysisDataPath,
                      db_table_name = db_table_name,
                      countries = AllocAndEffCountries,
                      years = Years,
                      dataset = clpfu_dataset,
                      conn = conn,
                      schema = DM,
                      fk_parent_tables = FKTables)),

  ## AMWPFUData
  targets::tar_target(
    AMWPFUData,
    aggcountries_mw_to_iea(mw_df = AMWPFUDataRaw,
                           exemplar_table = ExemplarTable,
                           db_table_name = db_table_name,
                           dataset = clpfu_dataset,
                           conn = conn,
                           schema = DM,
                           fk_parent_tables = FKTables)),


  # Human muscle work data -----------------------------------------------------

  # Dependencies among HMW targets

  #                         HMWPFUDataRaw ---> HMWPFUData
  #                          ^  ^  ^
  #                          |  |  |
  #                          |  |  |
  # HMWAnalysisDataPath -----|  |  |
  # ILOEmploymentDataLocal -----|  |
  # ILOWorkingHoursDataLocal ------|

  ## HMWAnalysisDataPath
  targets::tar_target_raw(
    "HMWAnalysisDataPath",
    clpfu_setup_paths[["hmw_analysis_data_path"]],
    format = "file"),

  ## ILOEmploymentDataLocal
  targets::tar_target(
    ILOEmploymentDataLocal,
    Rilostat::get_ilostat(id = "EMP_TEMP_SEX_ECO_NB_A", # Employment code
                          quiet = TRUE) |>
      Rilostat::label_ilostat(code = c("ref_area"))),

  ## ILOWorkingHoursDataLocal
  targets::tar_target(
    ILOWorkingHoursDataLocal,
    Rilostat::get_ilostat(id = "HOW_TEMP_SEX_ECO_NB_A", # Working hours code
                          quiet = TRUE) |>
      Rilostat::label_ilostat(code = c("ref_area"))),

  ## HMWPFUDataRaw
  targets::tar_target(
    HMWPFUDataRaw,
    prep_hmw_pfu_data(ilo_working_hours_data = ILOWorkingHoursDataLocal,
                      ilo_employment_data = ILOEmploymentDataLocal,
                      mw_concordance_path = MWConcordancePath,
                      hmw_analysis_data_path = HMWAnalysisDataPath,
                      db_table_name = db_table_name,
                      countries = AllocAndEffCountries,
                      years = Years,
                      dataset = clpfu_dataset,
                      conn = conn,
                      schema = DM,
                      fk_parent_tables = FKTables)),

  ## HMWPFUData
  targets::tar_target(
    HMWPFUData,
    aggcountries_mw_to_iea(mw_df = HMWPFUDataRaw,
                           exemplar_table = ExemplarTable,
                           db_table_name = db_table_name,
                           dataset = clpfu_dataset,
                           conn = conn,
                           schema = DM,
                           fk_parent_tables = FKTables)),


  # Allocation tables ----------------------------------------------------------

  # Dependencies among AllocationTable targets:
  #
  # IncompleteAllocationTables ---> CompletedAllocationTables ---> Cmats
  #  ^
  #  |
  #  |
  # FUAnalysisFolder

  ## FUAnalysisFolder
  targets::tar_target_raw(
    "FUAnalysisFolder",
    clpfu_setup_paths$fu_allocation_folder,
    format = "file"),

  ## IncompleteAllocationTables
  tarchetypes::tar_group_by(
    IncompleteAllocationTables,
    load_fu_allocation_tables(fu_analysis_folder = FUAnalysisFolder,
                              specified_iea_data = SpecifiedIEAData,
                              countries = AllocAndEffCountries,
                              dataset = clpfu_dataset,
                              db_table_name = db_table_name,
                              conn = conn,
                              schema = DM,
                              fk_parent_tables = FKTables),
    Country),

  ## CompletedAllocationTables
  targets::tar_target(
    CompletedAllocationTables,
    assemble_fu_allocation_tables(incomplete_allocation_tables = IncompleteAllocationTables,
                                  exemplar_lists = ExemplarLists,
                                  specified_iea_data = SpecifiedIEAData,
                                  countries = Countries,
                                  years = Years,
                                  dataset = clpfu_dataset,
                                  db_table_name = db_table_name,
                                  conn = conn,
                                  schema = DM,
                                  fk_parent_tables = FKTables),
    pattern = map(Countries)),

  ## Cmats
  targets::tar_target(
    Cmats,
    calc_C_mats(completed_allocation_tables = CompletedAllocationTables,
                countries = Countries,
                dataset = clpfu_dataset,
                db_table_name = db_table_name,
                conn = conn,
                schema = DM,
                fk_parent_tables = FKTables),
    pattern = map(Countries)),



  # Efficiency tables ----------------------------------------------------------

  # Dependencies among MachineData targets:
  #
  # AllMachineData ---> MachineData ---> CompletedEfficiencyTables
  #  ^
  #  |
  #  |
  # MachineDataPath

  ## MachineDataPath
  targets::tar_target_raw(
    "MachineDataPath",
    clpfu_setup_paths[["machine_data_folder"]],
    format = "file"),

  ## AllMachineData
  targets::tar_target(
    AllMachineData,
    read_all_eta_files(eta_fin_paths = get_eta_filepaths(MachineDataPath),
                       dataset = clpfu_dataset,
                       db_table_name = db_table_name,
                       conn = conn,
                       schema = DM,
                       fk_parent_tables = FKTables)),

  ## MachineData
  targets::tar_target(
    MachineData,
    PFUPipelineTools::inboard_filter_copy(source = "AllMachineData",
                                          dest = db_table_name,
                                          countries = AllocAndEffCountries,
                                          years = Years,
                                          empty_dest = TRUE,
                                          in_place = TRUE,
                                          dependencies = AllMachineData,
                                          conn = conn)),

  ## CompletedEfficiencyTables
  targets::tar_target(
    CompletedEfficiencyTables,
    assemble_eta_fu_tables(incomplete_eta_fu_tables = MachineData,
                           exemplar_lists = ExemplarLists,
                           completed_fu_allocation_tables = CompletedAllocationTables,
                           countries = Countries,
                           years = Years,
                           dataset = clpfu_dataset,
                           db_table_name = db_table_name,
                           conn = conn,
                           schema = DM,
                           fk_parent_tables = FKTables,
                           which_quantity = IEATools::template_cols$eta_fu),
    pattern = map(Countries)),


  # Phi values -----------------------------------------------------------------

  ## PhiConstantsPath
  targets::tar_target_raw(
    "PhiConstantsPath",
    clpfu_setup_paths[["phi_constants_path"]],
    format = "file"),

  ## PhiConstants
  targets::tar_target(
    PhiConstants,
    load_phi_values(phi_constants_path = PhiConstantsPath,
                    dataset = clpfu_dataset,
                    db_table_name = db_table_name,
                    conn = conn,
                    schema = DM,
                    fk_parent_tables = FKTables)),

  ## CompletedPhiuTables
  targets::tar_target(
    CompletedPhiuTables,
    assemble_phi_u_tables(incomplete_phi_u_table = MachineData,
                          phi_constants_table = PhiConstants,
                          completed_efficiency_table = CompletedEfficiencyTables,
                          countries = Countries,
                          years = Years,
                          dataset = clpfu_dataset,
                          db_table_name = db_table_name,
                          conn = conn,
                          schema = DM,
                          fk_parent_tables = FKTables),
    pattern = map(Countries)),

  ## Phipfvecs
  targets::tar_target(
    Phipfvecs,
    calc_phi_pf_vecs(phi_constants = PhiConstants,
                     phi_u_vecs = Phiuvecs,
                     countries = Countries,
                     dataset = clpfu_dataset,
                     db_table_name = db_table_name,
                     index_map = IndexMap,
                     rctypes = MatnameRCType,
                     conn = conn,
                     schema = DM,
                     fk_parent_tables = FKTables),
    pattern = map(Countries)),


  # eta and phi vectors --------------------------------------------------------

  ## EtafuPhiuvecs
  targets::tar_target(
    EtafuPhiuvecs,
    calc_eta_fu_phi_u_vecs(completed_efficiency_tables = CompletedEfficiencyTables,
                           completed_phi_tables = CompletedPhiuTables,
                           countries = Countries,
                           dataset = clpfu_dataset,
                           db_table_name = db_table_name,
                           conn = conn,
                           schema = DM,
                           fk_parent_tables = FKTables),
    pattern = map(Countries)),

  ## Etafuvecs
  targets::tar_target(
    Etafuvecs,
    sep_eta_fu_phi_u(EtafuPhiuvecs,
                     keep = IEATools::template_cols$eta_fu,
                     countries = Countries,
                     index_map = IndexMap,
                     rctypes = MatnameRCType,
                     dataset = clpfu_dataset,
                     db_table_name = db_table_name,
                     conn = conn,
                     schema = DM,
                     fk_parent_tables = FKTables),
    pattern = map(Countries)),

  ## Phiuvecs
  targets::tar_target(
    Phiuvecs,
    sep_eta_fu_phi_u(EtafuPhiuvecs,
                     keep = IEATools::template_cols$phi_u,
                     countries = Countries,
                     index_map = IndexMap,
                     rctypes = MatnameRCType,
                     dataset = clpfu_dataset,
                     db_table_name = db_table_name,
                     conn = conn,
                     schema = DM,
                     fk_parent_tables = FKTables),
    pattern = map(Countries))





) |>


  # tar_hook_before targets ----------------------------------------------------

  tarchetypes::tar_hook_before(
    hook = {
      # Ensure each target has access to the database,
      # using the hint found at https://github.com/ropensci/targets/discussions/1164.
      conn <- DBI::dbConnect(drv = RPostgres::Postgres(),
                             dbname = conn_params$dbname,
                             host = conn_params$host,
                             port = conn_params$port,
                             user = conn_params$user)
      # Make sure that the connection will be closed
      # after each target completes.
      on.exit(DBI::dbDisconnect(conn))
      # By default, make the target name available as the name
      # of the database table in which the result should be stored.
      # But keep everything before the underscore,
      # if it exists in the string.
      db_table_name <- strsplit(targets::tar_name(), "_")[[1]][[1]]
    })
