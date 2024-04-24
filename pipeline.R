
# This is the targets pipeline

# Note that this pipeline contains several complicated targets
# where the main input is processed with one country and/or year
# in each thread but other inputs need to be included in whole.
# Thus, this pipeline is *not* compatible with
# iterating over tar_groups.
# Iterations should be controlled by `map`ing over appropriate inputs.

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
    "IEADataFolder",
    clpfu_setup_paths[["iea_data_folder"]],
    format = "file"),

  ## AllIEAData
  targets::tar_target(
    AllIEAData,
    load_iea_data(iea_data_path = IEADataFolder,
                  countries = AllocAndEffCountries,
                  base_iea_country_filename = clpfu_setup_paths[["base_iea_country_filename"]],
                  override_df = CountryConcordanceTable,
                  specify_non_energy_flows = SpecifyNonEnergyFlows,
                  apply_fixes = ApplyFixes)),

  ## IEAData
  targets::tar_target(
    IEAData,
    PFUPipelineTools::inboard_filter_copy(source = "AllIEAData",
                                          dest = db_table_name_from_hook_before,
                                          countries = AllocAndEffCountries,
                                          years = Years,
                                          empty_dest = TRUE,
                                          in_place = TRUE,
                                          dependencies = AllIEAData,
                                          conn = conn)),

  ## BalancedBeforeIEA
  targets::tar_target(
    BalancedBeforeIEA,
    is_balanced(IEAData,
                countries = AllocAndEffCountries),
    pattern = map(AllocAndEffCountries)),

  ## BalancedIEAData
  targets::tar_target(
    BalancedIEAData,
    make_balanced(IEAData,
                  countries = AllocAndEffCountries),
    pattern = map(AllocAndEffCountries)),

  ## BalancedAfterIEA
  targets::tar_target(
    BalancedAfterIEA,
    is_balanced(BalancedIEAData,
                countries = AllocAndEffCountries),
    pattern = map(AllocAndEffCountries)),

  ## OKToProceedIEA
  targets::tar_target(
    OKToProceedIEA,
    ifelse(is.null(stopifnot(all(BalancedAfterIEA))), yes = TRUE, no = FALSE)),

  ## SpecifiedIEAData
  targets::tar_target(
    SpecifiedIEAData,
    specify(BalancedIEAData,
            countries = AllocAndEffCountries),
    pattern = map(AllocAndEffCountries)),

  ## PSUTFinalIEA
  targets::tar_target(
    PSUTFinalIEA,
    make_iea_psut(SpecifiedIEAData,
                  countries = AllocAndEffCountries),
    pattern = map(AllocAndEffCountries)),


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
                      countries = AllocAndEffCountries,
                      years = Years)),

  ## AMWPFUData
  targets::tar_target(
    AMWPFUData,
    aggcountries_mw_to_iea(mw_df = AMWPFUDataRaw,
                           exemplar_table = ExemplarTable)),


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
                      countries = AllocAndEffCountries,
                      years = Years)),

  ## HMWPFUData
  targets::tar_target(
    HMWPFUData,
    aggcountries_mw_to_iea(mw_df = HMWPFUDataRaw,
                           exemplar_table = ExemplarTable)),


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
  targets::tar_target(
    IncompleteAllocationTables,
    load_fu_allocation_tables(fu_analysis_folder = FUAnalysisFolder,
                              specified_iea_data = SpecifiedIEAData,
                              countries = AllocAndEffCountries),
    pattern = map(AllocAndEffCountries)),

  ## CompletedAllocationTables
  targets::tar_target(
    CompletedAllocationTables,
    assemble_fu_allocation_tables(incomplete_allocation_tables = IncompleteAllocationTables,
                                  exemplar_lists = ExemplarLists,
                                  specified_iea_data = SpecifiedIEAData,
                                  countries = Countries,
                                  years = Years,
                                  dataset = clpfu_dataset,
                                  db_table_name = db_table_name_from_hook_before,
                                  conn = conn,
                                  schema = DM,
                                  fk_parent_tables = FKTables),
    pattern = map(Countries)),

  ## Cmats
  targets::tar_target(
    Cmats,
    calc_C_mats(completed_allocation_tables = CompletedAllocationTables,
                countries = Countries),
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
    read_all_eta_files(eta_fin_paths = get_eta_filepaths(MachineDataPath))),

  ## MachineData
  targets::tar_target(
    MachineData,
    PFUPipelineTools::inboard_filter_copy(source = "AllMachineData",
                                          dest = db_table_name_from_hook_before,
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
    load_phi_values(phi_constants_path = PhiConstantsPath)),

  ## CompletedPhiuTables
  targets::tar_target(
    CompletedPhiuTables,
    assemble_phi_u_tables(incomplete_phi_u_table = MachineData,
                          phi_constants_table = PhiConstants,
                          completed_efficiency_table = CompletedEfficiencyTables,
                          countries = Countries,
                          years = Years),
    pattern = map(Countries)),

  ## Phipfvecs
  targets::tar_target(
    Phipfvecs,
    calc_phi_pf_vecs(phi_constants = PhiConstants,
                     phi_u_vecs = Phiuvecs,
                     countries = Countries),
    pattern = map(Countries)),


  # eta and phi vectors --------------------------------------------------------

  ## EtafuPhiuvecs
  targets::tar_target(
    EtafuPhiuvecs,
    calc_eta_fu_phi_u_vecs(completed_efficiency_tables = CompletedEfficiencyTables,
                           completed_phi_tables = CompletedPhiuTables,
                           countries = Countries),
    pattern = map(Countries)),

  ## Etafuvecs
  targets::tar_target(
    Etafuvecs,
    sep_eta_fu_phi_u(EtafuPhiuvecs,
                     keep = IEATools::template_cols$eta_fu,
                     countries = Countries),
    pattern = map(Countries)),

  ## Phiuvecs
  targets::tar_target(
    Phiuvecs,
    sep_eta_fu_phi_u(EtafuPhiuvecs,
                     keep = IEATools::template_cols$phi_u,
                     countries = Countries),
    pattern = map(Countries)),

  ## Phivecs
  targets::tar_target(
    Phivecs,
    sum_phi_vecs(phi_pf_vecs = Phipfvecs,
                 phi_u_vecs = Phiuvecs,
                 countries = Countries),
    pattern = map(Countries)),


  # Extend IEA data to useful stage --------------------------------------------

  ## PSUTUsefulIEAWithDetails
  targets::tar_target(
    PSUTUsefulIEAWithDetails,
    move_to_useful_with_details(psut_final = PSUTFinalIEA,
                                C_mats = Cmats,
                                eta_phi_vecs = EtafuPhiuvecs,
                                countries = Countries),
    pattern = map(Countries)),

  ## PSUTUsefulIEA
  #  Keep only the PSUT matrices for the energy conversion chains
  targets::tar_target(
    PSUTUsefulIEA,
    PSUTUsefulIEAWithDetails |>
            remove_cols_from_PSUTUsefulIEAWithDetails(
              cols_to_remove = c(IEATools::psut_cols$Y_fu_details,
                                 IEATools::psut_cols$U_eiou_fu_details),
              countries = Countries),
    pattern = map(Countries)),

  ## YfuUEIOUfudetailsEnergy
  #  Keep only the details matrices for a different database product
  targets::tar_target(
    YfuUEIOUfudetailsEnergy,
    PSUTUsefulIEAWithDetails |>
            remove_cols_from_PSUTUsefulIEAWithDetails(
              cols_to_remove = c(IEATools::psut_cols$R,
                                 IEATools::psut_cols$U,
                                 IEATools::psut_cols$U_feed,
                                 IEATools::psut_cols$U_eiou,
                                 IEATools::psut_cols$r_eiou,
                                 IEATools::psut_cols$V,
                                 IEATools::psut_cols$Y,
                                 IEATools::psut_cols$s_units),
              remove_final = TRUE, # This data frame has only useful stage.
              countries = Countries),
    pattern = map(Countries)),


  # Extend useful IEA data to exergy -------------------------------------------

  ## PSUTIEA
  targets::tar_target(
    PSUTIEA,
    move_to_exergy(psut_energy = PSUTUsefulIEA,
                   phi_vecs = Phivecs,
                   countries = Countries),
    pattern = map(Countries)),


  # Make PSUT matrices for MW data ---------------------------------------------

  ## PSUTMWEnergy
  targets::tar_target(
    PSUTMWEnergy,
    make_mw_psut(.hmw_df = HMWPFUData,
                 .amw_df = AMWPFUData,
                 countries = Countries),
    pattern = map(Countries)),

  ## BalancedPSUTMW
  targets::tar_target(
    BalancedPSUTMW,
    verify_mw_energy_balance(PSUTMWEnergy,
                             countries = Countries),
    pattern = map(Countries)),

  # Don't continue if there is a problem with the MW data.
  # stopifnot returns NULL if everything is OK.
  targets::tar_target(
    OKToProceedMW,
    ifelse(is.null(stopifnot(BalancedPSUTMW)), yes = TRUE, no = FALSE)),

  ## PhivecMW
  #  Create a single MW phi vector applicable to all years.
  #  This target is NOT uploaded to the database.
  targets::tar_target(
    PhivecMW,
    MWTools::phi_vec_mw(.phi_table = PhiConstants,
                        mw_energy_carriers = MWTools::mw_products,
                        matrix_class = "Matrix")),

  ## PhivecsMW
  # This target provides a MW phi vector for every Country-Year combination.
  # Note plural spelling.
  targets::tar_target(
    PhivecsMW,
    calc_phi_vecs_mw(psut_energy_mw = PSUTMWEnergy,
                     phi_vec_mw = PhivecMW,
                     countries = Countries),
    pattern = map(Countries)),

  ## PSUTMWAllYears
  targets::tar_target(
    PSUTMWAllYears,
    move_to_exergy(psut_energy = PSUTMWEnergy,
                   phi_vecs = PhivecsMW,
                   countries = Countries),
    pattern = map(Countries)),

  ## PSUTMW
  #  Trim MW to years also available in IEA
  targets::tar_target(
    PSUTMW,
    filter_mw_to_iea_years(PSUTMWAllYears,
                           PSUTIEA,
                           countries = Countries),
    pattern = map(Countries)),


  # PSUT -----------------------------------------------------------------------

  ## PSUTIEAMW
  #  Combine IEA and MW data by summing PSUT matrices
  targets::tar_target(
    PSUTIEAMW,
    add_iea_mw_psut(PSUTIEA, PSUTMW,
                    countries = Countries),
    pattern = map(Countries)),

  ## PSUT
  #  Put everything in the same data frame
  targets::tar_target(
    PSUT,
    build_psut_dataframe(psutiea = PSUTIEA,
                         psutmw = PSUTMW,
                         psutieamw = PSUTIEAMW,
                         countries = Countries),
    pattern = map(Countries)),


  # Aggregations ---------------------------------------------------------------

  ## CmatsAgg
  targets::tar_target(
    CmatsAgg,
    calc_C_mats_agg(C_mats = Cmats,
                    psut_iea = PSUTIEA,
                    countries = Countries),
    pattern = map(Countries)),


  # Efficiencies ---------------------------------------------------------------

  ## EtafuYEIOU
  targets::tar_target(
    EtafuYEIOU,
    calc_fu_Y_EIOU_efficiencies(C_mats = Cmats,
                                eta_m_vecs = Etafuvecs,
                                phi_vecs = Phivecs,
                                countries = Countries),
    pattern = map(Countries)),

  ## Etai
  targets::tar_target(
    Etai,
    calc_eta_i(.psut = PSUT, countries = Countries),
    pattern = map(Countries)),


  # Exiobase -------------------------------------------------------------------

  ## EtafuYEIOUagg
  #  Calculating the product efficiency at the
  #  (a) EIOU-wide,
  #  (b) Y-wide, and
  #  (c) economy-wide levels
  # targets::tar_target(
  #   EtafuYEIOUagg,
  #   calc_fu_Y_EIOU_agg_efficiencies(C_mats_agg = CmatsAgg,
  #                                   eta_fu_vecs = Etafuvecs,
  #                                   phi_vecs = Phivecs,
  #                                   countries = Countries),
  #   pattern = map(Countries)) # ,

  # targets::tar_target(
  #   ExiobaseEftoEuMultipliers,
  #   calc_Ef_to_Eu_exiobase(eta_fu_Y_EIOU_mats = EtafuYEIOU,
  #                          eta_fu_Y_EIOU_agg = EtafuYEIOUagg,
  #                          years_exiobase = ExiobaseYears,
  #                          full_list_exiobase_flows = ListExiobaseEnergyFlows,
  #                          country_concordance_table_df = CountryConcordanceTable)),

  ## exiobase_Ef_to_Eloss_multipliers
  # targets::tar_target(
  #   ExiobaseEftoElossMultipliers,
  #   calc_Ef_to_Eloss_exiobase(ExiobaseEftoEuMultipliers)),
  #
  # targets::tar_target(
  #   ReleaseExiobaseEftoElossMultipliers,
  #   PFUPipelineTools::release_target(pipeline_releases_folder = PipelineReleasesFolder,
  #                                    targ = ExiobaseEftoElossMultipliers,
  #                                    pin_name = "exiobase_Ef_to_Eloss_multipliers",
  #                                    type = "csv",
  #                                    release = Release)),


  # targets::tar_target(
  #   ListExiobaseEnergyFlows,
  #   read_list_exiobase_energy_flows(path_to_list_exiobase_energy_flows = ExiobaseEnergyFlowsPath)),
  #
  #
  #

  # Phi values
  # Multiplier to go from final energy to final exergy
  # targets::tar_target(
  #   ExiobaseEftoXfMultipliers,
  #   calc_Ef_to_Xf_exiobase(phi_vecs = Phivecs,
  #                          years_exiobase = ExiobaseYears,
  #                          full_list_exiobase_flows = ListExiobaseEnergyFlows,
  #                          country_concordance_table_df = CountryConcordanceTable)),

  #
  # Product K: exiobase_Ef_to_Xloss_multipliers ------------------------------
  #
  # Final energy to exergy losses multipliers
  # Multiplier to go from final energy to exergy losses
  # targets::tar_target(
  #   ExiobaseEftoXlossMultipliers,
  #   calc_Ef_to_Xloss_exiobase(ExiobaseEftoXuMultipliers)),


  # Remove NEU -----------------------------------------------------------------

  ## PSUTWithoutNEU
  #  Calculate a version of the PSUT data frame with all Non-energy use removed.
  targets::tar_target(
    PSUTWithoutNEU,
    remove_non_energy_use(PSUT,
                          countries = Countries),
    pattern = map(Countries)),


  # Aggregation maps -----------------------------------------------------------

  ## AggregationMapsPath
  targets::tar_target_raw(
    "AggregationMapsPath",
    clpfu_setup_paths[["aggregation_maps_path"]],
    format = "file"),

  ## AggregationMaps
  targets::tar_target(
    AggregationMaps,
    load_aggregation_maps(path = AggregationMapsPath)),

  ## ProductAggMap
  #  Separate the product aggregation map
  targets::tar_target(
    ProductAggMap,
    c(AggregationMaps[["ef_product_aggregation"]],
      AggregationMaps[["eu_product_aggregation"]])),

  ## IndustryAggMap
  #  Separate the industry aggregation map
  targets::tar_target(
    IndustryAggMap,
    AggregationMaps[["ef_sector_aggregation"]]),

  ## PIndustryPrefixes
  #  Establish prefixes for primary industries
  targets::tar_target(
    PIndustryPrefixes,
    IEATools::tpes_flows |> unname() |> unlist() |> list()),

  ## FinalDemandSectors
  #  Establish final demand sectors
  targets::tar_target(
    FinalDemandSectors,
    create_fd_sectors_list(IEATools::fd_sectors, AggregationMaps$ef_sector_aggregation)),

  ## Regions
  #  Identify the regions to which we'll aggregate
  targets::tar_target(
    Regions,
    names(AggregationMaps$region_aggregation)),

  ## Continents
  #  Identify the continents to which we'll aggregate
  targets::tar_target(
    Continents,
    AggregationMaps$world_aggregation$World),

  ## CountriesRegionsContinentsWorld
  targets::tar_target(
    CountriesRegionsContinentsWorld,
    c(Countries, Regions, Continents, "World")),


  # Regional aggregations ------------------------------------------------------

  ## PSUTReAll
  #  ********** Need to upload this to the database yet.
  #  However, first
  #  * Rename PSUT target to PSUTWithNEU
  #  * Combine PSUTWithNEU and PSUTWithoutNEU into PSUT with added column WithNEU (values 0 and 1 for FALSE and TRUE)
  #  * Build all aggregations with only PSUT
  targets::tar_target(
    PSUTReAll,
    region_pipeline(PSUT,
                    region_aggregation_map = AggregationMaps$region_aggregation,
                    continent_aggregation_map = AggregationMaps$continent_aggregation,
                    world_aggregation_map = AggregationMaps$world_aggregation,
                    countries = Countries),
    pattern = map(Countries)) # ,




) |>


  # tar_hook_before targets ----------------------------------------------------

  ## This hook supplies the following items to all targets:
  ## * the database connection,
  ## * the table name, and
  ## * the dataset.
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
      db_table_name_from_hook_before <- db_table_name_hook(targets::tar_name())

      # Now use the db_table_name to decide which dataset is being created
      if (db_table_name_from_hook_before %in% c("AllIEAData", "IEAData", "BalancedIEAData", "SpecifiedIEAData")) {
        # Working on the IEA data
        dataset_from_hook <- iea_dataset
      } else {
        # Everything else is in the CL-PFU dataset
        dataset_from_hook <- clpfu_dataset
      }}
  ) |>


  # tar_hook_inner -------------------------------------------------------------

  ## An inner hook for targets where Countries
  ## is the mapped variable.
  ## This is the typical inner hook.
  tarchetypes::tar_hook_inner(
    hook = download_dependency_hook(.x,
                                    countries = Countries,
                                    years = Years,
                                    index_map = IndexMap,
                                    rctypes = MatnameRCType,
                                    conn = conn,
                                    schema = DM,
                                    fk_parent_tables = FKTables),
    names = c("Cmats",
              "AMWPFUData", "HMWPFUData",
              "CompletedPhiuTables", "Phipfvecs", "Phiuvecs", "PhivecMW",
              "EtafuPhiuvecs", "Etafuvecs", "Phivecs", "PhivecMW", "PhivecsMW",
              "PSUTUsefulIEAWithDetails", "PSUTUsefulIEA", "YfuUEIOUfudetailsEnergy", "PSUTIEA",
              "PSUTMWEnergy", "BalancedPSUTMW",
              "PSUTMWAllYears", "PSUTMW", "PSUTIEAMW", "PSUT",
              "CmatsAgg", "EtafuYEIOU", "Etai",
              "EtafuYEIOUagg",
              "PSUTWithoutNEU",
              "PSUTReAll"),
    names_wrap = c("CompletedAllocationTables",
                   "AMWPFUDataRaw", "HMWPFUDataRaw", "HMWPFUData", "AMWPFUData",
                   "MachineData", "PhiConstants", "CompletedEfficiencyTables", "Phiuvecs",
                   "CompletedPhiuTables", "EtafuPhiuvecs", "Phipfvecs", "Phivecs",
                   "PSUTFinalIEA", "Cmats", "PSUTUsefulIEAWithDetails", "PSUTUsefulIEA",
                   "PSUTMWEnergy", "PhivecsMW", "PSUTMWAllYears",
                   "PSUTIEA", "PSUTMW", "PSUTIEAMW", "PSUT",
                   "Etafuvecs", "CmatsAgg")) |>


  ## An inner hook for targets where AllocAndEffCountries
  ## is the mapped variable
  tarchetypes::tar_hook_inner(
    hook = download_dependency_hook(.x,
                                    countries = AllocAndEffCountries,
                                    years = Years,
                                    index_map = IndexMap,
                                    rctypes = MatnameRCType,
                                    conn = conn,
                                    schema = DM,
                                    fk_parent_tables = FKTables),
    names = c("BalancedBeforeIEA", "BalancedIEAData", "BalancedAfterIEA",
              "SpecifiedIEAData", "PSUTFinalIEA", "IncompleteAllocationTables"),
    names_wrap = c("IEAData", "BalancedIEAData", "SpecifiedIEAData")) |>


  ## An inner hook to download only relevant countries and years
  ## of SpecifiedIEAData for the CompletedAllocationTables target
  tarchetypes::tar_hook_inner(
    hook = download_dependency_hook(.x,
                                    countries = Countries,
                                    years = Years,
                                    index_map = IndexMap,
                                    rctypes = MatnameRCType,
                                    conn = conn,
                                    schema = DM,
                                    fk_parent_tables = FKTables),
    names = c("CompletedAllocationTables"),
    names_wrap = c("SpecifiedIEAData")) |>


  ## An inner hook for downloading data
  ## for all countries but only specific years
  tarchetypes::tar_hook_inner(
    hook = download_dependency_hook(.x,
                                    countries = NULL, # Set NULL to download all data
                                    years = Years,
                                    index_map = IndexMap,
                                    rctypes = MatnameRCType,
                                    conn = conn,
                                    schema = DM,
                                    fk_parent_tables = FKTables),
    names = c("CompletedAllocationTables",
              "CompletedEfficiencyTables"),
    names_wrap = c("IncompleteAllocationTables",
                   "MachineData", "CompletedAllocationTables")) |>


  # tar_hook_outer -------------------------------------------------------------

  ## This hook uploads a resulting data frame to the database
  tarchetypes::tar_hook_outer(
    hook = {
      # It would be better to refer to db_table_name_from_hook_before
      # in this outer hook.
      # But that doesn't seem to work,
      # emitting an error from not finding
      # db_table_name_from_hook_before.
      db_table_name_from_hook_outer <- db_table_name_hook(targets::tar_name())
      upsert_hook(.x,
                  db_table_name = db_table_name_from_hook_outer,
                  dataset = dataset_from_hook,
                  index_map = IndexMap,
                  conn = conn,
                  schema = DM,
                  fk_parent_tables = FKTables,
                  dataset_colname = PFUPipelineTools::dataset_info$dataset_colname)
    },
    names = c("AllIEAData", "BalancedIEAData", "SpecifiedIEAData", "PSUTFinalIEA",
              "AMWPFUDataRaw", "AMWPFUData", "HMWPFUDataRaw", "HMWPFUData",
              "IncompleteAllocationTables", "CompletedAllocationTables", "Cmats",
              "AllMachineData", "CompletedEfficiencyTables",
              "PhiConstants", "CompletedPhiuTables", "Phipfvecs", "Phiuvecs",
              "EtafuPhiuvecs", "Etafuvecs", "Phivecs", "PhivecsMW",
              "PSUTUsefulIEAWithDetails", "PSUTUsefulIEA", "YfuUEIOUfudetailsEnergy", "PSUTIEA",
              "PSUTMWEnergy", "PSUTMWAllYears", "PSUTMW", "PSUTIEAMW", "PSUT",
              "CmatsAgg", "EtafuYEIOU", "Etai", "PSUTWithoutNEU"))



















