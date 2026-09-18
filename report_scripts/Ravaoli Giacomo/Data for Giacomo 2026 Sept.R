# Giacomo asked (in an email dated 21 August 2026)
# I just had a meeting with João (in CC) and I wanted to ask
# if you could send us the same data that you recently sent him
# from v3 but for useful energy/exergy, instead of final as you already sent him.
# I would need them for the work I'm doing with him,
# Tania and Tiago on bringing the Eurogreen model to the useful stage. Thanks a lot!



conn <- PFUPipelineTools::get_scratchmdb_conn()
on.exit(DBI::dbDisconnect(conn))

countries <- "ITA"

v_string <- "v3.0"

output_folder <- file.path("~",
                           "OneDrive - University of Leeds",
                           "Fellowship 1960-2015 PFU database research",
                           "Output Data",
                           v_string,
                           "For Giacomo")
if (!file.exists(output_folder)) {
  dir.create(output_folder)
}

psut_mats_downloaded <- PFUPipelineTools::pl_filter_collect(
  version_string = v_string,
  db_table_name = "PSUTReAllChopAllDsAllGrAll",
  Dataset == "CL-PFU IEA+MW",
  ProductAggregation == "Specified",
  IndustryAggregation == "Specified",
  Country %in% countries,
  LastStage == "Useful",
  IncludesNEU == TRUE,
  matname %in% c("U_EIOU", "Y"),
  create_matsindf = TRUE,
  collect = TRUE,
  conn = conn)

psut_mats_downloaded |>
  tidyr::pivot_longer(cols = c(U_EIOU, Y),
                      names_to = "matnames",
                      values_to = "matvals") |>
  matsindf::expand_to_tidy(drop = 0) |>
  write.csv(file = file.path(output_folder,
                             "Useful final energy and exergy for Giacomo.csv"),
            row.names = FALSE)



