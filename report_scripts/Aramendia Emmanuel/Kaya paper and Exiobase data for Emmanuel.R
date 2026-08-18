# These data are for Emmanuel Aramendia for the
# Kaya identity paper.

v_string <- "v3.0"
output_folder <- file.path("~",
                           "OneDrive - University of Leeds",
                           "Fellowship 1960-2015 PFU database research",
                           "Output Data",
                           v_string,
                           "For Emmanuel")
if (!file.exists(output_folder)) {
  dir.create(output_folder)
}

# Read and save Exiobase output files that remain local
targets::tar_read(ExiobaseEftoXfMultipliers) |>
  write.csv(file = file.path(output_folder,
                             "Ef_to_Xf_multipliers.csv"),
            row.names = FALSE)

targets::tar_read(ExiobaseEftoEuMultipliers) |>
  write.csv(file = file.path(output_folder,
                             "Ef_to_Eu_multipliers.csv"),
            row.names = FALSE)

targets::tar_read(ExiobaseXftoXuMultipliers) |>
  write.csv(file = file.path(output_folder,
                             "Xf_to_Xu_multipliers.csv"),
            row.names = FALSE)

targets::tar_read(ExiobaseEftoElossMultipliers) |>
  write.csv(file = file.path(output_folder,
                             "Ef_to_Eloss_multipliers.csv"),
            row.names = FALSE)

targets::tar_read(ExiobaseXftoXlossMultipliers) |>
  write.csv(file = file.path(output_folder,
                             "Xf_to_Xloss_multipliers.csv"),
            row.names = FALSE)


# Read data that are located in the Mexer database
conn <- PFUPipelineTools::get_scratchmdb_conn()
on.exit(DBI::dbDisconnect(conn))


v_string <- "v3.0"

Y_fu_U_EIOU_fu_details <- "YfuUEIOUfudetails" |>
  PFUPipelineTools::pl_filter_collect(version_string = v_string,
                                      conn = conn,
                                      collect = TRUE,
                                      matrix_class = "matrix")
saveRDS(Y_fu_U_EIOU_fu_details,
        file.path(output_folder,
                  "YfuUEIOUfudetails.rds"))



eta_i <- "Etai" |>
  PFUPipelineTools::pl_filter_collect(version_string = v_string,
                                      conn = conn,
                                      collect = TRUE,
                                      matrix_class = "matrix")
saveRDS(eta_i,
        file.path(output_folder,
                  "Etai.rds"))



phi_vecs <- "Phivecs" |>
  PFUPipelineTools::pl_filter_collect(version_string = v_string,
                                      conn = conn,
                                      collect = TRUE,
                                      matrix_class = "matrix")
saveRDS(phi_vecs,
        file.path(output_folder,
                  "Phivecs.rds"))



agg_eta_pfu <- "AggEtaPFU" |>
  PFUPipelineTools::pl_filter_collect(version_string = v_string,
                                      conn = conn,
                                      collect = TRUE,
                                      matrix_class = "matrix")
saveRDS(agg_eta_pfu,
        file.path(output_folder,
                  "AggEtaPFU.rds"))



# Dan Chester needs this table.
sector_agg_eta_fu <- "SectorAggEtaFU" |>
  PFUPipelineTools::pl_filter_collect(version_string = v_string,
                                      conn = conn,
                                      collect = TRUE,
                                      matrix_class = "matrix")
saveRDS(sector_agg_eta_fu,
        file.path(output_folder,
                  "SectorAggEtaFU.rds"))



psut_Re_all <- "PSUTReAll" |>
  PFUPipelineTools::pl_filter_collect(version_string = v_string,
                                      conn = conn,
                                      collect = TRUE,
                                      matrix_class = "matrix")

# When saving this object to disk on the next line,
# I receive this error:
#
# Error: vector memory limit of 32.0 Gb reached, see mem.maxVSize()
#
# To get around the error, I needed to increase the maximum vector
# heap size on macOS using

mem.maxVSize(32768*2)

# After that adjustment, saving to disk with the following line
# was successful.
saveRDS(psut_Re_all,
        file.path(output_folder,
                  "PSUTReAll.rds"))

# Set back to original size
mem.maxVSize(32768)

DBI::dbDisconnect(conn)
