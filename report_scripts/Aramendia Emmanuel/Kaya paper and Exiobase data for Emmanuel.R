# These data are for Emmanuel Aramendia for the
# Kaya identity paper.

# Upload to the OneDrive folder at
# https://leeds365-my.sharepoint.com/personal/earear_leeds_ac_uk/_layouts/15/onedrive.aspx?e=5%3A290cf82d24194866a4c562d0235bbe18&sharingv2=true&fromShare=true&at=9&CID=e67e3dfb%2D8a88%2D49ba%2Dab8a%2Db49e8a5bf6f7&id=%2Fpersonal%2Fearear%5Fleeds%5Fac%5Fuk%2FDocuments%2FDatasets%2FCL%2DPFU%2FPFU%5FDatabase%5Fv2%5FKayaPaper&FolderCTID=0x012000292DEB6833DAEC48BD5C809FE95AA088&view=0

# Read and save Exiobase output files that remain local
targets::tar_read(ExiobaseEftoXfMultipliers) |>
  write.csv(file = "~/Desktop/For Emmanuel/Ef_to_Xf_multipliers.csv", row.names = FALSE)

targets::tar_read(ExiobaseEftoEuMultipliers) |>
  write.csv(file = "~/Desktop/For Emmanuel/Ef_to_Eu_multipliers.csv", row.names = FALSE)

targets::tar_read(ExiobaseXftoXuMultipliers) |>
  write.csv(file = "~/Desktop/For Emmanuel/Xf_to_Xu_multipliers.csv", row.names = FALSE)

targets::tar_read(ExiobaseEftoElossMultipliers) |>
  write.csv(file = "~/Desktop/For Emmanuel/Ef_to_Eloss_multipliers.csv", row.names = FALSE)

targets::tar_read(ExiobaseXftoXlossMultipliers) |>
  write.csv(file = "~/Desktop/For Emmanuel/Xf_to_Xloss_multipliers.csv", row.names = FALSE)


# Read data that are located in the Mexer database
conn <- PFUPipelineTools::get_scratchmdb_conn()
on.exit(DBI::dbDisconnect(conn))


v_string <- "v3.0a2"

Y_fu_U_EIOU_fu_details <- "YfuUEIOUfudetails" |>
  PFUPipelineTools::pl_filter_collect(version_string = v_string,
                                      conn = conn,
                                      collect = TRUE,
                                      matrix_class = "matrix")
saveRDS(Y_fu_U_EIOU_fu_details, "~/Desktop/For Emmanuel/YfuUEIOUfudetails.rds")



eta_i <- "Etai" |>
  PFUPipelineTools::pl_filter_collect(version_string = v_string,
                                      conn = conn,
                                      collect = TRUE,
                                      matrix_class = "matrix")
saveRDS(eta_i, "~/Desktop/For Emmanuel/Etai.rds")



phi_vecs <- "Phivecs" |>
  PFUPipelineTools::pl_filter_collect(version_string = v_string,
                                      conn = conn,
                                      collect = TRUE,
                                      matrix_class = "matrix")
saveRDS(phi_vecs, "~/Desktop/For Emmanuel/Phivecs.rds")



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
saveRDS(psut_Re_all, "~/Desktop/For Emmanuel/PSUTReAll.rds")

DBI::dbDisconnect(conn)
