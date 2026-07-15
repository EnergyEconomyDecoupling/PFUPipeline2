# These data are for Emmanuel Aramendia for the
# Kaya identity paper.

# Upload to the OneDrive folder at
# https://leeds365-my.sharepoint.com/personal/earear_leeds_ac_uk/_layouts/15/onedrive.aspx?e=5%3A290cf82d24194866a4c562d0235bbe18&sharingv2=true&fromShare=true&at=9&CID=e67e3dfb%2D8a88%2D49ba%2Dab8a%2Db49e8a5bf6f7&id=%2Fpersonal%2Fearear%5Fleeds%5Fac%5Fuk%2FDocuments%2FDatasets%2FCL%2DPFU%2FPFU%5FDatabase%5Fv2%5FKayaPaper&FolderCTID=0x012000292DEB6833DAEC48BD5C809FE95AA088&view=0

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
saveRDS(psut_Re_all, "~/Desktop/For Emmanuel/PSUTReAll.rds")



