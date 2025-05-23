# These data are for Emmanuel Aramendia for the
# Kaya identity paper.

# Upload to the OneDrive folder at
# https://leeds365-my.sharepoint.com/personal/earear_leeds_ac_uk/_layouts/15/onedrive.aspx?e=5%3A895fe2332f3b4a16a6a84e4afcf07d58&sharingv2=true&fromShare=true&at=9&CID=09737963%2D17b4%2D46fb%2Da5c3%2De68f9a22b521&FolderCTID=0x012000292DEB6833DAEC48BD5C809FE95AA088&id=%2Fpersonal%2Fearear%5Fleeds%5Fac%5Fuk%2FDocuments%2FPFU%5FDatabase%5Fv2%5FKayaPaper

conn_params <- list(dbname = "ScratchMDB",
                    user = "dbcreator",
                    host = "mexer.site",
                    port = 6432)
conn <- DBI::dbConnect(drv = RPostgres::Postgres(),
                       dbname = conn_params$dbname,
                       host = conn_params$host,
                       port = conn_params$port,
                       user = conn_params$user)
on.exit(DBI::dbDisconnect(conn))



Y_fu_U_EIOU_fu_details <- PFUPipelineTools::pl_filter_collect(db_table_name = "YfuUEIOUfudetails",
                                                              conn = conn,
                                                              collect = TRUE,
                                                              matrix_class = "matrix")
saveRDS(Y_fu_U_EIOU_fu_details, "~/Desktop/For Emmanuel/YfuUEIOUfudetails.rds")


eta_i <- PFUPipelineTools::pl_filter_collect("Etai",
                                             conn = conn,
                                             collect = TRUE,
                                             matrix_class = "matrix")

saveRDS(eta_i, "~/Desktop/For Emmanuel/Etai.rds")



phi_vecs <- PFUPipelineTools::pl_filter_collect("Phivecs",
                                                conn = conn,
                                                collect = TRUE,
                                                matrix_class = "matrix")
saveRDS(phi_vecs, "~/Desktop/For Emmanuel/Phivecs.rds")




psut_Re_all <- PFUPipelineTools::pl_filter_collect("PSUTReAll",
                                                   conn = conn,
                                                   collect = TRUE,
                                                   matrix_class = "matrix")
saveRDS(psut_Re_all, "~/Desktop/For Emmanuel/PSUTReAll.rds")



