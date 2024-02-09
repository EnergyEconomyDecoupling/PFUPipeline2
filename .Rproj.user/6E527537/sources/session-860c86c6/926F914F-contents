#!/usr/bin/env Rscript

conn <- DBI::dbConnect(RPostgres::Postgres(),
                       dbname = "v1.4a1",
                       user = "postgres",
                       host = "eviz.cs.calvin.edu",
                       port = 5432)

tar_destroy(ask = FALSE)
PFUPipelineTools::pl_destroy(conn, drop_tables = TRUE)



DBI::dbDisconnect(conn)
