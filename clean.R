#!/usr/bin/env Rscript

# This script cleans everything to reset everything.
# Be sure to set the database name correctly.

conn <- DBI::dbConnect(RPostgres::Postgres(),
                       dbname = "v1.4a1",
                       user = "mkh2",
                       host = "eviz.cs.calvin.edu",
                       port = 5432)
on.exit(DBI::dbDisconnect(conn))

PFUPipelineTools::pl_destroy(conn, destroy_cache = TRUE, drop_tables = TRUE)
