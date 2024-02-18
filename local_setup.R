# This is a template file for defining local setup parameters for
# creating the CL-PFU database.
# Duplicate this file and rename to "local_setup.R"


# Countries --------------------------------------------------------------------

countries <- c(PFUPipelineTools::canonical_countries, "WRLD") |> as.character()


# Years ------------------------------------------------------------------------

# Set the years for IEA data analysis
years <- 1960:2020

# Set the years to provide exiobase coefficients
years_exiobase <- 1995:2020


# Other control parameters -----------------------------------------------------

# Tell whether to do chops
do_chops <- FALSE

# Should we specify non-energy flows?
specify_non_energy_flows <- TRUE

# Should we apply fixes to the IEA data?
apply_fixes <- TRUE

# Parallel processing
worker_threads <- 8


# Version information ----------------------------------------------------------
# Tells what version of the database you are creating.
# This should be a string of the form
# v2.0a1, v3.0, v4.5b2
# to indicate major version, minor version, and alpha/beta state.
clpfu_version <- "2.0a1"

# Directory for input and output -----------------------------------------------
clpfu_dir <- file.path("~", "OneDrive", "Fellowship 1960-2015 PFU database research")










# In a typical setup, everything below this point
# need not be adjusted.


# Calculate input data version from clpfu_version
# by trimming any alpha or beta information
input_data_version <- sub(pattern = "[ab]\\d*$", "", clpfu_version)


# Establish the database connection
# Never store passwords in this file.
# Rather, store passwords in the .pgpass file.
# See https://www.postgresql.org/docs/current/libpq-pgpass.html
# for details.
conn_params <- list(dbname = clpfu_version,
                    user = "postgres",
                    host = "eviz.cs.calvin.edu",
                    port = 5432)

