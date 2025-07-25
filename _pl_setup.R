#
# DO NOT EDIT THIS FILE
#
# This is a template file for defining local setup parameters for
# creating the CL-PFU database.
# Duplicate this file and rename to "local_setup.R"

# For debugging: tar_make(callr_function = NULL, use_crew = FALSE, as_job = FALSE)
# and
# (1) insert browser() calls for functions in PFUPipeline2
# (2) set breakpoints in functions from other packages.

# Countries --------------------------------------------------------------------

countries <- c(PFUPipelineTools::canonical_countries, wrld = "WRLD") |> as.character()
# countries <- "AGO"
# countries <- "BEN" # First country with no EIOU
# countries <- "CMR"
# countries <- "GHA"
# countries <- "USA"
# countries <- "ZAF"
# countries <- "WMBK"
# countries <- "WABK"
# countries <- "WRLD"
# countries <- c("GHA", "ZAF")
# countries <- c("USA", "WRLD")
# countries <- c("WMBK", "USA")
# countries <- c("AGO", "BEN", "WMBK")
# countries <- c("AGO", "BEN", "WMBK", "WABK", "WRLD", "GHA", "ZAF")


# Years ------------------------------------------------------------------------

years <- 1960:2020
# years <- 1960
# years <- 1971
# years <- 1996
# years <- 2010
# years <- 1960:1980
# years <- 1995:2020
# years <- 1995:1996
# years <- 1971:1972
# years <- 1971:1990
# years <- 1995:1996

# Set the years to provide exiobase coefficients
years_exiobase <- 1995:2020

# Set the vintage for the IEA EWEB data
iea_year <- 2022

# Other control parameters -----------------------------------------------------

# Tell whether to do chops
do_chops <- FALSE

# Should we specify non-energy flows?
specify_non_energy_flows <- TRUE

# Should we apply fixes to the IEA data?
apply_fixes <- TRUE

# Should we do a release?
release <- FALSE

# Should we compress data across versions upon upsert to a table?
compress_data <- FALSE

# Reset schema?
# Think VERY CAREFULLY before setting this TRUE!
reset_schema <- FALSE
# Set back to FALSE quickly, i.e. immediately after tar_make()!
# Likely only need to set TRUE after start_over().

# Dataset and version information ----------------------------------------------

# Tells what database you are targeting
dbname <- "ScratchMDB"
# dbname <- "SandboxDB"

# Tells what CL-PFU dataset you are creating
clpfu_dataset <- "CL-PFU"
clpfu_iea_dataset <- "CL-PFU IEA"
clpfu_mw_dataset <- "CL-PFU MW"
clpfu_both_dataset <- "CL-PFU IEA+MW"
clpfu_version <- "v2.1a2"

# Tells what IEAEWEB dataset you are using
iea_dataset <- "IEA EWEB"
iea_version <- "2022"

# Tells what FAO dataset you are using
fao_dataset <- "FAOSTAT"

# Tells what ILO dataset you are using
ilo_dataset <- "ILOSTAT"

# Parallel processing ----------------------------------------------------------

# Set worker_threads equal to or slightly less than
# the number of high-performance cores on your machine.
if (parallel::detectCores() == 10) {
  # M1 Pro
  worker_threads <- 8
} else if (parallel::detectCores() == 24) {
  # M2 Ultra
  worker_threads <- 16
} else {
  # To be safe
  worker_threads <- 2
}

# For parallel processing.
crew_controller <- crew::crew_controller_local(
  workers = worker_threads,
  seconds_idle = 60,
  r_arguments = "--max-connections=512")

# Directory for input and output data ------------------------------------------
project_path <- file.path("~",
                          "OneDrive",
                          "OneDrive - University of Leeds",
                          "Fellowship 1960-2015 PFU database research")

# In a typical setup, everything below this point
# need not be adjusted.
# However, users can override anything below
# for custon setups.

# Establish database connection parameters.
# Never store passwords in this file.
# Rather, store passwords in the .pgpass file.
# See https://www.postgresql.org/docs/current/libpq-pgpass.html
# for details.

# For regular connections
# conn_params <- list(dbname = version,
#                     user = "mkh2",
#                     host = "eviz.cs.calvin.edu",
#                     port = 5432)

# Use pgbouncer
conn_params <- list(dbname = dbname,
                    user = "dbcreator",
                    host = "mexer.site",
                    port = 6432)

# Calculate input data version from version
# by trimming any alpha or beta information from the end of the version string ...
# input_data_version <- sub(pattern = "[ab]\\d*$", "", clpfu_version)
# New policy: Input data folder name must match the version we are creating.
# This policy will enable reverting back to creating earlier versions.
input_data_version <- clpfu_version
# ... and by adding a leading "v" if it is missing.
if (!startsWith(input_data_version, "v")) {
  input_data_version <- paste0("v", input_data_version)
}

# Create a list of important paths for the CL-PFU pipeline
input_data_path <- file.path(project_path, "InputData", input_data_version)
output_data_path <- file.path(project_path, "OutputData")

clpfu_setup_paths <- list(
  project_path = project_path,
  input_data_path = input_data_path,
  output_data_path = output_data_path,
  iea_data_folder = file.path(project_path,
                              "IEA extended energy balance data",
                              paste("IEA", iea_year, "energy balance data"),
                              paste("IEA Extended Energy Balances", iea_year, "(TJ)")),
  base_iea_country_filename = "IEA Extended Energy Balances 2022 (TJ)",
  schema_path = file.path(input_data_path, "SchemaAndFKTables.xlsx"),
  fao_data_path = file.path(input_data_path, "External data sources", "FAO", "FAOSTAT_data.csv"),
  ilo_employment_data_path = file.path(input_data_path, "External data sources", "ILO", "EMP_TEMP_SEX_ECO_NB_A.csv"),
  ilo_working_hours_data_path = file.path(input_data_path, "External data sources", "ILO", "HOW_TEMP_SEX_ECO_NB_A.csv"),
  hmw_analysis_data_path = file.path(input_data_path, "hmw_analysis_data.xlsx"),
  amw_analysis_data_path = file.path(input_data_path, "amw_analysis_data.xlsx"),
  mw_concordance_path = file.path(input_data_path, "FAO_ISO_MW_Mapping.xlsx"),
  country_concordance_path = file.path(input_data_path, "Country_Concordance_Full.xlsx"),
  aggregation_mapping_path = file.path(input_data_path, "aggregation_mapping.xlsx"),
  phi_constants_path = file.path(input_data_path, "phi_constants.xlsx"),
  exemplar_table_path = file.path(input_data_path, "Exemplar_Table.xlsx"),
  fu_allocation_folder = file.path(input_data_path, "FU allocation data"),
  machine_data_folder = file.path(input_data_path, "Machines - Data"),
  ceda_data_folder = file.path(input_data_path, "CEDA Data"),
  exiobase_energy_flows_path = file.path(input_data_path, "exiobase_energy_flows_concordance.xlsx"),
  aggregation_maps_path = file.path(input_data_path, "aggregation_mapping.xlsx"),
  reports_source_folders = file.path(project_path, "reports"),
  # Output paths
  reports_dest_folder = file.path(output_data_path, "Reports"),
  pipeline_releases_folder = file.path(output_data_path, "PipelineReleases"),
  versions_and_products_path = file.path(output_data_path, "PipelineReleases", "versions and products.xlsx")
)
