# Run the pipeline: targets::tar_make()
# To run in parallel: targets::tar_make_future(workers = 4)
# To debug: targets::tar_make(callr_function = NULL) and insert browser() calls
# To view results: targets::tar_read(<<target_name>>)
# To completely start over: Source `start_over.R` and call `start_over(drop_tables = TRUE)`


# Get local machine setup information ------------------------------------------
# Duplicate file _pl_setup_template.R and
# rename as _pl_setup.R.
# Modify details for your setup, as needed.
source("_pl_setup.R")


# Load packages required to define the pipeline:
library(tarchetypes)
library(targets)


# Sort out a few issues with countries -----------------------------------------

# Additional exemplar countries are countries which aren't included in the workflow
# as individual countries, but from which allocation or efficiency data may be
# obtained and assigned to countries in the workflow using the exemplar system.
additional_exemplar_countries <- c("AFRI", # Africa
                                   "ASIA", # Asia
                                   "EURP", # Europe
                                   "MIDE", # Middle East
                                   "NAMR", # North America
                                   "OCEN", # Oceania
                                   "SAMR", # South America
                                   "BUNK") # Bunkers

# WRLD should not be in both countries and additional_exemplar_countries
if (("WRLD" %in% countries) & ("WRLD" %in% additional_exemplar_countries)) {
  # Remove WRLD from additional_exemplar_countries
  additional_exemplar_countries <- additional_exemplar_countries[!(additional_exemplar_countries == "WRLD")]
}

# WRLD should always be in countries or in additional_exemplar_countries.
if (!("WRLD" %in% countries) & !("WRLD" %in% additional_exemplar_countries)) {
  # Add WRLD to additional_exemplar_countries
  additional_exemplar_countries <- c("WRLD", additional_exemplar_countries)
}


# Set target options -----------------------------------------------------------

tar_option_set(
  # packages that your targets need to run
  packages = c("FAOSTAT",
               "IEATools",
               "Matrix",
               "MWTools",
               "PFUPipelineTools",
               "qs",
               "Rilostat",
               "tibble"),
  # Optionally set the default storage format. qs is fast.
  format = "qs",

  # Unload targets from memory when completed
  memory = "persistent",
  # Worker threads are responsible for saving and loading targets
  storage = "worker", retrieval = "worker",

  # For distributed computing in tar_make(), supply a {crew} controller
  # as discussed at https://books.ropensci.org/targets/crew.html.
  # Choose a controller that suits your needs. For example, the following
  # sets a controller with 2 workers which will run as local R processes:

  # controller = crew::crew_controller_local(workers = worker_threads)
  controller = crew_controller

  # Alternatively, if you want workers to run on a high-performance computing
  # cluster, select a controller from the {crew.cluster} package. The following
  # example is a controller for Sun Grid Engine (SGE).

  #   controller = crew.cluster::crew_controller_sge(
  #     workers = 50,
  #     # Many clusters install R as an environment module, and you can load it
  #     # with the script_lines argument. To select a specific verison of R,
  #     # you may need to include a version string, e.g. "module load R/4.3.0".
  #     # Check with your system administrator if you are unsure.
  #     script_lines = "module load R"
  #   )

  # Set other options as needed.
)


# Source scripts ---------------------------------------------------------------
tar_source()


# Source the pipeline ----------------------------------------------------------
source("pipeline.R")
