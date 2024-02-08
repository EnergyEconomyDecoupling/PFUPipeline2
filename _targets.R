# targets::tar_make() to run the pipeline
# targets::tar_make_future(workers = 8) to execute across multiple cores.
# targets::tar_read(<<target_name>>) to view the results.
# PFUPipelineTools::pl_destroy() to start over.
# targets::tar_make(callr_function = NULL) to debug.


##############################
# User-adjustable parameters #
##############################

countries <- c(PFUPipelineTools::canonical_countries, "WRLD") |> as.character()
years <- 1960:2020
do_chops <- FALSE

input_data_version <- "v1.4"
output_version <- "v1.4a1"

worker_threads <- 16 # For parallel processing

conn_params <- list(dbname = output_version,
                    user = "postgres",
                    host = "eviz.cs.calvin.edu",
                    port = 5432)


##################################
# End user-adjustable parameters #
##################################

# Load packages required to define the pipeline:
library(PFUPipeline2)
library(tarchetypes)
library(targets)

# Set database connection parameters

# Get file locations
setup <- PFUSetup::get_abs_paths(version = input_data_version)

# Set target options:
tar_option_set(
  # packages that your targets need to run
  packages = c("Matrix",
               "PFUPipelineTools",
               "qs",
               "tibble"),
  # Optionally set the default storage format. qs is fast.
  format = "qs",

  # For distributed computing in tar_make(), supply a {crew} controller
  # as discussed at https://books.ropensci.org/targets/crew.html.
  # Choose a controller that suits your needs. For example, the following
  # sets a controller with 2 workers which will run as local R processes:

  controller = crew::crew_controller_local(workers = worker_threads)

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

# Get the target list
PFUPipeline2::get_pipeline(input_data_version = input_data_version,
                           output_version = output_version,
                           conn_params = conn_params,
                           countries = countries,
                           years = years,
                           do_chops = do_chops,
                           schema_file_path = setup[["schema_path"]])


