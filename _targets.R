# targets::tar_make() to run the pipeline
# targets::tar_make_future(workers = 8) to execute across multiple cores.
# targets::tar_read(<<target_name>>) to view the results.
# PFUPipelineTools::pl_destroy() to start over.
# targets::tar_make(callr_function = NULL) to debug.



# # User-adjustable parameters ---------------------------------------------------
#
# # countries <- c(PFUPipelineTools::canonical_countries, "WRLD") |> as.character()
# countries <- c("USA", "WRLD")
# # countries <- c('AGO', 'ALB', 'ARE', 'ARG', 'ARM', 'AUS', 'AUT', 'AZE', 'BEL', 'BEN',
# #                'BGD', 'BGR', 'BHR', 'BIH', 'BLR', 'BOL', 'BRA', 'BRN', 'BWA', 'CAN',
# #                'CHE', 'CHL', 'CHNM', 'CMR', 'COD', 'COG', 'COL', 'CIV', 'CRI', 'CUB',
# #                'CUW', 'CYP', 'CZE', 'DEU', 'DNK', 'DOM', 'DZA', 'ECU', 'EGY', 'ERI',
# #                'ESP', 'EST', 'ETH', 'FIN', 'FRA', 'GAB', 'GBR', 'GEO', 'GHA', 'GIB',
# #                'GNQ', 'GRC', 'GTM', 'GUY', 'HKG', 'HND', 'HRV', 'HTI', 'HUN', 'IDN',
# #                'IND', 'IRL', 'IRN', 'IRQ', 'ISL', 'ISR', 'ITA', 'JAM', 'JOR', 'JPN',
# #                'KAZ', 'KEN', 'KGZ', 'KHM', 'KOR', 'KWT', 'LAO', 'LBN', 'LBY', 'LKA',
# #                'LTU', 'LUX', 'LVA', 'MAR', 'MDA', 'MDG', 'MEX', 'MKD', 'MLT', 'MMR',
# #                'MNE', 'MNG', 'MOZ', 'MUS', 'MYS', 'NAM', 'NER', 'NGA', 'NIC', 'NLD',
# #                'NOR', 'NPL', 'NZL', 'OAFR', 'OAMR', 'OASI', 'OMN', 'PAK', 'PAN', 'PER',
# #                'PHL', 'POL', 'PRK', 'PRT', 'PRY', 'QAT', 'ROU', 'RUS', 'RWA', 'SAU',
# #                'SDN', 'SEN', 'SGP', 'SLV', 'SRB', 'SSD', 'SUN', 'SUR', 'SVK', 'SVN',
# #                'SWE', 'SWZ', 'SYR', 'TGO', 'THA', 'TJK', 'TKM', 'TTO', 'TUN', 'TUR',
# #                'TWN', 'TZA', 'UGA', 'UKR', 'URY', 'USA', 'UZB', 'VEN', 'VNM', 'WABK',
# #                'WMBK', 'XKX', 'YEM', 'YUG', 'ZAF', 'ZMB', 'ZWE', 'AFRI', 'ASIA', 'BUNK',
# #                'EURP', 'MIDE', 'NAMR', 'OCEN', 'SAMR', 'WRLD', 'FoSUN', 'FoYUG', 'FoCZK', 'UnDEU',
# #                'Africa', 'Asia_', 'Europe', 'MidEast', 'NoAmr', 'Oceania', 'SoCeAmr')
#
# # Set the years for IEA data analysis
# years <- 1960:2020
#
# # Set the years to provide exiobase coefficients
# years_exiobase <- 1995:2020
#
# # Tell whether to do chops
# do_chops <- FALSE
#
# # Should we specify non-energy flows?
# specify_non_energy_flows <- TRUE
#
# # Should we apply fixes to the IEA data?
# apply_fixes <- TRUE
#
#
# # Set versions
# iea_dataset <- "IEAEWEB2022"
# input_data_version <- "v2.0"
# output_version <- "v2.0a1"
#
# worker_threads <- 8 # For parallel processing
# # worker_threads <- 16 # For parallel processing
#
# conn_params <- list(dbname = output_version,
#                     user = "postgres",
#                     host = "eviz.cs.calvin.edu",
#                     port = 5432)


# Copy the file local_setup_template.R to local_setup.R.
# Edit user-adjustable parameters in the file local_setup.R


# Get local machine setup information ------------------------------------------
# Duplicate file _pl_setup_template.R and
# rename as _pl_setup.R.
# Modify details for your setup, as needed.
source("_pl_setup.R")


# Load packages required to define the pipeline:
library(PFUPipeline2)
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
