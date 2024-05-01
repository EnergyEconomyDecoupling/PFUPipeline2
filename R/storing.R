#' Perform a database release
#'
#' A release involves both writing csv files and moving data from a
#' temporary database to the "real" database.
#'
#' @param pipeline_releases_folder The folder into which pins will be written.
#' @param pin_targs A list of targets to release, typically each a data frame.
#' @param pin_names A string vector of pin names for each target to be released.
#' @param pin_types A string vector of target types, typically "csv" or "rds".
#' @param release A boolean of length 1 that tells whether to release
#'
#' @return A string indicating status of the release process.
#'
#' @export
perform_release <- function(pipeline_releases_folder,
                            pin_targs, pin_names, pin_types,
                            release = FALSE) {

  # Create a tibble from the pin arguments
  csv_info <- tibble::tibble(targ = pin_targs,
                             pin_name = pin_names,
                             pin_type = pin_types)
  PFUPipelineTools::release_many_pins(pipeline_releases_folder,
                                      release_info = csv_info,
                                      release = release)

  # Add code to move info from some tables to the "real" database.
  # First, clean out all rows from the destination table with this version
  # Write new data from the temporary database to the destination database.
}
