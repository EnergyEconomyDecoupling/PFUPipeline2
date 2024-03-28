#' Add efficiency (`eta`) and exergy-to-energy ratio (`phi`) vectors
#' to a data frame.
#'
#' This function adds final-to-useful efficiency (`eta`) and
#' exergy-to-energy ratio vectors to the previously-created `WithCmats` target.#'
#'
#' @param completed_efficiency_tables A hash of the completed efficiency tables
#'                                    from which efficiency (`eta_fu`) vectors
#'                                    should be created.
#'                                    This data frame is most likely to be the `CompletedEfficiencyTables` target.
#' @param completed_phi_tables A hash of the completed phi tables
#'                             from which exergy-to-energy ratio vectors (`phi_u`)
#'                             should be created.
#'                             This data frame is most likely to be the `CompletedPhiTables` target.
#' @param countries The countries for which `eta_fu` and `phi_u` vectors should be formed.
#' @param dataset The name of the dataset to which these data belong.
#' @param db_table_name The name of the specified IEA data table in `conn`.
#' @param conn The database connection.
#' @param schema The data model (`dm` object) for the database in `conn`.
#'               See details.
#' @param fk_parent_tables A named list of all parent tables
#'                         for the foreign keys in `db_table_name`.
#'                         See details.
#' @param matrix_class The type of matrix that should be produced.
#'                     One of "matrix" (the default and not sparse) or "Matrix" (which may be sparse).
#' @param country,year See `IEATools::ieacols`.
#' @param c_source,eta_fu_source,.values,eta_fu,phi_u See `IEATools::template_cols`.
#' @param phi_u_source See `IEATools::phi_constants_names`.
#' @param dataset_colname See `PFUPipelineTools::dataset_info`.
#'
#' @return A data frame with `eta_fu` and `phi_u` vectors added as columns.
#'
#' @export
calc_eta_fu_phi_u_vecs <- function(completed_efficiency_tables,
                                   completed_phi_tables,
                                   countries,
                                   dataset,
                                   db_table_name,
                                   conn,
                                   schema = PFUPipelineTools::schema_from_conn(conn),
                                   fk_parent_tables = PFUPipelineTools::get_all_fk_tables(conn = conn, schema = schema),
                                   matrix_class = c("matrix", "Matrix"),
                                   country = IEATools::iea_cols$country,
                                   year = IEATools::iea_cols$year,
                                   c_source = IEATools::template_cols$c_source,
                                   eta_fu_source = IEATools::template_cols$eta_fu_source,
                                   .values = IEATools::template_cols$.values,
                                   eta_fu = IEATools::template_cols$eta_fu,
                                   phi_u = IEATools::template_cols$phi_u,
                                   phi_u_source = IEATools::phi_constants_names$phi_source_colname,
                                   dataset_colname = PFUPipelineTools::dataset_info$dataset_colname) {
  matrix_class <- match.arg(matrix_class)

  # Filter the incoming hash tables by country and
  # collect from the database
  completed_efficiency_tables <- completed_efficiency_tables |>
    dplyr::filter(.data[[country]] %in% countries) |>
    PFUPipelineTools::pl_collect_from_hash(conn = conn,
                                           schema = schema,
                                           fk_parent_tables = fk_parent_tables)
  completed_phi_tables <- completed_phi_tables |>
    dplyr::filter(.data[[country]] %in% countries) |>
    PFUPipelineTools::pl_collect_from_hash(conn = conn,
                                           schema = schema,
                                           fk_parent_tables = fk_parent_tables)

  lapply(list(completed_efficiency_tables, completed_phi_tables), function(t) {
    t |>
      dplyr::filter(.data[[country]] %in% countries) |>
      dplyr::mutate(
        # Eliminate the c_source, eta_fu_source, and phi_u_source columns
        # (if they exists) before sending
        # the completed_allocation_tables into form_eta_fu_phi_u_vecs().
        # The c_source, eta_fu_source, and phi_u_source columns
        # apply to individual eta_fu and phi_u values, and
        # we're making vectors out of them.
        # In other words, form_eta_fu_phi_u_vecs() doesn't
        # know what to do with those columns.
        "{c_source}" := NULL,
        "{eta_fu_source}" := NULL,
        "{phi_u_source}" := NULL
      )
  }) |>
    dplyr::bind_rows() |>
    # Need to form eta_fu and phi_u vectors from completed_efficiency_tables.
    # Use the IEATools::form_eta_fu_phi_u_vecs() function for this task.
    # The function accepts a tidy data frame in addition to wide-by-year data frames.
    IEATools::form_eta_fu_phi_u_vecs(matvals = .values, matrix_class = matrix_class) |>
    # Add the dataset column
    dplyr::mutate(
      "{dataset_colname}" := dataset
    ) |>
    dplyr::relocate(dplyr::all_of(dataset_colname)) |>
    PFUPipelineTools::pl_upsert(in_place = TRUE,
                                db_table_name = db_table_name,
                                conn = conn,
                                schema = schema,
                                fk_parent_tables = fk_parent_tables)

}
