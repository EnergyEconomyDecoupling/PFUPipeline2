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
#' @param industry_type,product_type,other_type See `IEATools::row_col_types`.
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
                                   matrix_class = "Matrix",
                                   country = IEATools::iea_cols$country,
                                   year = IEATools::iea_cols$year,
                                   c_source = IEATools::template_cols$c_source,
                                   eta_fu_source = IEATools::template_cols$eta_fu_source,
                                   .values = IEATools::template_cols$.values,
                                   eta_fu = IEATools::template_cols$eta_fu,
                                   phi_u = IEATools::template_cols$phi_u,
                                   phi_u_source = IEATools::phi_constants_names$phi_source_colname,
                                   industry_type = IEATools::row_col_types$industry,
                                   product_type = IEATools::row_col_types$product,
                                   other_type = IEATools::row_col_types$other,
                                   dataset_colname = PFUPipelineTools::dataset_info$dataset_colname) {

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
    dplyr::mutate(
      # Rowtype of etafu is Industry -> Product; coltype is etafu.
      # Coltype of phiu is phiu.
      # That's accurate, but it will not pick up the Industry and Product types
      # stored in the database.
      # For example,
      # Electric lamps -> L (a row name) in etafu is stored in the Industry table of the database.
      # Electricity -> Residential (a row name) in phiu is stored in the Product table of the database.
      # Change rowtype to Industry for etafu.
      # Coltype for both should be "Other", because these are column vectors.
      # Doing so enables indexing in the database.
      "{eta_fu}" := .data[[eta_fu]] |> matsbyname::setrowtype(industry_type) |> matsbyname::setcoltype(other_type),
      "{phi_u}" := .data[[phi_u]] |> matsbyname::setcoltype(other_type),
      # Add the dataset column
      "{dataset_colname}" := dataset
    ) |>
    dplyr::relocate(dplyr::all_of(dataset_colname)) |>
    PFUPipelineTools::pl_upsert(in_place = TRUE,
                                db_table_name = db_table_name,
                                # Don't keep single unique columns,
                                # because groups may have different columns
                                # with single unique values.
                                keep_single_unique_cols = FALSE,
                                conn = conn,
                                schema = schema,
                                fk_parent_tables = fk_parent_tables)
}


#' Choose etafu or phiu columns from a data frame of etafu and phiu vectors
#'
#' @param eta_fu_phi_u_vecs A hashed data frame containing metadata columns and columns for
#'                          etafu (final to useful efficiency) and
#'                          phiu (exergy-to-energy efficiency ratios).
#' @param keep Tells which column to keep, etafu or phiu.
#'             Must be one of `IEATools::template_cols$eta_fu` or `IEATools::template_cols$phi_u`.
#' @param countries The countries on which this operation should be accomplished.
#' @param dataset The name of the dataset to which these data belong.
#' @param db_table_name The name of the specified IEA data table in `conn`.
#' @param conn The database connection.
#' @param schema The data model (`dm` object) for the database in `conn`.
#'               See details.
#' @param fk_parent_tables A named list of all parent tables
#'                         for the foreign keys in `db_table_name`.
#'                         See details.
#' @param country The name of the Country column.
#'                Default is `IEATools::iea_cols$country`.
#' @param eta_fu_colname,phi_u_colname See `IEATools::template_cols`.
#' @param dataset_colname See `PFUPipelineTools::dataset_info`.
#'
#' @return A data frame of metadata and either an eta_fu column or a phi_u column
#'
#' @export
sep_eta_fu_phi_u <- function(eta_fu_phi_u_vecs,
                             keep = c(IEATools::template_cols$eta_fu,
                                      IEATools::template_cols$phi_u),
                             countries,
                             dataset,
                             db_table_name,
                             conn,
                             schema = PFUPipelineTools::schema_from_conn(conn),
                             fk_parent_tables = PFUPipelineTools::get_all_fk_tables(conn = conn, schema = schema),
                             country = IEATools::iea_cols$country,
                             eta_fu_colname = IEATools::template_cols$eta_fu,
                             phi_u_colname = IEATools::template_cols$phi_u,
                             dataset_colname = PFUPipelineTools::dataset_info$dataset_colname) {

  keep <- match.arg(keep, several.ok = FALSE)
  out <- eta_fu_phi_u_vecs |>
    dplyr::filter(.data[[country]] %in% countries) |>
    PFUPipelineTools::pl_collect_from_hash(set_tar_group = FALSE,
                                           conn = conn,
                                           schema = schema,
                                           fk_parent_tables = fk_parent_tables)
  if (keep == IEATools::template_cols$eta_fu) {
    col_to_delete <- phi_u_colname
  } else {
    col_to_delete <- eta_fu_colname
  }
  out |>
    dplyr::mutate(
      "{col_to_delete}" := NULL,
      "{dataset_colname}" := dataset
    ) |>
    dplyr::relocate(dplyr::all_of(dataset_colname)) |>
    PFUPipelineTools::pl_upsert(in_place = TRUE,
                                db_table_name = db_table_name,
                                # Don't keep single unique columns,
                                # because groups may have different columns
                                # with single unique values.
                                keep_single_unique_cols = FALSE,
                                conn = conn,
                                schema = schema,
                                fk_parent_tables = fk_parent_tables)

}
