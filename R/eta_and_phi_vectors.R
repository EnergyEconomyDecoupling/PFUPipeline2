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
#' @param matrix_class The type of matrix that should be produced.
#'                     One of "matrix" (the default and not sparse) or "Matrix" (which may be sparse).
#' @param country,year See `IEATools::ieacols`.
#' @param c_source,eta_fu_source,.values,eta_fu,phi_u See `IEATools::template_cols`.
#' @param phi_u_source See `IEATools::phi_constants_names`.
#' @param industry_type,product_type,other_type See `IEATools::row_col_types`.
#'
#' @return A data frame with `eta_fu` and `phi_u` vectors added as columns.
#'
#' @export
calc_eta_fu_phi_u_vecs <- function(completed_efficiency_tables,
                                   completed_phi_tables,
                                   countries,
                                   matrix_class = "Matrix",
                                   country = IEATools::iea_cols$country,
                                   year = IEATools::iea_cols$year,
                                   c_source = IEATools::template_cols$c_source,
                                   eta_fu_source = IEATools::template_cols$eta_fu_source,
                                   .values = IEATools::template_cols$.values,
                                   eta_fu = IEATools::template_cols$eta_fu,
                                   phi_u = IEATools::template_cols$phi_u,
                                   phi_u_source = IEATools::phi_constants_names$phi_source_colname  # ,
                                   # industry_type = IEATools::row_col_types$industry,
                                   # product_type = IEATools::row_col_types$product,
                                   # other_type = IEATools::row_col_types$other
                                   ) {

  browser()

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
    IEATools::form_eta_fu_phi_u_vecs(matvals = .values, matrix_class = matrix_class) # |>
    # dplyr::mutate(
    #   # Rowtype of etafu is Industry -> Product; coltype is etafu.
    #   # Coltype of phiu is phiu.
    #   # That's accurate, but it will not pick up the Industry and Product types
    #   # stored in the database.
    #   # For example,
    #   # Electric lamps -> L (a row name) in etafu is stored in the Industry table of the database.
    #   # Electricity -> Residential (a row name) in phiu is stored in the Product table of the database.
    #   # Change rowtype to Industry for etafu.
    #   # Coltype for both should be "Other", because these are column vectors.
    #   # Doing so enables indexing in the database.
    #   "{eta_fu}" := .data[[eta_fu]] |> matsbyname::setrowtype(industry_type) |> matsbyname::setcoltype(other_type),
    #   "{phi_u}" := .data[[phi_u]] |> matsbyname::setcoltype(other_type)
    # )
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
#' @param index_map A list of data frames to assist with decoding matrices.
#'                  Passed to [decode_matsindf()] when `decode_matsindf` is `TRUE`
#'                  but otherwise not needed.
#' @param rctypes A data frame of row and column types.
#'                Passed to [decode_matsindf()] when `decode_matsindf` is `TRUE`
#'                but otherwise not needed.
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
                             country = IEATools::iea_cols$country,
                             eta_fu_colname = IEATools::template_cols$eta_fu,
                             phi_u_colname = IEATools::template_cols$phi_u) {

  keep <- match.arg(keep, several.ok = FALSE)
  if (keep == IEATools::template_cols$eta_fu) {
    col_to_delete <- phi_u_colname
  } else {
    col_to_delete <- eta_fu_colname
  }
  eta_fu_phi_u_vecs |>
    dplyr::mutate(
      "{col_to_delete}" := NULL
    )
}


#' Create a data frame of phi_pf vectors
#'
#' This function creates a data frame that contains all the metadata columns
#' from `phi_u_vecs` and a column of phi_pf vectors.
#' This work is accomplished by creating a vector from `phi_constants`,
#' adding one instance of the vector to the right side of the `phi_constants` data frame
#' for each row of the data frame,
#' and deleting `phi_u_colname` from the data frame.
#'
#' @param phi_constants A hashed data frame of constant phi values (for primary, final, and useful stages)
#'                      with columns `product`, `phi_colname`, and `is_useful_colname`.
#' @param phi_u_vecs A hashed data frame containing metadata columns and a column of phi_u vectors.
#'                   A column of phi_pf vectors replaces the column of phi_u vectors on output.
#' @param countries The countries for which you want to perform this task.
#' @param matrix_class A string that tells which type of matrix to create,
#'                     a "matrix" (the built-in type) or a "Matrix" (could be sparse).
#'                     Default is "matrix".
#' @param country,product See `IEATools::iea_cols`.`
#' @param product_type,other_type See `IEATools::row_col_types`.
#' @param eta_fu,phi_u,phi_pf_colname See `IEATools::template_cols`.
#' @param phi_colname,is_useful_colname See `IEATools::phi_constants_colnames`.
#'
#' @return A version of the `phi_constants` data frame
#'         with the column of useful phi (useful exergy-to-energy ratio) vectors
#'         replaced by a column of primary and final phi vectors.
#'
#' @export
calc_phi_pf_vecs <- function(phi_constants,
                             phi_u_vecs,
                             countries,
                             matrix_class = "Matrix",
                             country = IEATools::iea_cols$country,
                             product = IEATools::iea_cols$product,
                             product_type = IEATools::row_col_types$product,
                             # other_type = IEATools::row_col_types$other,
                             eta_fu = IEATools::template_cols$eta_fu,
                             phi_u = IEATools::template_cols$phi_u,
                             phi_pf_colname = IEATools::template_cols$phi_pf,
                             phi_colname = IEATools::phi_constants_names$phi_colname,
                             is_useful_colname = IEATools::phi_constants_names$is_useful_colname) {

  # Pick up non-useful (i.e., primary and final)
  # phi values.
  phi_pf_constants <- phi_constants |>
    dplyr::filter(! .data[[is_useful_colname]])
  # Create a vector from phi_pf_constants
  if (matrix_class == "matrix") {
    phi_pf_vec <- matrix(phi_pf_constants[[phi_colname]], nrow = nrow(phi_pf_constants), ncol = 1,
                         dimnames = list(c(phi_pf_constants[[product]]), phi_colname))
  } else {
    phi_pf_vec <- matsbyname::Matrix(phi_pf_constants[[phi_colname]], nrow = nrow(phi_pf_constants), ncol = 1,
                                     dimnames = list(c(phi_pf_constants[[product]]), phi_colname))
  }
  phi_pf_vec <- phi_pf_vec |>
    # matsbyname::setrowtype(product_type) |>
    # matsbyname::setcoltype(other_type)
    matsbyname::setrowtype(product_type) |>
    matsbyname::setcoltype(phi_colname)

  trimmed_phi_u_vecs <- phi_u_vecs |>
    dplyr::mutate(
      # We don't need the eta_fu or phi_u column on output.
      "{eta_fu}" := NULL,
      "{phi_u}" := NULL
    )
  nrows_trimmed_phi_u_vecs <- nrow(trimmed_phi_u_vecs)

  if (nrows_trimmed_phi_u_vecs == 0) {
    # If we have no rows, add the column that would have been created, and
    # return the zero-row data frame.
    out <- trimmed_phi_u_vecs |>
      dplyr::mutate(
        "{phi_pf_colname}" := list()
      )
  } else {
    out <- trimmed_phi_u_vecs |>
      dplyr::mutate(
        # Add a column of phi_pf vectors
        "{phi_pf_colname}" := RCLabels::make_list(phi_pf_vec,
                                                  n = nrows_trimmed_phi_u_vecs,
                                                  lenx = 1)
      )
  }
  return(out)
}


#' Sums phi_pf and phi_u vectors
#'
#' This function verifies that there are no rows in common between the
#' two input vectors.
#'
#' @param phi_pf_vecs A data frame of phi_pf vectors.
#' @param phi_u_vecs A data frame of phi_u vectors.
#' @param countries The countries for which you want to perform this task.
#' @param matrix_class A string that tells which type of matrix to create,
#'                     a "matrix" (the built-in type) or a "Matrix" (could be sparse).
#'                     Default is "matrix".
#' @param country,last_stage,energy_type,method See `IEATools::iea_cols`.
#' @param phi_pf_colname,phi_u_colname See `IEATools::template_cols`.
#' @param phi_colname See `IEATools::phi_constants_names`.
#' @param .nrow_diffs,.phi_shape_OK,.phi_names_OK,.phi_cols_OK,.phi_sum_OK,.phi_pf_colnames,.phi_u_colnames Names of temporary error-checking columns created internally.
#'
#' @return A data frame of summed phi_pf and phi_u vectors.
#'
#' @export
#'
#' @examples
#' phi_pf_vec <- matrix(c(1.1,
#'                        1.05), nrow = 2, ncol = 1,
#'                      dimnames = list(c("Coal", "Oil"), "phi"))
#' # Make a data frame of phi_pf vectors
#' phi_pf <- tibble::tibble(phi.pf = RCLabels::make_list(phi_pf_vec, n = 2, lenx = 1),
#'                          Country = "GHA",
#'                          Year = c(1971, 2000))
#' phi_u_vec <- matrix(c(0.8,
#'                       0.9,
#'                       0.7), nrow = 3, ncol = 1,
#'                     dimnames = list(c("Light", "MD", "Propulsion"), "phi"))
#' phi_u <- tibble::tibble(phi.u = RCLabels::make_list(phi_u_vec, n = 2, lenx = 1),
#'                         Country = "GHA",
#'                         Year = c(1971, 2000))
#' sum_phi_vecs(phi_pf, phi_u, countries = "GHA")
sum_phi_vecs <- function(phi_pf_vecs,
                         phi_u_vecs,
                         countries,
                         matrix_class = "Matrix",
                         country = IEATools::iea_cols$country,
                         last_stage = IEATools::iea_cols$last_stage,
                         energy_type = IEATools::iea_cols$energy_type,
                         method = IEATools::iea_cols$method,
                         phi_pf_colname = IEATools::template_cols$phi_pf,
                         phi_u_colname = IEATools::template_cols$phi_u,
                         phi_colname = IEATools::phi_constants_names$phi_colname,
                         .nrow_diffs = ".nrow_diffs",
                         .phi_shape_OK = ".phi_shape_OK",
                         .phi_names_OK = ".phi_names_OK",
                         .phi_cols_OK = ".phi_cols_OK",
                         .phi_sum_OK = ".phi_sum_OK",
                         .phi_pf_colnames = ".phi_pf_colnames",
                         .phi_u_colnames = ".phi_u_colnames") {

  temp <- dplyr::full_join(phi_pf_vecs,
                           phi_u_vecs,
                           by = matsindf::everything_except(phi_pf_vecs, phi_pf_colname) |>
                             as.character())
  if (nrow(temp) == 0) {
    out <- temp |>
      # Add an empty list column that would otherwise contain vectors.
      dplyr::mutate(
        "{phi_colname}" := list()
      )
  } else {
    # This line works around a weird bug that prevents
    # ncol_byname from working correctly.
    # Seemingly, the Matrix class needs to be pinged
    # before the following code works correctly.
    m <- matsbyname::Matrix(42)

    phi_df <- temp |>
      dplyr::mutate(
        # Check that all phi vectors have 1 column.
        "{.phi_shape_OK}" := (matsbyname::ncol_byname(.data[[phi_pf_colname]]) == 1) &
          (matsbyname::ncol_byname(.data[[phi_u_colname]]) == 1)
      )
    if (! all(phi_df[[.phi_shape_OK]])) {
      # Prepare an error message.
      bad_rows <- phi_df |>
        dplyr::filter(!.data[[.phi_shape_OK]])
      err_msg <- paste("In sum_phi_vecs(), need phi vectors with one column only. These vectors are bad:", matsindf::df_to_msg(bad_rows))
      stop(err_msg)
    }

    out <- phi_df |>
      dplyr::mutate(
        "{phi_colname}" := matsbyname::sum_byname(.data[[phi_pf_colname]], .data[[phi_u_colname]]),
        "{.phi_shape_OK}" := NULL
      )

    # Check that the length of each phi vector is the sum of the lengths of the phi_pf and phi_u vectors.
    # If not, there are duplicate rows in the vectors, which should be an error.
    # There should be no primary-final energy carriers that are also useful energy carriers.
    # Also check that the result of the sum is a single column.
    # If we get 2 or more columns, it means that the column names were different for phi_pf and phi_u,
    # which is an error.

    err_check <- out |>
      dplyr::mutate(
        "{.nrow_diffs}" := matsbyname::nrow_byname(.data[[phi_pf_colname]]) |> as.numeric() +
          matsbyname::nrow_byname(.data[[phi_u_colname]]) |> as.numeric() -
          matsbyname::nrow_byname(.data[[phi_colname]]) |> as.numeric(),
        "{.phi_sum_OK}" := matsbyname::iszero_byname(.data[[.nrow_diffs]]),
        "{.phi_cols_OK}" := matsbyname::ncol_byname(.data[[phi_colname]]) == 1
      )

    if (!all(err_check[[.phi_sum_OK]])) {
      # There is a problem.
      problem_rows <- err_check |>
        dplyr::filter(!.data[[.phi_sum_OK]]) |>
        dplyr::mutate(
          "{.nrow_diffs}" := NULL,
          "{.phi_sum_OK}" := NULL,
          "{.phi_cols_OK}" := NULL,
          "{phi_pf_colname}" := NULL,
          "{phi_u_colname}" := NULL,
          "{phi_colname}" := NULL
        )

      err_msg <- paste("In PFUPipeline::sum_phi_vecs(), the length of the sum of phi_pf and phi_u vectors",
                       "was not the same as the sum of vector lengths. The rows that failed the test are",
                       matsindf::df_to_msg(problem_rows))
      stop(err_msg)
    }
    if (!all(err_check[[.phi_cols_OK]])) {
      # There is a problem.
      problem_rows <- err_check |>
        dplyr::filter(!.data[[.phi_cols_OK]]) |>
        dplyr::mutate(
          "{phi_pf_colname}" := paste(phi_pf_colname, "=", matsbyname::getcolnames_byname(.data[[phi_pf_colname]])),
          "{phi_u_colname}" := paste(phi_u_colname, "=", matsbyname::getcolnames_byname(.data[[phi_u_colname]])),
          "{.nrow_diffs}" := NULL,
          "{.phi_sum_OK}" := NULL,
          "{.phi_cols_OK}" := NULL,
          "{phi_colname}" := NULL
        )
      err_msg <- paste("In PFUPipeline::sum_phi_vecs(), the names of the phi.pf and phi.u columns should be the same.",
                       "Rows that failed the test are",
                       matsindf::df_to_msg(problem_rows))
      stop(err_msg)
    }
  }

  out |>
    dplyr::mutate(
      # Delete the columns we no longer need.
      # These are not relevant
      # The output works for energy at all stages of the energy conversion chain,
      # so we don't need to track last stage.
      "{last_stage}" := NULL,
      # This function converts from energy to exergy, so we
      # should remove dependence on energy type.
      "{energy_type}" := NULL,
      # This function work for any method of counting the primary energy of renewable electricity.
      # Once we have the primary energy of renewable electricity,
      # this function will have identified the exergy-to-energy ratio associated with that
      # primary energy carrier.
      "{method}" := NULL,
      # These were temporary columns
      "{phi_pf_colname}" := NULL,
      "{phi_u_colname}" := NULL
    )
}
