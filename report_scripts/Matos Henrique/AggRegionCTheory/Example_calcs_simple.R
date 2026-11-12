Y_f <- matrix(c(10, 0,
                0, 20), byrow = TRUE, nrow = 2,
              dimnames = list(c("Electricity", "Gas"),
                              c("Industry", "Residential"))) |>
  matsbyname::setrowtype("Product") |>
  matsbyname::setcoltype("Industry")

U_u <- matrix(c(10, 0,
                0, 20), byrow = TRUE, nrow = 2,
              dimnames = list(c("Electricity", "Gas"),
                              c("Pumps -> HPF", "Furnaces -> LTH.20.C"))) |>
  matsbyname::setrowtype("Product") |>
  matsbyname::setcoltype("Industry -> Product")

V_u <- matrix(c(12, 0,
                0, 5), byrow = TRUE, nrow = 2,
              dimnames = list(c("Furnaces -> LTH.20.C",
                                "Pumps -> HPF"),
                              c("LTH.20.C [from Furnaces]",
                                "HPF [from Pumps]"))) |>
  matsbyname::setrowtype("Industry -> Product") |>
  matsbyname::setcoltype("Product [from Industry]")

Y_u <- matrix(c(0, 12,
                5, 0), byrow = TRUE, nrow = 2,
              dimnames = list(c("LTH.20.C [from Furnaces]",
                                "HPF [from Pumps]"),
                              c("Industry", "Residential"))) |>
  matsbyname::setrowtype("Product [from Industry]") |>
  matsbyname::setcoltype("Industry")

Y_u_rename <- Y_u |>
  matsbyname::switch_notation_byname(margin = 1,
                                     from = RCLabels::from_notation,
                                     to = RCLabels::arrow_notation,
                                     flip = TRUE)

eta_i <- matsbyname::quotient_byname(
  matsbyname::rowsums_byname(V_u, colname = "Product") |>
    matsbyname::setcoltype("Product"),
  matsbyname::colsums_byname(U_u) |>
    matsbyname::setrowtype("Product") |>
    matsbyname::transpose_byname())

eta_i_hatinv <- eta_i |>
  matsbyname::hatinv_byname(keep = "rownames")

# Define a new notation
underline_notation <- RCLabels::notation_vec(sep = " ___ ")

tol <- 1e-10
common_coltype <- "Common"

this_sector <- "Industry"
this_Ef <- "Electricity"


C_mat <- NULL
for (this_sector in colnames(Y_f)) {
  for (this_Ef in rownames(Y_f)) {
    U_u_Ef_row <- U_u |>
      matsbyname::select_rows_byname(this_Ef) |>
      matsbyname::selector_byname(margin = 2, tol = tol) |>
      matsbyname::transpose_byname() |>
      matsbyname::setcoltype(common_coltype)

    Y_u_s_col <- Y_u_rename |>
      matsbyname::select_cols_byname(this_sector) |>
      matsbyname::selector_byname(margin = 1, tol = tol) |>
      matsbyname::setcoltype(common_coltype)

    # Multiply to find common machines
    common_machines <- matsbyname::hadamardproduct_byname(U_u_Ef_row, Y_u_s_col) |>
      matsbyname::clean_byname() |>
      matsbyname::getrownames_byname()
    if (is.null(common_machines)) {
      next
    }

    # Select the common machines from Y_u
    Y_u_common <- Y_u_rename |>
      matsbyname::select_rows_byname(retain_pattern = RCLabels::make_or_pattern(common_machines))

    # Figure out new column names
    new_colnames <- RCLabels::paste_pref_suff(pref = this_Ef,
                                              suff = colnames(Y_u_common),
                                              notation = RCLabels::arrow_notation)

    Y_u_common_new_colnames <- Y_u_common |>
      matsbyname::setcolnames_byname(new_colnames)

    # Find the final energy associated with these machines
    # by multiplying by hatinv(etai)
    Y_u_common_Ef <- eta_i_hatinv |>
      matsbyname::matrixproduct_byname(Y_u_common_new_colnames)

    # Create the piece of the allocation matrix
    this_C_submat <- Y_u_common_Ef |>
      matsbyname::transpose_byname() |>
      matsbyname::fractionize_byname(margin = 1) |>
      matsbyname::clean_byname()

    C_mat <- matsbyname::sum_byname(C_mat, this_C_submat)
  }
}



