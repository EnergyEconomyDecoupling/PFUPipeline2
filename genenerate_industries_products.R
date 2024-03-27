# This script generates industries and products
# in the arrow notation format.
# Use this script when you have a few
# industries and products missing
# from the Industry and Product foreign key tables.
# Copy the resulting column of data
# into the Industry and Product tables
# in SchemaAndFKTables.xlsx file.

# Set up the connection to the database
conn <- DBI::dbConnect(drv = RPostgres::Postgres(),
                       dbname = "v2.0a1",
                       host = "eviz.cs.calvin.edu",
                       port = 5432,
                       user = "postgres")

# Get the existing Industry and Product tables from the database
industry_table <- DBI::dbReadTable(conn = conn, name = "Industry")
product_table <- DBI::dbReadTable(conn = conn, name = "Product")
# Read the allocation tables
completed_allocation_tables <- tar_read(CompletedAllocationTables) |>
  # Use the "ticket" to load all allocations from the database
  PFUPipelineTools::pl_collect_from_hash(conn = conn)

# CompletedAllocationTables contains all
# Machine -> Useful product
# relationships in the database.

completed_allocation_tables |>
  dplyr::select(dplyr::all_of(c(IEATools::template_cols$machine, IEATools::template_cols$eu_product))) |>
  unique() |>
  # Join by industry and product tables to sort
  dplyr::left_join(industry_table, by = dplyr::join_by(Machine == Industry)) |>
  dplyr::left_join(product_table, by = dplyr::join_by(EuProduct == Product)) |>
  # Now sort by Machine, then Product, according to the tables already in the database.
  dplyr::arrange(IndustryID, ProductID) |>
  # Make a column of arrow notation strings
  dplyr::mutate(
    industry_string = RCLabels::paste_pref_suff(pref = .data[[IEATools::template_cols$machine]],
                                                suff = .data[[IEATools::template_cols$eu_product]],
                                                notation = RCLabels::arrow_notation),
    Machine = NULL,
    EuProduct = NULL,
    IndustryID = NULL,
    ProductID = NULL
  ) |>
  # And write to disk
  write.csv(file = "extra_machine_product_pairs.csv", row.names = FALSE)

# CompletedAllocationTables contains all
# Final energy -> Sector
# relationships in the database.

completed_allocation_tables |>
  dplyr::select(dplyr::all_of(c(IEATools::template_cols$ef_product, IEATools::template_cols$destination))) |>
  unique() |>
  # Join by industry and product tables to sort
  dplyr::left_join(product_table, by = dplyr::join_by(EfProduct == Product)) |>
  dplyr::left_join(industry_table, by = dplyr::join_by(Destination == Industry)) |>
  # Now sort by Machine, then Product, according to the tables already in the database.
  dplyr::arrange(ProductID, IndustryID) |>
  # Make a column of arrow notation strings
  dplyr::mutate(
    product_string = RCLabels::paste_pref_suff(pref = .data[[IEATools::template_cols$ef_product]],
                                               suff = .data[[IEATools::template_cols$destination]],
                                               notation = RCLabels::arrow_notation),
    EfProduct = NULL,
    Destination = NULL,
    IndustryID = NULL,
    ProductID = NULL
  ) |>
  # And write to disk
  write.csv(file = "extra_product_sector_pairs.csv", row.names = FALSE)

