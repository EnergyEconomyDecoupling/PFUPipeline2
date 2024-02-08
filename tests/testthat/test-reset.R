test_that("pl_destroy() works as expected", {
  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  df <- tibble::tribble(~A, ~B,
                        1,  2,
                        3,  4)
  DBI::dbWriteTable(conn, name = "df", df)
  table_names <- DBI::dbListTables(conn)
  expect_equal(table_names, "df")

  pl_destroy(conn)
  table_names_2 <- DBI::dbListTables(conn)
  expect_equal(length(table_names_2), 0)
  DBI::dbDisconnect(conn)
})
