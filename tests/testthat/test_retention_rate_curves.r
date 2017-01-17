glootility::connect_to_redshift()

test_that("calculateWeeklyRetention returns results",{
  allUserRetention <- calculateWeeklyRetention()
  multiUserRetention <- calculateWeeklyRetention(users = 1:20)

  testthat::expect_is(object = allUserRetention
                      , class = "data.frame")
  testthat::expect_error(calculateWeeklyRetention(users = 1))
  testthat::expect_is(object = multiUserRetention
                      , class = "data.frame")

  testthat::expect_gt(object = nrow(allUserRetention)
                      , expected = 0)
  testthat::expect_gt(object = nrow(multiUserRetention)
                      , expected = 0)

})
RPostgreSQL::dbDisconnect(conn = redshift_connection$con)
