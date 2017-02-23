glootility::connect_to_redshift()

test_that("calculateWeeklyRetention returns results",{
  allUserRetention <- calculateWeeklyRetention(runDate=20170201)
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

test_that("calculateIndividualRetention returns results",{
  allUserRetention <- calculateIndividualRetention(runDate=20170201)
  allUserColnames <- colnames(allUserRetention)
  multiUserRetention <- calculateIndividualRetention(users = 1:20)
  multiUserColnames <- colnames(multiUserRetention)
  expectedColnames <- c('user_id'
                        , 'relative_session_week'
                        , 'active')
  testthat::expect_is(object = allUserRetention
                      , class = "data.frame")
  testthat::expect_error(calculateIndividualRetention(users = 1))
  testthat::expect_is(object = multiUserRetention
                      , class = "data.frame")
  testthat::expect_equal(object = allUserColnames[order(allUserColnames)]
                         , expected=expectedColnames[order(expectedColnames)])
  testthat::expect_gt(object = nrow(allUserRetention)
                      , expected = 0)
  testthat::expect_gt(object = nrow(multiUserRetention)
                      , expected = 0)
  testthat::expect_equal(object = multiUserColnames[order(multiUserColnames)]
                         , expected=expectedColnames[order(expectedColnames)])
})

RPostgreSQL::dbDisconnect(conn = redshift_connection$con)
