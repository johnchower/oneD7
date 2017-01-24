glootility::connect_to_redshift()
RPostgreSQL::dbGetQuery(conn = redshift_connection$con
                        , statement = glootility::query_pa_flash_cat)
RPostgreSQL::dbGetQuery(conn = redshift_connection$con
                        , statement = glootility::query_user_flash_cat)
test_that("calculatePADist returns results",{
  allUserPADist <- oneD7::calculatePADist(maxTime = 1)
  multiUserPADist <- calculatePADist(users = 1:20, maxTime = 1)
  aggUserPADist <- calculatePADist(maxTime=1, agg=T)

  testthat::expect_is(object = allUserPADist
                      , class = "data.frame")
  testthat::expect_error(calculatePADist(users = 1, maxTime = 1))
  testthat::expect_is(object = multiUserPADist
                      , class = "data.frame")
  testthat::expect_is(object = aggUserPADist
                      , class = "data.frame")

  testthat::expect_gt(object = nrow(allUserPADist)
                      , expected = 0)
  testthat::expect_gt(object = nrow(multiUserPADist)
                      , expected = 0)
  testthat::expect_gt(object = nrow(aggUserPADist)
                      , expected = 0)
})
RPostgreSQL::dbDisconnect(conn = redshift_connection$con)
