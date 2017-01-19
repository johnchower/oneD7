glootility::connect_to_redshift()

test_that("calculatePADist throws error if pa_flash_cat doesn't exist in Redshift",{
  testthat::expect_error(calculatePADist(users = 1:20, maxTime = 1))
})

RPostgreSQL::dbGetQuery(conn = redshift_connection$con
                        , statement = glootility::query_pa_flash_cat)

test_that("calculatePADist returns results",{
  allUserPADist <- calculatePADist(maxTime = 1)
  multiUserPADist <- calculatePADist(users = 1:20, maxTime = 1)

  testthat::expect_is(object = allUserPADist
                      , class = "data.frame")
  testthat::expect_error(calculatePADist(users = 1, maxTime = 1))
  testthat::expect_is(object = multiUserPADist
                      , class = "data.frame")

  testthat::expect_gt(object = nrow(allUserPADist)
                      , expected = 0)
  testthat::expect_gt(object = nrow(multiUserPADist)
                      , expected = 0)
})
result_set <- RPostgreSQL::dbGetInfo(redshift_connection$con)$rsId
lapply(result_set
       , function(x){RPostgreSQL::dbClearResult(x)}
)
RPostgreSQL::dbDisconnect(conn = redshift_connection$con)
