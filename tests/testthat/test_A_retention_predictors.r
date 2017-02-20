library(dplyr)

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

N <- 1000
set.seed(1)
userSet <- RPostgreSQL::dbGetQuery(conn = redshift_connection$con
                                   , statement = "
  SELECT DISTINCT id
  FROM user_dimensions
  where email is not null
  ") %>%
  {.$id} %>%
  sample(size = N)
userSetChar <- as.character(userSet)
x <- calculatePADist(users= userSet
                     , maxTime = 60
                     , con = redshift_connection$con)

test_that("calculatePADist gives everyone a distribution",{
  user_ids_to_test <- unique(x$user_id)
  expect_equal(object = user_ids_to_test[order(user_ids_to_test)]
               , expected = userSet[order(userSet)])
})
RPostgreSQL::dbDisconnect(conn = redshift_connection$con)
