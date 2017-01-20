glootility::connect_to_redshift()

RPostgreSQL::dbGetQuery(conn = redshift_connection$con
                        , statement = glootility::query_pa_flash_cat)

test_that("spreadPADistData returns correct columns",{
  x <- calculatePADist(users = 1:10
                       , maxTime = 1
                       , con = redshift_connection$con)
  y <- spreadPADistData(x)
  desired_colnames <- c('user_id'
                        , 'Uncategorized'
                        , 'Other actions'
                        , 'Feed'
                        , 'Connect'
                        , 'Space'
                        , 'To-do'
                        , 'Consume'
                        , 'Invite'
                        , 'Create'
                        , NA)
  testthat::expect_equal(colnames(y)[order(colnames(y))]
                         , desired_colnames[order(desired_colnames)])
})

test_that("clusterUsers returns results",{
  userSet <- c(1,20, 3000, 5,99)
  userSetChar <- as.character(userSet)
  x <- calculatePADist(users= userSet
                       , maxTime = 60
                       , con = redshift_connection$con)
  z <- clusterUsers(x)
  expect_is(object = z
            , class = "hclust")
  expect_is(object = z$labels
            , class = 'character')
  expect_equal(object = z$labels[order(z$labels)]
               , expected = userSetChar[order(userSetChar)])
})

RPostgreSQL::dbDisconnect(conn = redshift_connection$con)
