glootility::connect_to_redshift()
RPostgreSQL::dbGetQuery(conn = redshift_connection$con
                        , statement = glootility::query_pa_flash_cat)
RPostgreSQL::dbGetQuery(conn = redshift_connection$con
                        , statement = glootility::query_user_flash_cat)

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
                      , 'NA')

test_that("spreadPADistData returns correct columns",{
  expect_equal(colnames(y)[order(colnames(y))]
                         , desired_colnames[order(desired_colnames)])
})

userSet <- c(1,20, 3000, 5,99)
userSetChar <- as.character(userSet)
x <- calculatePADist(users= userSet
                     , maxTime = 60
                     , con = redshift_connection$con)
z <- clusterUsers(x)

test_that("clusterUsers returns results",{
  expect_is(object = z
            , class = "hclust")
  expect_is(object = z$labels
            , class = 'character')
  expect_equal(object = z$labels[order(z$labels)]
               , expected = userSetChar[order(userSetChar)])
})

test_that("clustApply returns results.",{
  w <- clustApply(z, height=.24, FUN=mean)
  expect_is(w, 'list')
  expect_equal(object = w[[1]]
               , expected = 3100/3)
  expect_equal(object = w[[2]]
               , expected = 12.5)
  expect_error(clustApply(z, FUN=mean))
  expect_error(clustApply(z, height = .24, num_clusters = 3, FUN=mean))
})

extraData_test <- data.frame(user_id = rep(userSet, times = 2)
                             , variable = c(rep("a", times = length(userSet))
                                            , rep("b", times = length(userSet)))
                             , value = 1:(2*length(userSet))
                             , stringsAsFactors = F)
clustVariables_test_a <- c('a')
clustVariables_test_b <- c('Connect')
clustVariables_test_c <- desired_colnames


test_that("clusterUsers returns results even with subsets and extra variables",{
  clusterUsers_results_with_extraData <- 
    clusterUsers(x, extraData = extraData_test)
  expect_is(clusterUsers_results_with_extraData
             , 'hclust')

  clusterUsers_results_with_subset_a <- 
    clusterUsers(x, extraData=extraData_test, clustVariables=clustVariables_test_a)
  expect_is(clusterUsers_results_with_subset_a
             , 'hclust')

  expect_error(
    clusterUsers(x, extraData=extraData_test, clustVariables=clustVariables_test_b)
  )

  clusterUsers_results_with_subset_c <- 
    clusterUsers(x, extraData=extraData_test, clustVariables=clustVariables_test_c)
  expect_is(clusterUsers_results_with_subset_c
             , 'hclust')
})

RPostgreSQL::dbDisconnect(conn = redshift_connection$con)
