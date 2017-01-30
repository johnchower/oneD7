library(tidyr)

glootility::connect_to_redshift()
RPostgreSQL::dbGetQuery(conn = redshift_connection$con
                        , statement = glootility::query_pa_flash_cat)
RPostgreSQL::dbGetQuery(conn = redshift_connection$con
                        , statement = glootility::query_user_flash_cat)

############## Test squashRetentionList ######################
userSet <- 1:20
multiUserPADist <- calculatePADist(users =userSet, maxTime = 1)
multiUserClust <- clusterUsers(multiUserPADist)
retentionList <- clustApply(hclustObject=multiUserClust
                            , height = 5
                            , FUN = calculateWeeklyRetention)
extraGroupings_test <- 
  data.frame(
    user_id = userSet
    , var1 = rep(c(1,2), times = length(userSet)/2)
    , stringsAsFactors=F
  )  
extraGroupings_test_long <- gather(data = extraGroupings_test
                                   , key = 'variable'
                                   , value = 'value'
                                   , -user_id)
multiUserClust_extraGroupings <- 
  clusterUsers(paDistData = multiUserPADist
               , extraData = extraGroupings_test_long)
retentionList_extraGroupings <- 
  clustApply(hclustObject = multiUserClust_extraGroupings
             , height = 5
             , extraGroupings = extraGroupings_test
             , FUN = calculateWeeklyRetention)

test_that("squashRetentionList returns results",{
  object_to_test <- squashRetentionList(retentionList)
  testthat::expect_is(object = object_to_test
                      , class = 'data.frame')
  testthat::expect_gt(object = nrow(object_to_test)
                      , expected = 0)
  testthat::expect_equal(object = colnames(object_to_test)
                         , expected = c('cluster'
                                        , 'relative_session_week'
                                        , 'pct_active'))
})

test_that("squashRetentionList returns results with extra variables",{
  object_to_test <- squashRetentionList(retentionList_extraGroupings)
  testthat::expect_is(object = object_to_test
                      , class = 'data.frame')
  testthat::expect_gt(object = nrow(object_to_test)
                      , expected = 0)
  testthat::expect_equal(object = colnames(object_to_test)
                         , expected = c('cluster'
                                        , 'var1'
                                        , 'relative_session_week'
                                        , 'pct_active'))
})


############## Test squashPADistList ######################
multiUserPADist <- calculatePADist(users = 1:100, maxTime = 1)
multiUserClust <- clusterUsers(multiUserPADist)
aggPADistList <- clustApply(
  hclustObject=multiUserClust
  , height = 5
  , FUN = function(u){
      dplyr::select(calculatePADist(u, agg = T)
                    , flash_report_category, pct_platform_actions)
    }
)
desired_colnames <- c('cluster'
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
filtered_colnames <- c('cluster'
                      , 'Feed'
                      , 'Connect'
                      , 'Space'
                      , 'To-do'
                      , 'Consume'
                      , 'Invite'
                      , 'Create'
                      , 'NA')

test_that("squashPADistList returns results",{
  object_to_test <- squashPADistList(aggPADistList)
  testthat::expect_is(object = object_to_test
                      , class = 'data.frame')
  testthat::expect_gt(object = nrow(object_to_test)
                      , expected = 0)
  testthat::expect_equal(
    object = colnames(object_to_test)[order(colnames(object_to_test))]
    , expected = desired_colnames[order(desired_colnames)]
  )
})
test_that("squashPADistList returns filtered results",{
  object_to_test <- squashPADistList(aggPADistList
                                     , clustVariables = filtered_colnames)
  testthat::expect_is(object = object_to_test
                      , class = 'data.frame')
  testthat::expect_gt(object = nrow(object_to_test)
                      , expected = 0)
  testthat::expect_equal(
    object = colnames(object_to_test)[order(colnames(object_to_test))]
    , expected = desired_colnames[order(desired_colnames)]
  )
})
test_that("squashPADistList returns long results",{
  object_to_test <- squashPADistList(aggPADistList
                                     , long = T)
  desired_colnames <- c('cluster'
                        , 'flash_report_category'
                        , 'pct_platform_actions')
  expect_is(object = object_to_test
                      , class = 'data.frame')
  expect_gt(object = nrow(object_to_test)
                      , expected = 0)
  expect_equal(
    object = colnames(object_to_test)[order(colnames(object_to_test))]
    , expected = desired_colnames[order(desired_colnames)]
  )
})
dbDisconnect(conn = redshift_connection$con)
