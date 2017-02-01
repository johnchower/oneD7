glootility::connect_to_redshift()
RPostgreSQL::dbGetQuery(conn = redshift_connection$con
                        , statement = glootility::query_pa_flash_cat)
RPostgreSQL::dbGetQuery(conn = redshift_connection$con
                        , statement = glootility::query_user_flash_cat)

########## Test getConfounders #########
users_test <- 1:100
queryList_test <-  list(oneD7::query_confounder_use_case_sub
                        , oneD7::query_confounder_first_name_sub)

test_that("getConfounders returns results",{
  object_to_test <- getConfounders(users=NULL
                                   , queryList=queryList_test)
  variable_names <- unique(object_to_test$variable)
  expect_is(object = object_to_test
            , class = 'data.frame')
  expect_gt(object = nrow(object_to_test)
                      , expected = 0)
  expect_equal(object = colnames(object_to_test)
               , expected = c('user_id', 'variable', 'value') )
  expect_equal(object = variable_names[order(variable_names)]
               , expected = c('account_type'
                              , 'first_name'
                              , 'use_case') )
})
test_that("getConfounders returns results on a subset",{
  object_to_test <- getConfounders(users=users_test
                                   , queryList=queryList_test)
  variable_names <- unique(object_to_test$variable)
  expected_variable_names <- c('account_type'
                              , 'first_name'
                              , 'use_case')
  expect_is(object = object_to_test
            , class = 'data.frame')
  expect_gt(object = nrow(object_to_test)
                      , expected = 0)
  expect_equal(object = colnames(object_to_test)
               , expected = c('user_id', 'variable', 'value') )
  expect_equal(
    object = variable_names[order(variable_names)]
    , expected =  expected_variable_names[order(expected_variable_names)]
  )
})
