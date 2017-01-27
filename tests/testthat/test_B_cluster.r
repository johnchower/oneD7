glootility::connect_to_redshift()
RPostgreSQL::dbGetQuery(conn = redshift_connection$con
                        , statement = glootility::query_pa_flash_cat)
RPostgreSQL::dbGetQuery(conn = redshift_connection$con
                        , statement = glootility::query_user_flash_cat)

############### Test spreadPADistData ###############
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

############### Test clusterUsers ###############
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

test_that("clustApply throws errors properly",{
  expect_error(clustApply(z, FUN=mean))
  expect_error(clustApply(z, height = .24, num_clusters = 3, FUN=mean))
})

############### Test clustApply ###############
w <- clustApply(z, height=.24, FUN=mean)
expected_result_w <- 
  list(
    list(
      varCombo=c(cluster=1)
      , result = 3100/3
    )
    , list(
      varCombo=c(cluster=2)
      , result = 25/2
    )
  )
names(expected_result_w) <- as.character(c(1,2))
varCombos_w <- w %>%
  sapply(function(x)x$varCombo) %>%
  as.data.frame
varCombos_expected_result_w <- expected_result_w %>% 
  sapply(function(x)x$varCombo) %>%
  as.data.frame
extraGroupings_test <- 
  data.frame(
    user_id = userSet
    , var1 = c(1,1,1,2,2)
    , var2 = c(1,2,1,1,1)
    , stringsAsFactors=F
  )  
w2 <- clustApply(z
                , height=.24
                , extraGroupings = extraGroupings_test
                , FUN=mean)
expected_result_w2 <- 
  list(
    list(
      varCombo=c(cluster=1, var1=1, var2=1)
      , result = 3101/2
    )
    , list(
      varCombo=c(cluster=2, var1=1, var2=2)
      , result = 20
    )
    , list(
      varCombo=c(cluster=2, var1=2, var2=1)
      , result = 5
    )
    , list(
      varCombo=c(cluster=1, var1=2, var2=1)
      , result = 99
    )
  )
names(expected_result_w2) <- 
  c('1.1.1','2.1.2','2.2.1','1.2.1')
varCombos_w2 <- w2 %>%
  sapply(function(x)x$varCombo) %>%
  as.data.frame
varCombos_expected_result_w2 <- expected_result_w2 %>% 
  sapply(function(x)x$varCombo) %>%
  as.data.frame

test_that("clustApply returns results.",{
  expect_is(w, 'list')
})
test_that("clustApply returns correct varCombos",{
  expect_equal(varCombos_w
               , varCombos_expected_result_w)
})
test_that("clustApply returns results with extra groupings",{
  expect_is(w2, 'list')
})
test_that("clustApply returns correct varCombos with extra groupings",{
  expect_equal(varCombos_w2
               , varCombos_expected_result_w2[
                   ,order(colnames(varCombos_expected_result_w2))
                 ])
})

############### Test clusterUsers ###############
extraData_test <- data.frame(user_id = rep(userSet, times = 2)
                             , variable = 
                                c(rep("a", times = length(userSet))
                                  , rep("b", times = length(userSet))
                                )
                             , value = 1:(2*length(userSet))
                             , stringsAsFactors = F)
clustVariables_test_a <- c('a')
clustVariables_test_b <- c('Connect')
clustVariables_test_c <- desired_colnames

test_that("clusterUsers returns results with subsets and extra variables",{
  clusterUsers_results_with_extraData <- 
    clusterUsers(x, extraData = extraData_test)
  expect_is(clusterUsers_results_with_extraData
             , 'hclust')

  clusterUsers_results_with_subset_a <- 
    clusterUsers(x
                 , extraData=extraData_test
                 , clustVariables=clustVariables_test_a)
  expect_is(clusterUsers_results_with_subset_a
             , 'hclust')

  expect_error(
    clusterUsers(x
                 , extraData=extraData_test
                 , clustVariables=clustVariables_test_b)
  )

  clusterUsers_results_with_subset_c <- 
    clusterUsers(x
                 , extraData=extraData_test
                 , clustVariables=clustVariables_test_c)
  expect_is(clusterUsers_results_with_subset_c
             , 'hclust')
})

RPostgreSQL::dbDisconnect(conn = redshift_connection$con)
