library(dplyr)

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
N <- 1000
set.seed(1)
userSet <- RPostgreSQL::dbGetQuery(conn = redshift_connection$con
                                   , statement =
      paste0("SELECT DISTINCT ud.id "
             , "FROM user_dimensions ud "
             , "LEFT JOIN user_platform_action_facts upaf "
             , "on upaf.user_id=ud.id "
             , "WHERE ud.email IS NOT NULL "
             , "AND upaf.platform_action=\'Account Created\' ")
  ) %>%
  {.$id} %>%
  sample(size = N)
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


############### Test clustApply ###############
userSet <- 1:16
userSetChar <- as.character(userSet)
x <- data.frame(
  user_id=userSet
  , flash_report_category = c(rep("Space", times = 8)
                              , rep("Connect", times = 8) )
  , count_platform_actions = rep(1, times = 16)
  , total_platform_actions = rep(1, times = 16)
  , pct_platform_actions = rep(1, times = 16)
  , stringsAsFactors = F
)
z <- clusterUsers(x)
w <- clustApply(z, height=2, FUN=mean)
expected_result_w <- 
  list(
    list(
      varCombo=c(cluster=1)
      , result = 4.5
    )
    , list(
      varCombo=c(cluster=2)
      , result = 12.5
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
    , var1 = rep(c(rep(1, times = 4)
                   , rep(2, times = 4))
                 , times = 2)
    , var2 = rep(c(rep(1, times = 2)
                   , rep(2, times = 2))
                 , times = 4)
    , stringsAsFactors=F
  )  
w2 <- clustApply(z
                , height=2
                , extraGroupings = extraGroupings_test
                , FUN=mean)
expected_result_w2 <- 
  list(
    list(
      varCombo=c(cluster=1, var1=1, var2=1)
      , result = 1.5
    )
    , list(
      varCombo=c(cluster=1, var1=1, var2=2)
      , result = 3.5
    )
    , list(
      varCombo=c(cluster=1, var1=2, var2=1)
      , result = 5.5
    )
    , list(
      varCombo=c(cluster=1, var1=2, var2=2)
      , result = 7.5
    )
    , list(
      varCombo=c(cluster=2, var1=1, var2=1)
      , result = 9.5
    )
    , list(
      varCombo=c(cluster=2, var1=1, var2=2)
      , result = 11.5
    )
    , list(
      varCombo=c(cluster=2, var1=2, var2=1)
      , result = 13.5
    )
    , list(
      varCombo=c(cluster=2, var1=2, var2=2)
      , result = 15.5
    )
  )
names(expected_result_w2) <- 
  c('1.1.1','1.1.2','1.2.1','1.2.2','2.1.1','2.1.2','2.2.1','2.2.2')
varCombos_w2 <- w2 %>%
  sapply(function(x)x$varCombo) %>%
  as.data.frame
varCombos_expected_result_w2 <- expected_result_w2 %>% 
  sapply(function(x)x$varCombo) %>%
  as.data.frame

test_that("clustApply throws errors properly",{
  expect_error(clustApply(z, FUN=mean))
  expect_error(clustApply(z, height = .24, num_clusters = 3, FUN=mean))
})
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
test_that("clustApply returns correct varCombos with extra groupings",{
  # Set paramaters
  K <- 6
  N <- 1000 #Number of users
  cluster_variables <- c('Connect'
                          ,'Consume'
                          ,'Create'
                          ,'Feed'
                          ,'Invite'
                          ,'Other actions'
                          ,'Space'
                          ,'To-do')
  query_list <- list(query_confounder_use_case_sub)
  allUserPADist <- calculatePADist(maxTime = 60*24) 
  # Select a subset of users to perform the analysis on
  set.seed(seed = 1)
  userSet <- sample(unique(allUserPADist$user_id), size = N, replace = F)
  allUserPADist <- allUserPADist %>%
    filter(user_id %in% userSet)
  # Get values of each confounding variable for each user.
  allUserConfounders <- getConfounders(users = userSet
                                       , queryList = query_list)
  allUserClust <- clusterUsers(allUserPADist
                               , clustVariables = cluster_variables)
  object_to_test <- clustApply(hclustObject=allUserClust
                              , num_clusters = K
                              , extraGroupings = allUserConfounders
                              , FUN = calculateWeeklyRetention)
  expect_is(object_to_test, 'list')
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
clustVariables_test_b <- c('Invite')
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
