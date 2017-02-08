glootility::connect_to_redshift()
RPostgreSQL::dbGetQuery(conn = redshift_connection$con
                        , statement = glootility::query_pa_flash_cat)
RPostgreSQL::dbGetQuery(conn = redshift_connection$con
                        , statement = glootility::query_user_flash_cat)

allUserPADist <- calculatePADist(maxTime = 60*24) 
N <- 2000 #Number of users
cluster_variables <- c('Connect'
                        ,'Consume'
                        ,'Create'
                        ,'Feed'
                        ,'Invite'
                        ,'Other actions'
                        ,'Space'
                        ,'To-do')
query_list <- list(query_confounder_use_case_sub
                   , query_confounder_oneD7_sub
                   , query_confounder_FL_REVEAL_sub
                   , query_confounder_belongs_to_cohort_sub)
userSet <- sample(unique(allUserPADist$user_id), size = N, replace = F)
allUserPADist <- dplyr::filter(allUserPADist, user_id %in% userSet)
allUserClust <- clusterUsers(allUserPADist
                             , clustVariables = cluster_variables)
allUserConfounders <- getConfounders(users = userSet
                                            , queryList = query_list) 
allUserRetention <- calculateIndividualRetention(users = userSet)

test_that("combineClusterRetentionConfounders returns results",{
  object_to_test <- 
    combineClusterRetentionConfounders(userClust=allUserClust
                                       , individualRetention=allUserRetention
                                       , userConfounder=allUserConfounders
                                       , num_clusters=2)
  colnames_to_test <- colnames(object_to_test)
  colnames_expected <- c('user_id'
                         , 'relative_session_week'
                         , 'active'
                         , 'cluster'
                         , 'belongs_to_cohort'
                         , 'connected_to_reveal'
                         , 'connected_to_fl'
                         , 'oned7'
                         , 'account_type')
  expect_is(object_to_test
            , 'data.frame')
  expect_gt(nrow(object_to_test)
            , 0)
  expect_equal(
    object = colnames_to_test[order(colnames_to_test)]
    , expected =  colnames_expected[order(colnames_expected)]
  )
})

RPostgreSQL::dbDisconnect(conn = redshift_connection$con)
