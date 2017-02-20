#' Calculate the weekly retention rate curve given a group of users.
#'
#' @param users A numeric vector of user ids.
#' @param rundate A dateid of the form yyyymmdd (numeric). All dates after the
#' rundate will be filtered out.
#' @param con Database connection to use for query.
#' @return A data frame of the form (weekBeginning, pctActive)
#' @import RPostgreSQL
calculateWeeklyRetention <- function(users = NULL
                                     , rundate = as.numeric(
                                                   gsub(pattern = "-" 
                                                        , replacement = "" 
                                                        , x = Sys.Date())
                                                 )
                                     ,con = redshift_connection$con){
  if(length(users)==1){
    stop("'users' must be either NULL or a group of at least 2 users")
  } else if(is.null(users)){
    userGroupQuery <- 
      paste0("SELECT DISTINCT ud.id "
             , "FROM user_dimensions ud "
             , "LEFT JOIN user_platform_action_facts upaf "
             , "on upaf.user_id=ud.id "
             , "WHERE ud.email IS NOT NULL "
             , "AND upaf.platform_action=\'Account Created\' ")

  } else {  
    usersChar <- paste(users, collapse = ',') 
    userGroupQuery <- paste0(
      'SELECT DISTINCT id FROM user_dimensions WHERE id IN ('
      , usersChar
      , ')'
    )
  }
  runDateQuery <- paste0('SELECT id as date_id FROM date_dim where id='
                         , rundate)
  retentionQuery0 <- gsub(pattern = 'xyz_userGroupQuery_xyz'
                         , replacement = userGroupQuery
                         , x = query_retention_sub)
  retentionQuery <- gsub(pattern = 'xyz_runDateQuery_xyz'
                         , replacement = runDateQuery
                         , x = retentionQuery0)
  RPostgreSQL::dbGetQuery(conn = con
                          , statement = retentionQuery)
}

#' Calculate weekly retention numbers for individual users.
#' 
#' @param users A numeric vector of user ids.
#' @param rundate A dateid of the form yyyymmdd (numeric). All dates after the
#' rundate will be filtered out.
#' @param con Database connection to use for query.
#' @return A data frame of the form (user_id, week, showed_up)
#' @import RPostgreSQL
calculateIndividualRetention <- function(users=NULL
                                         , rundate = as.numeric(
                                                       gsub(pattern = "-" 
                                                            , replacement = "" 
                                                            , x = Sys.Date())
                                                     )
                                         ,con = redshift_connection$con){
  if(length(users)==1){
    stop("'users' must be either NULL or a group of at least 2 users")
  } else if(is.null(users)){
    userGroupQuery <- 
      paste0("SELECT DISTINCT ud.id "
             , "FROM user_dimensions ud "
             , "LEFT JOIN user_platform_action_facts upaf "
             , "on upaf.user_id=ud.id "
             , "WHERE ud.email IS NOT NULL "
             , "AND upaf.platform_action=\'Account Created\' ")

  } else {  
    usersChar <- paste(users, collapse = ',') 
    userGroupQuery <- paste0(
      'SELECT DISTINCT id FROM user_dimensions WHERE id IN ('
      , usersChar
      , ')'
    )
  }
  runDateQuery <- paste0('SELECT id as date_id FROM date_dim where id='
                         , rundate)
  retentionQuery0 <- gsub(pattern = 'xyz_userGroupQuery_xyz'
                         , replacement = userGroupQuery
                         , x = query_individual_retention_sub)
  retentionQuery <- gsub(pattern = 'xyz_runDateQuery_xyz'
                         , replacement = runDateQuery
                         , x = retentionQuery0)
  RPostgreSQL::dbGetQuery(conn = con
                          , statement = retentionQuery)
}
