#' Calculate the platform action distribution of a group of users.
#'
#' Calculates a platform action distribution for a given group of users over a
#' given time frame. Requires the existence of a pa_flash_cat temporary table.
#' If no such table exists, query the 
#' @param users A numeric vector of user ids.
#' @param minTime The earliest moment to count platform actions, relative to
#' the users' first platform action. (Numeric, measured in minutes.)
#' @param maxTime The last moment to count platform actions, relative to
#' the users' first platform action. (Numeric, measured in minutes.)
#' @param con Database connection to use for query.
#' @return A data frame of the form (user_id, platform_action_category,
#' percentage).
#' @import RPostgreSQL
calculatePADist <- function(users = NULL
                            , minTime = 0
                            , maxTime = 60
                            , con = redshift_connection$con){
  try_pa_flash_cat <- 
    try(
      RPostgreSQL::postgresqlExecStatement(
        con
        , statement = "select * from pa_flash_cat limit 10;"
      )
      , silent = T
    )
  if(length(users)==1){
    stop("'users' must be either NULL or a group of at least 2 users")
  } else if(is(try_pa_flash_cat)[1]!="PostgreSQLResult"){
    stop(try_pa_flash_cat[1])
  } else if(is.null(users)){
    userGroupQuery <- 'SELECT DISTINCT id FROM user_dimensions'
  } else {  
    usersChar <- paste(users, collapse = ',') 
    userGroupQuery <- paste0('SELECT DISTINCT id FROM user_dimensions WHERE id IN ('
                             , usersChar
                             , ')')
  }
  distQuery <- gsub(pattern = 'xyz_userGroupQuery_xyz'
                       , replacement = userGroupQuery
                         , x = query_pa_dist_sub)
  distQuery <- gsub(pattern = 'xyz_minTime_xyz'
                       , replacement = minTime
                         , x = distQuery)
  distQuery <- gsub(pattern = 'xyz_maxTime_xyz'
                       , replacement = maxTime
                         , x = distQuery)
  RPostgreSQL::postgresqlCloseResult(try_pa_flash_cat, conn = con)
  RPostgreSQL::dbGetQuery(conn = con
                          , statement = distQuery)
}
