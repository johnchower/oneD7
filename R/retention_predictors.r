#' Calculate the platform action distribution of a group of users.
#'
#' Calculates a platform action distribution for a given group of users over a
#' given time frame. Requires the existence of a pa_flash_cat temporary table.
#' @param agg Logical. Should results be reported in aggregate (T) or on an
#' individual basis (F). 
#' @param users A numeric vector of user ids.
#' @param minTime The earliest moment to count platform actions, relative to
#' the users' first platform action. (Numeric, measured in minutes.)
#' @param maxTime The last moment to count platform actions, relative to
#' the users' first platform action. (Numeric, measured in minutes.)
#' @param rundate A dateid of the form yyyymmdd (numeric). All dates after the
#' rundate will be filtered out.
#' @param con Database connection to use for query.
#' @return A data frame of the form (user_id, platform_action_category,
#' percentage).
#' @import RPostgreSQL
#' @export
calculatePADist <- function(users = NULL
                            , minTime = 0
                            , maxTime = 60
                            , agg = F
                            , rundate = as.numeric(
                                          gsub(pattern = "-"
                                               , replacement = ""
                                               , x = Sys.Date())
                                        )
                            , con = redshift_connection$con){
  if(length(users)==1){
    stop("'users' must be either NULL or a group of at least 2 users")
  } else if(is.null(users)){
    userGroupQuery <- 
      'SELECT DISTINCT id FROM user_dimensions WHERE email IS NOT NULL'
  } else {  
    usersChar <- paste(users, collapse = ',') 
    userGroupQuery <- 
      paste0(
        'SELECT DISTINCT id FROM user_dimensions WHERE id IN ('
        , usersChar
        , ')'
      )
  }
  runDateQuery <- paste0('SELECT id as date_id FROM date_dim where id='
                         , rundate)
  distQuery <- gsub(pattern = 'xyz_userGroupQuery_xyz'
                       , replacement = userGroupQuery
                         , x = query_pa_dist_sub)
  distQuery <- gsub(pattern = 'xyz_runDateQuery_xyz'
                    , replacement = runDateQuery
                    , x = distQuery)
  distQuery <- gsub(pattern = 'xyz_minTime_xyz'
                       , replacement = minTime
                         , x = distQuery)
  distQuery <- gsub(pattern = 'xyz_maxTime_xyz'
                       , replacement = maxTime
                         , x = distQuery)
  if(agg){
    distQuery <- gsub(pattern = 'xyz_resultQuery_xyz'
                         , replacement = 'SELECT * FROM agg_pa_pct'
                         , x = distQuery)
  } else {
    distQuery <- gsub(pattern = 'xyz_resultQuery_xyz'
                         , replacement = 'SELECT * FROM user_pa_pct'
                         , x = distQuery)
  }
  RPostgreSQL::dbGetQuery(conn = con
                          , statement = distQuery)
}
