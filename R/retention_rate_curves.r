#' Calculate the weekly retention rate curve given a group of users.
#'
#' @param users A numeric vector of user ids.
#' @param con Database connection to use for query.
#' @return A data frame of the form (weekBeginning, pctActive)
#' @import RPostgreSQL
calculateWeeklyRetention <- function(users = NULL
                                     ,con = redshift_connection$con){
  if(length(users)==1){
    stop("'users' must be either NULL or a group of at least 2 users")
  } else if(is.null(users)){
    userGroupQuery <- 
      'SELECT DISTINCT id FROM user_dimensions WHERE email IS NOT NULL'
  } else {  
    usersChar <- paste(users, collapse = ',') 
    userGroupQuery <- paste0(
      'SELECT DISTINCT id FROM user_dimensions WHERE id IN ('
      , usersChar
      , ')'
    )
  }
  retentionQuery <- gsub(pattern = 'xyz_userGroupQuery_xyz'
                         , replacement = userGroupQuery
                         , x = query_retention_sub)
  RPostgreSQL::dbGetQuery(conn = con
                          , statement = retentionQuery)
}
