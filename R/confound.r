#' Get a data frame matching each user to the values of several confounding
#' variables.
#'
#' @param users Numeric, a vector of user ids to include in the calculation.
#' @param queryList A list of queries to run in the calculation. Each query
#' calculates one or more confounding variables, and returns the results in a
#' data.frame. 
#' @param wide Should results be returned in wide format (T) or long format
#' (F)?
#' @param runDate A dateid of the form yyyymmdd (numeric). All dates after the
#' runDate will be filtered out.
#' @param con The database connection to run the queryList against.
#' @return A data.frame of the form (user_id, variable, value). 
#' @importFrom RPostgreSQL dbGetQuery
#' @importFrom plyr ldply
#' @importFrom tidyr gather
#' @export
getConfounders <- function(users = NULL
                           , queryList
                           , wide = F
                           , runDate = as.numeric(
                                         gsub(pattern = "-" 
                                              , replacement = "" 
                                              , x = Sys.Date())
                                       )
                           , con = redshift_connection$con){
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
    userGroupQuery <- 
      paste0(
        'SELECT DISTINCT id FROM user_dimensions WHERE id IN ('
        , usersChar
        , ')'
      )
  }
  runDateQuery <- paste0('SELECT id as date_id FROM date_dim where id='
                         , runDate)
  resultList <- 
    lapply(X = queryList
           , FUN = function(query){
             queryToRun0 <- gsub(pattern = 'xyz_userGroupQuery_xyz'
                                , replacement = userGroupQuery
                                , x = query)
             queryToRun <- gsub(pattern = 'xyz_runDateQuery_xyz'
                                , replacement = runDateQuery
                                , x = queryToRun0)
             resultsWide <- RPostgreSQL::dbGetQuery(conn = con
                                     , statement = queryToRun)
             tidyr::gather(resultsWide
                           , "variable"
                           , "value"
                           , -user_id)
         })
  out <- plyr::ldply(.data = resultList
                     , .fun = function(x)x)
  if(wide){
    out <- tidyr::spread(data = out
                         , key = 'variable'
                         , value = 'value')
  }
  return(out)
}
