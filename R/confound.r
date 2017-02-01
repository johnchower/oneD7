#' Get a data frame matching each user to the values of several confounding
#' variables.
#'
#' @param users Numeric, a vector of user ids to include in the calculation.
#' @param queryList A list of queries to run in the calculation. Each query
#' calculates one or more confounding variables, and returns the results in a
#' data.frame. 
#' @param wide Should results be returned in wide format (T) or long format
#' (F)?
#' @param con The database connection to run the queryList against.
#' @return A data.frame of the form (user_id, variable, value). 
#' @importFrom RPostgreSQL dbGetQuery
#' @importFrom plyr ldply
#' @importFrom tidyr gather
getConfounders <- function(users = NULL
                           , queryList
                           , wide = F
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
  resultList <- 
    lapply(X = queryList
           , FUN = function(query){
             queryToRun <- gsub(pattern = 'xyz_userGroupQuery_xyz'
                                , replacement = userGroupQuery
                                , x = query)
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
