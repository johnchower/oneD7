#' Combine cluster, individual retention, and confounding variables into a
#' single data frame.
#'
#' @param userClust An hclust object representing the platform action
#' distribution clusters for all users.
#' @param individualRetention A data.frame, the result of calling
#' calculateIndividualRetention 
#' @param userConfounders A data.frame, the result of calling getConfounders
#' @param ... Additional arguments to pass to clustApply
#' @return A data frame of the form (user_id, week, confounder1, ... confounder
#' N, active)
#' @importFrom plyr ldply
#' @importFrom dplyr left_join
#' @importFrom dplyr select
combineClusterRetentionConfounders <- function(userClust
                                               , individualRetention
                                               , userConfounders
                                               , ...){
  userConfoundersWide <- tidyr::spread(data = userConfounders
                                       , key = 'variable'
                                       , value = 'value')
  userClustList <- clustApply(hclustObject = userClust
                            , FUN = function(u){data.frame(user_id = u)}
                            , ...) 
  userClustDF <- plyr::ldply(.data = userClustList
              , .fun = function(x){
                x$result
              })
  out0 <- dplyr::left_join(individualRetention
                   , userClustDF
                   , by = 'user_id')
  out1 <- dplyr::left_join(out0
                   , userConfoundersWide
                   , by = 'user_id')
  out1
}
