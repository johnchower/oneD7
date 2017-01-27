#' Convert a retentionList to a single data frame that's ready for plotting.
#'
#' @param retentionList The result of calling
#' clustApply(FUN=calculateWeeklyRetention) on a userClust object.
#' @return A data.frame of the form  (cluster, <additional variables>
#' , relative_session_week, pct_active). Here, <additional variables>
#' represents the variables which appear in the extraGroupings parameter of the
#' clustApply call.
#' @importFrom dplyr mutate
#' @importFrom dplyr select
#' @importFrom plyr ldply
#' @export
squashRetentionList <- function(retentionList){
  plyr::ldply( 
    .data = retentionList
    , .fun = function(list){
      userGroup <- data.frame(as.list(list$varCombo)) 
      userGroup <- dplyr::mutate(userGroup, dummy=T)
      retentionData <- list$result
      retentionData <- dplyr::mutate(retentionData, dummy=T)
      out <- dplyr::full_join(userGroup, retentionData, by = 'dummy')
      dplyr::select(out, -dummy, -total_eligible_users, -active_users)
  }) 
}

#' Convert an aggPADistList to a single data frame that's ready to be
#' displayed.
#'
#' @param aggPADistList The result of calling
#' clustApply(FUN = function(u){
#'       dplyr::select(calculatePADist(u, agg = T)
#'                     , flash_report_category, pct_platform_actions)
#'     }
#' )
#' on a userClust object.
#' @param long Logical; should data be returned in long format (T) or wide
#' format (F)?
#' @param clustVariables Character vector. Variables that were used in 
#' the clustering process. All variables but these will be dropped, and
#' percentages will be re-computed based on these variables only. If left null,
#' then no variables will be dropped.
#' @return A data frame showing each cluster's aggregate platform 
#' action distribution.
#' @importFrom dplyr mutate
#' @importFrom dplyr rename
#' @importFrom dplyr filter
#' @importFrom dplyr group_by
#' @importFrom dplyr ungroup
#' @importFrom dplyr %>%
#' @export
squashPADistList <- function(aggPADistList
                             , long = F
                             , clustVariables = NULL){
  aggPADistList <- lapply(aggPADistList, function(x)x$result)
  longData <- data.frame(stringsAsFactors=F)
  for(i in 1:length(aggPADistList)){
    newDf <- dplyr::mutate(aggPADistList[[i]], user_id=i)
    longData <- rbind(longData, newDf)
  }
  if(!is.null(clustVariables)){
    longData <- longData %>% 
      filter(flash_report_category %in% clustVariables) %>%
      group_by(user_id) %>%
      mutate(
        pct_platform_actions = pct_platform_actions/sum(pct_platform_actions)
      ) %>%
      ungroup
  }
  if(long){
    longData <- dplyr::rename(longData, cluster=user_id)
    out <- longData
  } else {
    wideData <- spreadPADistData(longData)
    wideData <- dplyr::rename(wideData, cluster=user_id)
    out <- wideData
  }
  out
}

