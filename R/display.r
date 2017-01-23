#' Convert a retentionList to a single data frame that's ready for plotting.
#'
#' @param retentionList The result of calling
#' clustApply(FUN=calculateWeeklyRetention) on a userClust object.
#' @return A data.frame of the form  (cluster, relative_session_week,
#' pct_active)
#' @importFrom dplyr mutate
#' @importFrom dplyr select
#' @export
squashRetentionList <- function(retentionList){
  out <- data.frame(stringsAsFactors=F)
  for(i in 1:length(retentionList)){
    newDf <- dplyr::mutate(retentionList[[i]], cluster=i)
    newDf <- dplyr::select(newDf, cluster, relative_session_week, pct_active)
    out <- rbind(out, newDf)
  }
  out
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
#' @return A data frame, one row for each cluster, showing that cluster's
#' aggregate platform action distribution.
#' @importFrom dplyr mutate
#' @importFrom dplyr rename
#' @export
squashPADistList <- function(aggPADistList){
  longData <- data.frame(stringsAsFactors=F)
  for(i in 1:length(aggPADistList)){
    newDf <- dplyr::mutate(aggPADistList[[i]], user_id=i)
    longData <- rbind(longData, newDf)
  }
  wideData <- spreadPADistData(longData)
  dplyr::rename(wideData, cluster=user_id)
}

