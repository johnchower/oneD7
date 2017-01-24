#' Spread the platform action distribution data
#'
#' Spreads the platform action ditribution data and adds variable columns if
#' any platform action groups are missing. This function gets called by
#' clusterUsers so there will typically be no reason to call it explicitly in
#' your workflow. 
#' @param paDistData A data frame, the result of calling calculatePADist
#' @importFrom tidyr gather
#' @importFrom dplyr select
#' @importFrom dplyr mutate_
spreadPADistData <- function(paDistData){
  longData <- dplyr::select(paDistData
                            , user_id
                            , flash_report_category
                            , pct_platform_actions)
  wideData <- tidyr::spread(longData
                            , flash_report_category
                            , pct_platform_actions
                            , fill = 0)
  desired_colnames <- c('user_id'
                        , 'Uncategorized'
                        , 'Other actions'
                        , 'Feed'
                        , 'Connect'
                        , 'Space'
                        , 'To-do'
                        , 'Consume'
                        , 'Invite'
                        , 'Create'
                        , NA)
  missing_columns <- setdiff(desired_colnames, colnames(wideData))
  if(length(missing_columns)>0){
    x <- wideData
    for(colname in missing_columns){
      x <- dplyr::mutate_(x, .dots=setNames(list(quote(0)), colname))
    }
    out <- x
  } else {out <- wideData}
  out
}

#' Cluster users according to their platform action distributions.
#'
#' Convenience function that chains calls to dist and hclust. The hclust method
#' defaults to ward.D since that's produced the cleanest clusterings in early
#' exploratory analyses.
#' @param paDistData A data frame, the result of calling calculatePADist
#' @param distParams Named list of additional parameteres to pass to stats::dist
#' @param hclustParams Named list of additional parameters to pass to
#' fastcluster::hclust
#' @return An object of type hclust whose labels correspond to the user ids
#' that appear in paDistDataWide
#' @importFrom stats dist
#' @importFrom dplyr select
#' @importFrom fastcluster hclust
clusterUsers <- function(paDistData
                         , distParams = NULL
                         , hclustParams = list(method='ward.D')){
  paDistDataWide <- spreadPADistData(paDistData)
  paDistDataWide2 <- dplyr::select(paDistDataWide, -user_id)
  rownames(paDistDataWide2) <- paDistDataWide$user_id
  distMatrix <- do.call(dist, c(list(x=paDistDataWide2), distParams))
  do.call(hclust, c(list(d=distMatrix), hclustParams))
}

#' Apply a function to each cluster from an hclust object, at a given height or
#' with a given number of clusters.
#'
#' @param hclustObject An hclust object, the result of calling clusterUsers
#' @param height Numeric, indicates the height at which to cut the dendogram
#' and take groups. Must specify either 'height' or 'num_clusters', but not
#' both
#' @param num_clusters Numeric, indicates the number of clusters to use from
#' the heirarchy
#' @param FUN A function, one of whose arguments is a set of user_ids.
#' @param ... Additional arguments to pass to FUN
#' @return A named list of objects, of type value(FUN), one for each cluster in
#' hclustObject, when cut at height 'height'.
#' @importFrom stats cutree
#' @export
clustApply <- function(hclustObject
                       , height = NULL
                       , num_clusters = NULL
                       , FUN
                       , ...){
  if(sum(c(is.null(height), is.null(num_clusters)))!=1){
    stop("Must specify exactly one of 'height' or 'num_clusters'")
  }
  clusters <- if(is.null(height) & !is.null(num_clusters)){
    stats::cutree(hclustObject, k=num_clusters)
  } else if(!is.null(height) & is.null(num_clusters)){
    stats::cutree(hclustObject, h=height)
  } 
  out <- list()
  for(clust in unique(clusters)){
    users <- names(clusters)[clusters==clust]
    users <- as.numeric(users)
    listEntry <- FUN(users, ...)
    out[[clust]] <- listEntry
  }
  out
}
