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
#' @importFrom dplyr mutate
spreadPADistData <- function(paDistData){
  longData <- dplyr::select(paDistData
                            , user_id
                            , flash_report_category
                            , pct_platform_actions)
  longData <- 
    dplyr::mutate(longData
                 , flash_report_category = ifelse(is.na(flash_report_category)
                                                  , 'NA'
                                                  , flash_report_category))
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
                        , 'NA')
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
#' @param extraData A data frame of form (user_id, variable, value) containing
#' data on users outside of their platform action distributions.
#' @param clustVariables A character vector specifying which variables to
#' include in the clustering routine. 
#' @param distParams Named list of additional parameteres to pass to stats::dist
#' @param hclustParams Named list of additional parameters to pass to
#' fastcluster::hclust
#' @return An object of type hclust whose labels correspond to the user ids
#' that appear in paDistDataWide
#' @importFrom stats dist
#' @importFrom dplyr left_join
#' @importFrom dplyr select
#' @importFrom dplyr filter
#' @importFrom fastcluster hclust
#' @importFrom tidyr spread
clusterUsers <- function(paDistData = NULL
                         , extraData = NULL
                         , clustVariables = NULL
                         , distParams = NULL
                         , hclustParams = list(method='ward.D')){
  paDistDataExists <- !is.null(paDistData)
  extraDataExists <- !is.null(extraData)
  clustVariablesExist <- !is.null(clustVariables)

  if(!extraDataExists & !paDistDataExists){
    stop("Must specify at least one of paDistData or extraData")
  }

  if(extraDataExists & clustVariablesExist){
    extraData <- dplyr::filter(extraData
                                , variable %in% clustVariables)
    extraDataExists <- nrow(extraData)>0
  }

  if(paDistDataExists & clustVariablesExist){
    paDistData <- dplyr::filter(paDistData
                                , flash_report_category %in% clustVariables)
    paDistDataExists <- nrow(paDistData)>0
  }

  if(!extraDataExists & !paDistDataExists){
    stop("Filtering on clustVariables left nothing!")
  }

  if(extraDataExists & paDistDataExists){
    paDistDataWide <- spreadPADistData(paDistData)
    extraDataWide <- tidyr::spread(extraData, variable, value)
    clusterDataWide <- dplyr::left_join(x = paDistDataWide
                                    , y = extraDataWide
                                    , by = 'user_id')
  } else if(extraDataExists & !paDistDataExists){
    extraDataWide <- tidyr::spread(extraData, variable, value)
    clusterDataWide <- extraDataWide
  } else if(!extraDataExists & paDistDataExists){
    paDistDataWide <- spreadPADistData(paDistData)
    clusterDataWide <- paDistDataWide
  }

  rownames(clusterDataWide) <- clusterDataWide$user_id
  clusterDataWide <- dplyr::select(clusterDataWide, -user_id)
  distMatrix <- do.call(dist, c(list(x=clusterDataWide), distParams))
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
#' @param extraGroupings data.frame of the form (user_id, var1, ... varN). The
#' var's are categorical, and are used to split the users into smaller groups.
#' @param FUN A function, one of whose arguments is a set of user_ids.
#' @param ... Additional arguments to pass to FUN
#' @return A named list of objects, of type value(FUN), one for each cluster in
#' hclustObject, when cut at height 'height'.
#' @importFrom stats cutree
#' @importFrom plyr dlply
#' @importFrom dplyr full_join
#' @importFrom dplyr %>%
#' @importFrom dplyr select
#' @export
clustApply <- function(hclustObject
                       , height = NULL
                       , num_clusters = NULL
                       , extraGroupings = NULL
                       , FUN
                       , ...){
  if(sum(c(is.null(height), is.null(num_clusters)))!=1){
    stop("Must specify exactly one of 'height' or 'num_clusters'")
  }
  cluster <- if(is.null(height) & !is.null(num_clusters)){
    stats::cutree(hclustObject, k=num_clusters)
  } else if(!is.null(height) & is.null(num_clusters)){
    stats::cutree(hclustObject, h=height)
  } 
  cluster <- as.data.frame(cluster)
  userDf <- data.frame(user_id = as.numeric(rownames(cluster)))
  cluster <- cbind(cluster, userDf)
  if(!is.null(extraGroupings)){
    totalGroupings <- 
      dplyr::full_join(cluster, extraGroupings, by = 'user_id')
  } else {
    totalGroupings <- cluster
  }
  totalGroupingsColnames <- colnames(totalGroupings)
  resultList <- plyr::dlply(
    .data = totalGroupings
    , .variables = totalGroupingsColnames[totalGroupingsColnames!='user_id']
    , .fun = function(df){
      Result <- FUN(df$user_id, ...)
      Combo <- df %>% {
        dplyr::select(., -user_id)
        } %>%
        unique %>%
        unlist
      out <- list(varCombo=Combo, result=Result)
      return(out)
  })
  names(resultList) <- as.character(names(resultList))
  resultList
}
