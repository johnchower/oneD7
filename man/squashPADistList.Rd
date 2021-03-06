% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/display.r
\name{squashPADistList}
\alias{squashPADistList}
\title{Convert an aggPADistList to a single data frame that's ready to be
displayed.}
\usage{
squashPADistList(aggPADistList, long = F, clustVariables = NULL)
}
\arguments{
\item{aggPADistList}{The result of calling
clustApply(FUN = function(u){
      dplyr::select(calculatePADist(u, agg = T)
                    , flash_report_category, pct_platform_actions)
    }
)
on a userClust object.}

\item{long}{Logical; should data be returned in long format (T) or wide
format (F)?}

\item{clustVariables}{Character vector. Variables that were used in 
the clustering process. All variables but these will be dropped, and
percentages will be re-computed based on these variables only. If left null,
then no variables will be dropped.}
}
\value{
A data frame showing each cluster's aggregate platform 
action distribution.
}
\description{
Convert an aggPADistList to a single data frame that's ready to be
displayed.
}

