% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/retention_rate_curves.r
\name{calculateIndividualRetention}
\alias{calculateIndividualRetention}
\title{Calculate weekly retention numbers for individual users.}
\usage{
calculateIndividualRetention(users = NULL, runDate = as.numeric(gsub(pattern
  = "-", replacement = "", x = Sys.Date())), con = redshift_connection$con)
}
\arguments{
\item{users}{A numeric vector of user ids.}

\item{runDate}{A dateid of the form yyyymmdd (numeric). All dates after the
runDate will be filtered out.}

\item{con}{Database connection to use for query.}
}
\value{
A data frame of the form (user_id, week, showed_up)
}
\description{
Calculate weekly retention numbers for individual users.
}

