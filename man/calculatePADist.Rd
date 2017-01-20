% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/retention_predictors.r
\name{calculatePADist}
\alias{calculatePADist}
\title{Calculate the platform action distribution of a group of users.}
\usage{
calculatePADist(users = NULL, minTime = 0, maxTime = 60,
  con = redshift_connection$con)
}
\arguments{
\item{users}{A numeric vector of user ids.}

\item{minTime}{The earliest moment to count platform actions, relative to
the users' first platform action. (Numeric, measured in minutes.)}

\item{maxTime}{The last moment to count platform actions, relative to
the users' first platform action. (Numeric, measured in minutes.)}

\item{con}{Database connection to use for query.}
}
\value{
A data frame of the form (user_id, platform_action_category,
percentage).
}
\description{
Calculates a platform action distribution for a given group of users over a
given time frame. Requires the existence of a pa_flash_cat temporary table.
If no such table exists, query the
}
