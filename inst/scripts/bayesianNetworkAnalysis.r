library(bnlearn)
library(tidyr)
library(ggplot2)
library(dplyr)
library(plotly)
devtools::load_all()

# Should we cluster everyone or, just N users?
N <- 2000 #Number of users
take_sample <- F
rundate <- 20170213

########### RUN ONCE #############
# Connect to Redshift and create temporary tables user_flash_cat 
# and pa_flash_cat.
glootility::connect_to_redshift()
RPostgreSQL::dbGetQuery(conn = redshift_connection$con
                        , statement = glootility::query_pa_flash_cat)
RPostgreSQL::dbGetQuery(conn = redshift_connection$con
                        , statement = glootility::query_user_flash_cat)

# Calculate the platform action distribution for all users, in their first hour
# on the platform.
allUserPADist <- oneD7::calculatePADist(maxTime = 60*24
                                        , runDate = rundate) 

# Select a subset of users to perform the analysis on
set.seed(seed = 1)
if(!take_sample){
  N <- length(unique(allUserPADist$user_id))
}
userSet <- sample(unique(allUserPADist$user_id), size = N, replace = F)
allUserPADist <- allUserPADist %>%
  filter(user_id %in% userSet)

# Cluster all users according to their hour-1 platform action distribution.
# 
# #### Create the Dataset from scratch and save it as an .rda file ######
# clusterStartTime <- Sys.time()
# allUserClust <- clusterUsers(allUserPADist
#                              , clustVariables = cluster_variables)
# clusterEndTime <- Sys.time()
# clusterEndTime - clusterStartTime
# save(allUserClust, file = '/Users/johnhower/Data/allUserClust_20170213.rda')
 
#### Load the dataset from an .rda file #####
load(file = '/Users/johnhower/Data/allUserClust_20170213.rda')

############ SET PARAMETERS, RUN MANY TIMES ###########
K <- 2
cluster_variables <- c('Connect'
                        ,'Consume'
                        ,'Create'
                        ,'Feed'
                        ,'Invite'
                        ,'Other actions'
                        ,'Space'
                        ,'To-do')
query_list <- list(oneD7::query_confounder_use_case_sub
                   , oneD7::query_confounder_oneD7_sub
                   , oneD7::query_confounder_FL_REVEAL_sub
                   , oneD7::query_confounder_belongs_to_cohort_sub)

# Get values of each confounding variable for each user.
allUserConfounders <- oneD7::getConfounders(queryList = query_list
                                            , runDate = rundate) %>%
  filter(user_id %in% userSet)

# Calculate individual retention numbers for each user.
allUserIndividualRetention <- calculateIndividualRetention(runDate=rundate) %>%
  filter(user_id %in% userSet) %>%
  mutate(active = ifelse(is.na(active), 0 ,1))

# Get bayesian network input data
bnInputData <- 
  combineClusterRetentionConfounders(
    userClust = allUserClust
    , individualRetention = allUserIndividualRetention
    , userConfounders = allUserConfounders
    , num_clusters = K
  )

# Debug NA issue

diff1 <- setdiff(as.numeric(allUserIndividualRetention$user_id)
	,as.numeric(allUserClust$labels))
diff2 <- setdiff(allUserConfounders$user_id
	,as.numeric(allUserClust$labels))
diff3 <- setdiff(allUserConfounders$user_id
	,as.numeric(allUserIndividualRetention$user_id))
diff4 <- setdiff(as.numeric(allUserClust$labels)
	,as.numeric(allUserIndividualRetention$user_id) )
diff5 <- setdiff(as.numeric(allUserClust$labels)
	,allUserConfounders$user_id )
diff6 <- setdiff(as.numeric(allUserIndividualRetention$user_id)
	,allUserConfounders$user_id )
diffs = list(diff1, diff2, diff3, diff4, diff5, diff6)
lapply(diffs, length)

allUserConfoundersWide <- tidyr::spread(data = allUserConfounders
                                        , key = 'variable'
                                        , value = 'value')

# How many users don't have a cluster?
bnInputData %>%
  filter(!is.na(cluster)) %>%
  {.$user_id} %>%
  unique %>%
  length

# Calculate bayesian network for week 10
W <- 10
bnInputData %>%
  filter(relative_session_week==W) %>%
  select(-relative_session_week) %>%
  {bnlearn::hc(.)} %>%
  plot

plot(cut(x = as.dendrogram(allUserClust), h = 8200))
