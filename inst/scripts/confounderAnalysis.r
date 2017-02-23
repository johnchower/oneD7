library(tidyr)
library(ggplot2)
library(dplyr)
library(plotly)
devtools::load_all()

########### RUN ONCE #############
# Connect to Redshift and create temporary tables user_flash_cat 
# and pa_flash_cat.
glootility::connect_to_redshift()
RPostgreSQL::dbGetQuery(conn = redshift_connection$con
                        , statement = glootility::query_pa_flash_cat)
RPostgreSQL::dbGetQuery(conn = redshift_connection$con
                        , statement = glootility::query_user_flash_cat)
# Set rundate
rundate <- 20170213

# Calculate the platform action distribution for all users, in their first hour
# on the platform.
allUserPADist <- oneD7::calculatePADist(maxTime = 60*24
                                        , runDate=rundate) 

# Select a subset of users to perform the analysis on
N <- 2000 #Number of users
set.seed(seed = 1)
take_sample <- F # Should we cluster everyone or, just N users?
if(!take_sample){
  N <- length(unique(allUserPADist$user_id))
}
userSet <- sample(unique(allUserPADist$user_id), size = N, replace = F)
allUserPADist <- allUserPADist %>%
  filter(user_id %in% userSet)
# Define which confounders will be controlled for
query_list <- list(oneD7::query_confounder_use_case_sub
                   , oneD7::query_confounder_oneD7_sub
                   , oneD7::query_confounder_FL_REVEAL_sub
                   # , oneD7::query_confounder_belongs_to_cohort_sub
                   )

# Cluster all users according to their hour-1 platform action distribution.
#### Create the Dataset from scratch and save it as an .rda file ######
# clusterStartTime <- Sys.time()
# allUserClust <- clusterUsers(allUserPADist
#                              , clustVariables = cluster_variables)
# clusterEndTime <- Sys.time()
# clusterEndTime - clusterStartTime
# save(allUserClust, file = '/Users/johnhower/Data/allUserClust_20170201.rda')
#### Load the dataset from an .rda file #####
# load(file = '/Users/johnhower/Data/allUserClust_20170213.rda')

# Get values of each confounding variable for each user.
allUserConfounders <- oneD7::getConfounders(queryList = query_list
                                            , runDate = rundate) %>%
  filter(user_id %in% userSet)

############ SET PARAMETERS, RUN MANY TIMES ###########
K <- 6
cluster_variables <- c('Connect'
                        ,'Consume'
                        ,'Create'
                        ,'Feed'
                        ,'Invite'
                        ,'Other actions'
                        ,'Space'
                        ,'To-do')
# Calculate aggregate platform action distributions for each cluster.
aggPADistList <- clustApply(
  hclustObject=allUserClust
  , num_clusters = K
  , FUN = function(u){
      dplyr::select(calculatePADist(u, agg = T, runDate = rundate)
                    , flash_report_category, pct_platform_actions)
    }
)

# Display aggregate platform action distributions for each cluster
paDistDataWide <- squashPADistList(aggPADistList
                                   , clustVariables = cluster_variables)

# Plot platform action distributions as stacked bar chart
q <- paDistDataWide %>%
  plot_ly(
    x = ~cluster, y = ~Connect, type = 'bar', name = 'Connect'
    , text = ~paste0('Connect', ': ', round(100*Connect), '%')
    , hoverinfo = 'text'
  ) %>%
  add_trace(y = ~Consume, name = 'Consume'
            , text = ~paste0('Consume', ': ', round(100*Consume), '%')
            , hoverinfo = 'text') %>%
  add_trace(y = ~Create, name = 'Create'
            , text = ~paste0('Create', ': ', round(100*Create), '%')
            , hoverinfo = 'text') %>%
  add_trace(y = ~Feed, name = 'Feed'
            , text = ~paste0('Feed', ': ', round(100*Feed), '%')
            , hoverinfo = 'text') %>%
  add_trace(y = ~Invite, name = 'Invite'
            , text = ~paste0('Invite', ': ', round(100*Invite), '%')
            , hoverinfo = 'text') %>%
  add_trace(y = ~`Other actions`, name = 'Other actions'
            , text=~paste0('Other actions',': ',round(100*`Other actions`),'%')
            , hoverinfo = 'text') %>%
  add_trace(y = ~Space, name = 'Space'
            , text = ~paste0('Space', ': ', round(100*Space), '%')
            , hoverinfo = 'text') %>%
  add_trace(y = ~`To-do`, name = 'To-do'
            , text = ~paste0('To-do', ': ', round(100*`To-do`), '%')
            , hoverinfo = 'text') %>%
  layout(yaxis = list(title = ''), barmode = 'stack') 
q

# Calculate long-term retention numbers for each cluster, confounder pair. 
allUserConfoundersWide <- spread(allUserConfounders
                                 , key = 'variable'
                                 , value = 'value')
retentionList <- clustApply(hclustObject=allUserClust
                            , num_clusters = K
                            , extraGroupings = allUserConfoundersWide
                            , FUN = function(x){
                                calculateWeeklyRetention(x, runDate = rundate)
                            })
retentionData <- squashRetentionList(retentionList)

# Calculate p(confounder) for each cluster
pConfounder <- allUserConfoundersWide %>%
  mutate(total_users=length(unique(user_id))) %>%
  group_by(# belongs_to_cohort
           connected_to_cfp
           , connected_to_cru
           , connected_to_summerconnect
           , connected_to_tyro
           , connected_to_cedar
           , connected_to_reveal
           , connected_to_fl
           # , oned7 
           , account_type) %>%
  summarise(probability=length(unique(user_id))/mean(total_users))

# Cluster size list
clusterSizeList <- clustApply(hclustObject = allUserClust
                              , num_clusters = K
                              , FUN = length)

# Calculate confounder-controlled long-term retention probabilities.
  # Mutate NAs to zero
  retentionData[is.na(retentionData)] <- 0
retentionData2 <-  retentionData %>%
  left_join(pConfounder, by = c('account_type'
                                , 'connected_to_cfp'
                                , 'connected_to_cru'
                                , 'connected_to_summerconnect'
                                , 'connected_to_tyro'
                                , 'connected_to_cedar'
                                , 'connected_to_reveal'
                                , 'connected_to_fl'
                               # , 'oned7'
                                )) %>%
  group_by(relative_session_week, cluster) %>%
  summarise(pct_active=sum(pct_active*probability))

# Plot long-term retention numbers for each cluster
x <- retentionData2 %>%
  filter(relative_session_week > 0) %>%
  mutate(cluster = as.character(cluster)) %>%
  ggplot(aes(x=relative_session_week
             , y=pct_active
             , color = cluster
             , group = cluster)) +
  geom_line() +
  ggthemes::theme_tufte()
ggplotly(x)

RPostgreSQL::dbDisconnect(conn = redshift_connection$con)
