library(oneD7)
library(broom)
library(bnlearn)
library(tidyr)
library(ggplot2)
library(dplyr)
library(plotly)
devtools::load_all()

# Should we use a previous cluster file, or generate a new one?
clustFile <- '/Users/johnhower/Data/allUserClust_20170221.rda'

# Should we cluster everyone or, just N users?
N <- 2000 #Number of users
take_sample <- F
rundate <- 20170221
cluster_variables <- c('Connect'
                        ,'Consume'
                        ,'Create'
                        ,'Feed'
                        ,'Invite'
                        ,'Other actions'
                        ,'Space'
                        ,'To-do')

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

# Generate allUserClust
if (!is.null(clustFile)){
  print(paste0("Loading ", clustFile))
  clusterStartTime <- Sys.time()
  load(file = clustFile )
  clusterEndTime <- Sys.time()
} else {
  print("Generating allUserClust.")
  clusterStartTime <- Sys.time()
  allUserClust <- clusterUsers(allUserPADist
                               , clustVariables = cluster_variables)
  clusterEndTime <- Sys.time()
  # save(allUserClust,file = '/Users/johnhower/Data/allUserClust_20170221.rda')
}
clusterEndTime - clusterStartTime

# Select a subset of users to perform the analysis on
set.seed(seed = 1)
if(!take_sample){
  N <- length(unique(allUserPADist$user_id))
}
userSet <- sample(unique(allUserPADist$user_id), size = N, replace = F)
allUserPADist <- allUserPADist %>%
  filter(user_id %in% userSet)

#### Load the dataset from an .rda file #####

############ SET PARAMETERS, RUN MANY TIMES ###########
K <- 8
query_list <- list(oneD7::query_confounder_use_case_sub
                   , oneD7::query_confounder_oneD7_sub
                   # , oneD7::query_confounder_FL_REVEAL_sub
                   , oneD7::query_confounder_belongs_to_cohort_sub
                   , oneD7::query_confounder_first_champ_sub
                   )

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

allUserConfoundersWide <- tidyr::spread(data = allUserConfounders
                                        , key = 'variable'
                                        , value = 'value')

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


retentionData2 <- data.frame()
for( W in 1:50){
  print(W)
  bnInputDataClean <- bnInputData %>%
    filter(relative_session_week==W) %>%
    {.[complete.cases(.),]} %>%
    select(-relative_session_week
           , -user_id) %>%
    lapply(FUN=as.factor) %>%
    as.data.frame %>%
    select(active
      , oned7
      , cluster
      , first_champ
    )
  bnStructure <- 
    bnlearn::hc(
      bnInputDataClean
      , whitelist = data.frame(
          from = c('cluster'
                   , 'cluster'
                   , 'first_champ'
                   , 'oned7')
          , to = c('active'
                   , 'first_champ'
                   , 'oned7'
                   , 'active')
        )
      , blacklist = data.frame(
          from = c('cluster'
                   , 'first_champ'
                   , 'first_champ'
                   , 'oned7'
                   , 'oned7'
                   , 'active'
                   , 'active'
                   , 'active'
                   )
          , to = c('oned7'
                   , 'cluster'
                   , 'active'
                   , 'cluster'
                   , 'first_champ'
                   , 'cluster'
                   , 'first_champ'
                   , 'oned7'
                   )
        )
    )
  bnFit <- bn.fit(bnStructure, bnInputDataClean)
  bnFit$active$prob
  pActive <- tidy(bnFit$active$prob) %>%
    rename(pAGivenCO = Freq)
  pOneD7 <- tidy(bnFit$oned7$prob) %>%
    rename(pOGivenF = Freq)
  pFirstChamp <- tidy(bnFit$first_champ$prob) %>%
    rename(pFGivenC = Freq)
  probMatrix <- pActive %>%
    left_join(pOneD7, by = 'oned7') %>% 
    left_join(pFirstChamp, by = c('first_champ', 'cluster'))
  pActiveGivenDoCluster <- probMatrix %>%
    distinct(active, oned7, cluster, first_champ, .keep_all = T) %>%
    group_by(active,cluster) %>%
    summarise(pct_active = sum(pAGivenCO*pOGivenF*pFGivenC)) %>%
    mutate(relative_session_week = W) %>%
    ungroup
  retentionData2 <- rbind(retentionData2, pActiveGivenDoCluster)
}

x <- retentionData2 %>%
  ungroup %>%
  filter(relative_session_week > 0
         , relative_session_week <= 50
         , active == 1) %>%
  mutate(active=as.character(active)
         , cluster = as.character(cluster)) %>%
  ggplot(aes(x=relative_session_week
             , y=pct_active
             , color = cluster
             , group = cluster)) +
  geom_line() +
  ggthemes::theme_tufte()
ggplotly(x)

RPostgreSQL::dbDisconnect(conn = redshift_connection$con)
