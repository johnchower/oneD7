library(tidyr)
library(ggplot2)
library(dplyr)
library(plotly)

# Set paramaters
K <- 3
cluster_variables <- c('Connect'
                        ,'Consume'
                        ,'Create'
                        ,'Feed'
                        ,'Invite'
                        ,'Other actions'
                        ,'Space'
                        ,'To-do')

# Connect to Redshift and create temporary tables user_flash_cat 
# and pa_flash_cat.
glootility::connect_to_redshift()
RPostgreSQL::dbGetQuery(conn = redshift_connection$con
                        , statement = glootility::query_pa_flash_cat)
RPostgreSQL::dbGetQuery(conn = redshift_connection$con
                        , statement = glootility::query_user_flash_cat)
# Calculate the platform action distribution for all users, in their first hour
# on the platform.
allUserPADist <- 
  oneD7::calculatePADist(maxTime = 60*24) 

# Cluster all users according to their hour-1 platform action distribution.
allUserClust <- clusterUsers(allUserPADist
                             , clustVariables = cluster_variables)

# Calculate aggregate platform action distributions for each cluster.
aggPADistList <- clustApply(
  hclustObject=allUserClust
  , num_clusters = K
  , FUN = function(u){
      dplyr::select(calculatePADist(u, agg = T)
                    , flash_report_category, pct_platform_actions)
    }
)

# Display aggregate platform action distributions for each cluster
paDistDataWide <- squashPADistList(aggPADistList
                                   , clustVariables = cluster_variables)

# Plot platform action distributions as stacked bar chart
p <- paDistDataWide %>%
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
p

# Calculate long-term retention numbers for each (cluster,variable) combo.
retentionList <- clustApply(hclustObject=allUserClust
                            , num_clusters = K
                            , FUN = calculateWeeklyRetention)

# Plot long-term retention numbers for each cluster
retentionData <- squashRetentionList(retentionList)
x <- retentionData %>%
  filter(relative_session_week > 0) %>%
  mutate(cluster = as.character(cluster)) %>%
  ggplot(aes(x=relative_session_week
             , y=pct_active
             , color = cluster
             , group = cluster)) +
  geom_line() +
  ggthemes::theme_tufte()
ggplotly(x)

