library(ggplot2)
library(dplyr)
library(plotly)

# Connect to Redshift and create temporary tables user_flash_cat and pa_flash_cat.
glootility::connect_to_redshift()
RPostgreSQL::dbGetQuery(conn = redshift_connection$con
                        , statement = glootility::query_pa_flash_cat)
RPostgreSQL::dbGetQuery(conn = redshift_connection$con
                        , statement = glootility::query_user_flash_cat)
# Calculate the platform action distribution for all users, in their first hour
# on the platform.
allUserPADist <- 
  oneD7::calculatePADist() 

sampleUserPADist <- allUserPADist %>%
  slice(sample(1:nrow(.), size = 1000))

# Cluster all users according to their hour-1 platform action distribution.
allUserClust <- clusterUsers(allUserPADist)
sampleUserClust <- clusterUsers(sampleUserPADist)

K <- 4

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
.env$view(squashPADistList(aggPADistList))

# Calculate long-term retention numbers for each cluster. 
retentionList <- clustApply(hclustObject=allUserClust
                            , num_clusters = K
                            , FUN = calculateWeeklyRetention)

# Plot long-term retention numbers for each cluster
retentionData <- squashRetentionList(retentionList)
x <- retentionData %>%
  filter(relative_session_week > 0) %>%
  mutate(cluster = as.character(cluster)) %>%
  ggplot(aes(x=relative_session_week, y=pct_active, color = cluster, group = cluster)) +
  geom_line() 
ggplotly(x)
  
RPostgreSQL::dbDisconnect(conn = redshift_connection$con)
