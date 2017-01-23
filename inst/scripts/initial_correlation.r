library(dplyr)

# Connect to Redshift and create temporary tables user_flash_cat and pa_flash_cat.
glootility::connect_to_redshift()
RPostgreSQL::dbGetQuery(conn = redshift_connection$con
                        , statement = glootility::query_pa_flash_cat)
RPostgreSQL::dbGetQuery(conn = redshift_connection$con
                        , statement = glootility::query_user_flash_cat)

# Take a sample of the set of all users on the platform.
sampleUserSet <- 
  RPostgreSQL::dbGetQuery(conn = redshift_connection$con
                          , statement = "select user_id as id from user_flash_cat")  %>%
  {slice(., sample(1:nrow(.), size = 500))} %>%
  {.$id}

# Calculate the platform action distribution for the sample set, in their first hour
# on the platform.
sampleUserPADist <- 
  oneD7::calculatePADist(users = sampleUserSet)

sampleUserClust <- clusterUsers(sampleUserPADist)
plot(sampleUserClust) #Use the dendrogram to choose a suitable height.
H <- 7

# Calculate the platform action distribution for all users, in their first hour
# on the platform.
allUserPADist <- 
  oneD7::calculatePADist()

# Cluster all users according to their hour-1 platform action distribution.
allUserClust <- clusterUsers(allUserPADist)

# Calculate aggregate platform action distributions for each cluster.
aggPADistList <- clustApply(
  hclustObject=allUserClust
  , height = H
  , FUN = function(u){
      dplyr::select(calculatePADist(u, agg = T)
                    , flash_report_category, pct_platform_actions)
)

# Calculate long-term retention numbers for each cluster. 
retentionList <- clustApply(hclustObject=allUserClust
                            , height = H
                            , FUN = calculateWeeklyRetention)

# Plot long-term retention numbers for each cluster

RPostgreSQL::dbDisconnect(conn = redshift_connection$con)
