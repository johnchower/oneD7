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

# Cluster all users according to their hour-1 platform action distribution.
allUserClust <- clusterUsers(allUserPADist)
plot(allUserClust)

# Calculate aggregate platform action distributions for each cluster.
aggPADistList <- clustApply(
  hclustObject=allUserClust
  , height = 4
  , FUN = function(u){
      dplyr::select(calculatePADist(u, agg = T)
                    , flash_report_category, pct_platform_actions)
)

# Calculate long-term retention numbers for each cluster. 
retentionList <- clustApply(hclustObject=allUserClust
                            , height = 4
                            , FUN = calculateWeeklyRetention)

# Plot long-term retention numbers for each cluster

RPostgreSQL::dbDisconnect(conn = redshift_connection$con)
