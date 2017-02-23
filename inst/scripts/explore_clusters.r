library(dplyr)
glootility::connect_to_redshift()

# Load cluster data set.
load(file = '/Users/johnhower/Data/allUserClust_20170213.rda')

# Convert allUserClust to a data frame that matches user_ids to their
# corresponding clusters.
x <- allUserClust %>%
  clustApply(num_clusters = 6
             , FUN = function(x) x) %>%
  lapply(FUN = function(x){
           data.frame(cluster = x$varCombo
                      , user_id = x$result)
         }) %>%
  {plyr::ldply(.)} %>%
  {dplyr::select(., user_id, cluster)}

# Grab data set of user-to-champion connections
query_uccb <- 
  paste0(
    "SELECT uccb.user_id AS user_id "
    , ", cd.name AS champion_name "
    , "FROM user_connected_to_champion_bridges uccb "
    , "left join champion_dimensions cd "
    , "ON uccb.champion_id=cd.id ")
user_connected_to_champion_bridges  <- 
  RPostgreSQL::dbGetQuery(conn = redshift_connection$con
                          , statement = query_uccb)

# Find out which champions are represented in each cluster.
left_join(user_connected_to_champion_bridges, x
                 , by = "user_id") %>%
  filter(!is.na(champion_name)) %>%
  group_by(cluster, champion_name) %>%
  summarise(count_users = length(unique(user_id))) %>%
  {.env$view(.)}

