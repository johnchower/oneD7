devtools::load_all()
library(dplyr)
library(plotly)

glootility::connect_to_redshift()

CeDAR_users <- read.csv(file = '/Users/johnhower/Data/CeDAR_2016_YTD_Active_Users.csv'
                        , stringsAsFactors = F) %>%
  {.$User_Dimensions_ID}  

CeDAR_retention <- calculateWeeklyRetention(users = CeDAR_users) %>%
  mutate(user_group = 'CeDAR') %>%
  filter(relative_session_week <= 50)
avg_retention <- calculateWeeklyRetention() %>%
  mutate(user_group = 'All') %>%
  filter(relative_session_week <= 50)


retentionData2 <- rbind(CeDAR_retention, avg_retention)
retentionDataLong <- tidyr::gather(data = retentionData2
                                   , key = 'variable'
                                   , value = 'value'
                                   , -relative_session_week
                                   , -user_group)

# Plot long-term retention numbers for each cluster
x <- retentionData2 %>%
  filter(relative_session_week > 0) %>%
  mutate(user_group = as.character(user_group)
         , pct_active = round(100*pct_active, 1)) %>%
  ggplot(aes(x=relative_session_week
             , y=pct_active
             , color = user_group
             , group = user_group)) +
  geom_line() +
  labs(x = "Weeks Since Signup", y = "Percent Weekly Active Users") +
  theme(legend.position='none'
        , axis.text=element_text(size = 24)
        , axis.title=element_text(size = 32)) +
  ggthemes::theme_tufte()

p <- plotly_build(x)

p$x$data[[1]]$text <- gsub(pattern = 'relative_session_week'
                           , replacement = 'Weeks Since Signup'
                           , x = p$x$data[[1]]$text)
p$x$data[[1]]$text <- gsub(pattern = 'pct_active'
                           , replacement = 'Percent Weekly Active Users'
                           , x = p$x$data[[1]]$text)
p$x$data[[1]]$text <- gsub(pattern = 'pct_active'
                           , replacement = 'Percent Weekly Active Users'
                           , x = p$x$data[[1]]$text)
p$x$data[[1]]$text <- gsub(pattern = '<br>user_group: All'
                           , replacement = ''
                           , x = p$x$data[[1]]$text)
p$x$data[[1]]$text <- gsub(pattern = '$'
                           , replacement = '%<br>User Group: All'
                           , x = p$x$data[[1]]$text)

p$x$data[[2]]$text <- gsub(pattern = 'relative_session_week'
                           , replacement = 'Weeks Since Signup'
                           , x = p$x$data[[2]]$text)
p$x$data[[2]]$text <- gsub(pattern = 'pct_active'
                           , replacement = 'Percent Weekly Active Users'
                           , x = p$x$data[[2]]$text)
p$x$data[[2]]$text <- gsub(pattern = 'pct_active'
                           , replacement = 'Percent Weekly Active Users'
                           , x = p$x$data[[2]]$text)
p$x$data[[2]]$text <- gsub(pattern = '<br>user_group: CeDAR'
                           , replacement = ''
                           , x = p$x$data[[2]]$text)
p$x$data[[2]]$text <- gsub(pattern = '$'
                           , replacement = '%<br>User Group: CeDAR'
                           , x = p$x$data[[2]]$text)

p$x$layout$hovermode <- 'compare'
p

# Save plot as html
htmlwidgets::saveWidget(p, '/Users/johnhower/Data/CeDAR_retention_rate.html')

RPostgreSQL::dbDisconnect(conn = redshift_connection$con)
