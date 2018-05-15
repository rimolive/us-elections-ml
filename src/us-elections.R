library(dplyr)
library(sparklyr)

DF_LOCATION <- "https://raw.githubusercontent.com/rimolive/us-elections/master/data/us-elections.csv"

sc <- spark_connect(master = paste("spark://", Sys.getenv("OSHINKO_CLUSTER_NAME"), ":7077", sep=""))

us.elections_tbl <- copy_to(sc, read.csv(DF_LOCATION))

elections_tbl <- us.elections_tbl %>%
  group_by(state_name, party) %>%
  summarise(total_2008 = sum(total_votes_2008),
            total_2012 = sum(total_votes_2012),
            total_2016 = sum(total_votes_2016)) %>%
  arrange(state_name) %>%
  collect()

elections.2008_tbl <- elections_tbl %>%
  select(state_name, party, total_2008)

elections.2012_tbl <- elections_tbl %>%
  select(state_name, party, total_2012)

elections.2016_tbl <- elections_tbl %>%
  select(state_name, party, total_2016)

