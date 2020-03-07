library(tidyverse)
library(bigrquery)

# https://console.cloud.google.com/home/
# project name: reddit-data
# Project ID: reddit-data-270419
# Project number: 315840168133

### querying pushshift from bigquery
# https://pushshift.io/using-bigquery-with-reddit-data/

### Show hidden files on a Mac
# https://www.macworld.co.uk/how-to/mac-software/show-hidden-files-mac-3520878/
# cmd + shift + .


# Example query

#billing <- bq_test_project() # replace this with your project ID 
project_id <-
    'reddit-data-270419' # put your project ID here

#sql <- "SELECT year, month, day, weight_pounds FROM `publicdata.samples.natality` LIMIT"
sql_string <-
    "SELECT author FROM `fh-bigquery.reddit_comments.2007` LIMIT 1"

# No need to provide bq_dataset(project, dataset)
#   if already included in query (e.g. fh-bigquery.reddit_comments.2007)
# Must provide destination_table parametter with big queries (> 128 mb)

# Will open a browser for authorization
#   Stored: /Users/juan/.R/gargle
tb_search <-
    bq_project_query(x = project_id,
                     query = sql_string)

temp_df <-
    bq_table_download(tb_search)





# Execute the query and store the result

project_id <-
    'reddit-data-270419'

sql_string <-
    '
        SELECT author, subreddit, count(*) as cnt, sum(score) as tot_score 
        FROM `fh-bigquery.reddit_comments.2007` 
        group by author, subreddit 
        LIMIT 100;
    '

query_results <-
    bq_project_query(x = project_id,
                     query = sql_string)

