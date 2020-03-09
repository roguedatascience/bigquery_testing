library(tidyverse)
library(lubridate)
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

### Bigquery console
# https://console.cloud.google.com/bigquery

# Example query


#############################################################################
# Setup and test query
#############################################################################

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




#############################################################################
# Setup and test query
#############################################################################

# id
project_id <-
    'reddit-data-270419' # put your project ID here

# Query
sql_string <-
    '
        SELECT author, subreddit, count(*) as cnt, sum(score) as tot_score 
        FROM `fh-bigquery.reddit_comments.2018_09` 
        group by author, subreddit;
    '

# Send query
tb_search <-
    bq_project_query(x = project_id,
                     query = sql_string)

temp_df <-
    bq_table_download(tb_search)

write_csv(temp_df, 'reddit_map_2018_09.csv')




#############################################################################
# Pull full data
#############################################################################


# id
project_id <-
    'reddit-data-270419' # put your project ID here

reddit_tables <-
  c(
    
    2005:2014 %>%
      as.character(),
    
    seq(as_date('2015-01-01'),
        as_date('2019-09-01'),
        by = 'months') %>%
      as.character() %>%
      str_remove_all('-[0-9]{1,2}$') %>%
      str_replace_all('-', '_')
  
    )


for(i in 1:length(reddit_tables)) {
    
    # i = 1
    
    message('\n\n\n', i)
    
    # Query
    temp_sql_string <-
        str_c(
            'SELECT author, subreddit, count(*) as cnt, sum(score) as tot_score ',
            'FROM `fh-bigquery.reddit_comments.',
            reddit_tables[i], '` ',
            'group by author, subreddit;')
    
    # Send query
    temp_search <-
        bq_project_query(x = project_id,
                         query = temp_sql_string)
    
    temp_df <- NULL
    cntr <- 0
    
    while( is.null(temp_df)) {
        
        temp_df <-
            tryCatch(bq_table_download(temp_search),
                     
                     warning = function(w) {
                         NULL;
                     },
                     
                     error = function(e) {
                         NULL;
                     })
        
        if( is.null(temp_df) ) {
            
            message('Reseting...')
            Sys.sleep(500)
            
            cntr <- cntr + 1
            
        }
        
        if( cntr >= 3 ) {
            break
        }
        
    }
    
    write_csv(temp_df,
              str_c('reddit_map_', reddit_tables[i], '.csv'))
    
    ### Clean up temp files
    # Not doing this step will cause oauth to fail and possibly fill hard drive
    tempdir() %>%
      list.files(pattern = 'bq-[[:xdigit:]]+\\.json', full.names = TRUE) %>%
      map(file.remove)
            
}
