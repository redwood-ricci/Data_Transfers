# read in usage data
# Product: Core = Run My Jobs   | RMJ
# Product: App = Run My Finance | FA
source('ActiveCo Functions.R')
library(dplyr)
library(httr)
library(jsonlite)
library(lubridate)
library(tidyr)
print(paste0("SaaS_Usage_Upload_to_Bigquery ",Sys.Date()))

api.key <- "ylMKsntx4LU4oqoRRRqrTyua29UEhgwSxMLo6ytYqvyKL4FF3aoUn6AvMCAKeJ1k"


# get a list of all the customers
response.all.customers <- 
  GET("https://contracting.runmyjobs.cloud/api/v1.0/customers",
      add_headers(accept = 'application/json',
                  `X-Api-Key` = api.key))

fromjson.customers <- fromJSON(content(response.all.customers,'text', encoding = "UTF-8"))
customers <- as.data.frame(fromjson.customers$customers)
rm(response.all.customers,fromjson.customers)

# get usage for all time
start.time <- format(as.numeric(as.POSIXct('2000-01-01')) * 1000,scientific = FALSE) # *1000 is to convert to miliseconds
end.time <- format(as.numeric(as.POSIXct('2040-01-31')) * 1000,scientific = FALSE)

for (i in unique(customers$rmjPortalId)) {
  # i <- "john-deere-and-company"  # unique(customers$rmjPortalId)[5]
  # i <- "richemont-international-sa"
  # i <- "avaya"
  print(i)
  client.code <- i
  response.usage <- 
    GET(paste("https://contracting.runmyjobs.cloud/api/v1.0/consumption",
              client.code,
              start.time,#start.time,
              end.time,
              'DAILY',sep = "/"),
        add_headers(accept = 'application/json',
                    `X-Api-Key` = api.key))
  
  usage <- fromJSON(content(response.usage,'text', encoding = "UTF-8"),flatten = TRUE)
  rm(response.usage)
  # flatten return file
  consumption <- as.data.frame(usage$periods)
  rm(usage)
  if(nrow(consumption)> 0){
    consumption$rmjPortalId <- i
    # pivot response longer
    names(consumption) <- gsub("environments\\.","",names(consumption))
    # drop the totals because can be made by totaling envitonments
    consumption$jobExecutions <- NULL
    consumption <- consumption %>%
      pivot_longer(cols = names(consumption)[which(!(names(consumption) %in%c('startMillis','endMillis','rmjPortalId')))]
                   , names_to = "environment", values_to = "jobExecutions")
    
    if(exists('total_consumption')){ # if total consumption exists, do rbind
      total_consumption <- bind_rows(total_consumption,consumption)
    }else{# else create total_consumption
      total_consumption <- consumption
    }
  }else(warning(paste(i,": No Usage")))
  # remove consumption record, rinse and repeat
  rm(consumption)
}
# fill in zeros for cases of no job executions
total_consumption$jobExecutions[which(is.na(total_consumption$jobExecutions))] <- 0
# usage <- read.csv("2022-03-09-billing-usage-1293840000000-1703980800000.csv",
#                   stringsAsFactors = FALSE)


# service.auth.file <- "activeco-d1b25a375754.json"
# bq_auth(path = service.auth.file)
# gs4_auth(path = service.auth.file)
# billing <-  "activeco"

# make sure there are no blank times
total_consumption <- total_consumption[which(!grepl("NA",total_consumption$startMillis,ignore.case = TRUE)),]
total_consumption <- total_consumption[which(!grepl("NA",total_consumption$endMillis,ignore.case = TRUE)),]

# move the usage into a human readable date
total_consumption$human_start <- as_datetime(as.numeric(total_consumption$startMillis / 1000))
total_consumption$human_end <- as_datetime(as.numeric(total_consumption$endMillis / 1000))

# remove list cols
customers$billingAddress <- NULL
customers$owner <- NULL
customers$shippingAddress <- NULL

# upload the raw data to bigquery
print("Uploading to Bigquery")
data.set.bq <- bq_dataset(billing,"ContractServer") # create redwood dataset

# set the BQ table and upload the data
upload.to.bigquery(total_consumption,"ContractServer","SaaS_Usage")
upload.to.bigquery(customers,"ContractServer","Customers")

# upload the usage contracts
rm(list = ls())
source('Contracts Server to Bigquery.R')
