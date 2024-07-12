# read in usage data
# Product: Core = Run My Jobs   | RMJ
# Product: App = Run My Finance | FA
source('ActiveCo Functions.R')
library(data.table)
library(dplyr)
library(httr)
library(jsonlite)
library(lubridate)
library(tidyr)
print(paste0("SaaS_Usage_Upload_to_Bigquery ",Sys.Date()))

api.key <- "ylMKsntx4LU4oqoRRRqrTyua29UEhgwSxMLo6ytYqvyKL4FF3aoUn6AvMCAKeJ1k"


# get a list of all the customers
response.all.clients <- 
  GET("https://contracting.runmyjobs.cloud/api/v1.0/consumption/clients",
      add_headers(accept = 'application/json',
                  `X-Api-Key` = api.key))
fromjson.clients <- fromJSON(content(response.all.clients,'text', encoding = "UTF-8"))
all.clients <- fromjson.clients
rm(response.all.clients,fromjson.clients)


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

# pick up latent Activity API customers
portal.customers <- query.bq(
"
select
distinct
customerid  
from `ContractServer.Activity_Customers`
"
)

sfdc.mapping <- query.bq(
    "
select
    m.*
,case when a.Timezone__c is null then 'UTC' else a.Timezone__c end as TimeZone
from ContractServer.PortalSalesforceAccountMap m
left join skyvia.Account a on m.salesforceAccountId = a.Id
"
  )


portal.ids <- unique(c(customers$rmjPortalId,portal.customers$customerid,all.clients$customerCode))
# remove some bad portal IDs
portal.ids <- portal.ids[which(!grepl("\'",portal.ids))]
portal.ids <- portal.ids[which(!grepl(" ",portal.ids))]
portal.ids <- portal.ids[which(!grepl("/r2w/",portal.ids))]
portal.ids <- portal.ids[which(!is.na(portal.ids))]


usage.list <- list()
usage.list.upload <- list()

for (i in portal.ids) {
  # i <- "john-deere-and-company"  # unique(customers$rmjPortalId)[5]
  # i <- "richemont-international-sa"
  # i <- "new-york-university"
  # i <- "staples-europe-bv"
  # i <- "apple"
  # i <- "asdf"
  print(i)
  client.code <- i
  timezone <- sfdc.mapping$TimeZone[which(sfdc.mapping$rmjPortalId == client.code)]
  
  if(identical(timezone,character(0))){ # if timezone is blank use UTC
    timezone <- "UTC"
  }
  
  response.usage <- 
    GET(paste("https://contracting.runmyjobs.cloud/api/v1.0/consumption",
              client.code,
              start.time,#start.time,
              end.time,
              'HOURLY',sep = "/"),
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
    # convert to local timezone
    # make sure there are no blank times
    consumption <- consumption[which(!grepl("NA",consumption$startMillis,ignore.case = TRUE)),]
    consumption <- consumption[which(!grepl("NA",consumption$endMillis,ignore.case = TRUE)),]
    
    # move the usage into a human readable date
    consumption$human_start <- as_datetime(as.numeric(consumption$startMillis / 1000), tz = timezone)
    consumption$human_end <- as_datetime(as.numeric(consumption$endMillis / 1000), tz = timezone)
    
    # test new data upload format
    # consumption$Date <- floor_date(consumption$human_start)
    # consumption$Date <- as.Date(consumption$Date)
    consumption$Date <- as.Date(consumption$human_start, tz = timezone)
    consumption.upload <- consumption %>%
      group_by(rmjPortalId,environment,Date) %>%
      summarise(jobExecutions = sum(jobExecutions,na.rm = T),.groups = "keep")
    consumption.upload$human_end <- consumption.upload$Date
    
    consumption <- consumption %>%
      group_by(rmjPortalId,environment,Date) %>%
      summarise(jobExecutions = sum(jobExecutions,na.rm = T),
                human_start = min(human_start,na.rm = T),
                human_end = max(human_end,na.rm = T),
                .groups = "keep")
    
    # consumption$Date <- NULL
    
    # if(exists('total_consumption')){ # if total consumption exists, do rbind
    #   total_consumption <- bind_rows(total_consumption,consumption)
    # }else{# else create total_consumption
    #   total_consumption <- consumption
    # }
    usage.list <- append(usage.list, list(consumption))
    usage.list.upload <- append(usage.list.upload, list(consumption.upload))
  }else(warning(paste(i,": No Usage")))
  # remove consumption record, rinse and repeat
  rm(consumption)
  rm(consumption.upload)
  Sys.sleep(1) # sleep for a beat to avoid overloading server
}
# fill in zeros for cases of no job executions
total_consumption <- rbindlist(usage.list, use.names = TRUE, fill = TRUE)
total_consumption.upload <- rbindlist(usage.list.upload, use.names = TRUE, fill = TRUE)

total_consumption$jobExecutions[which(is.na(total_consumption$jobExecutions))] <- 0
total_consumption.upload$jobExecutions[which(is.na(total_consumption.upload$jobExecutions))] <- 0
# usage <- read.csv("2022-03-09-billing-usage-1293840000000-1703980800000.csv",
#                   stringsAsFactors = FALSE)

# View(total_consumption[which(tolower(total_consumption$environment) == 'redwoodplatform:rncpimutrap12:8443' & total_consumption$Date >= '2024-01-01'),])

# service.auth.file <- "activeco-d1b25a375754.json"
# bq_auth(path = service.auth.file)
# gs4_auth(path = service.auth.file)
# billing <-  "activeco"

# remove list cols
customers$billingAddress <- NULL
customers$owner <- NULL
customers$shippingAddress <- NULL

# upload the raw data to bigquery
print("Uploading to Bigquery")
data.set.bq <- bq_dataset(billing,"ContractServer") # create redwood dataset

# make a list of all known portalIDs to upload
known.ids <- query.bq("select distinct rmjPortalId from ContractServer.SaaS_Usage")
all.portalIds <- unique(c(portal.ids,known.ids$rmjPortalId))
all.portalIds <- data.frame(rmjportalid = all.portalIds)

# set the BQ table and upload the data
upload.to.bigquery(total_consumption,"ContractServer","SaaS_Usage")
# upload.to.bigquery(total_consumption.upload,"ContractServer","SaaS_Usage_New")
upload.to.bigquery(customers,"ContractServer","Customers")
upload.to.bigquery(all.portalIds,"ContractServer","AllPortalIds")

# upload the usage contracts
rm(list = ls())
source('Contracts Server to Bigquery.R')
