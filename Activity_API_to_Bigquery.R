# The purpose of this program is to query the billing-test API
# and return a data frame of the usage by year for all clients
source('ActiveCo Functions.R')
# a change for git
# ylMKsntx4LU4oqoRRRqrTyua29UEhgwSxMLo6ytYqvyKL4FF3aoUn6AvMCAKeJ1k
print(paste0("Activity_API_to_Bigquery ",Sys.Date()))
# library(httr2)
library(dplyr)
library(httr)
library(jsonlite)
library(lubridate)

rmj.rmf.apis <- c('runmyjobs','runmyfinance')
activity.api.key <- "delsNHbxSmZAbFZCDyBUAkqZsiyQFvEwu9gOUtgMvE19Jn29IDLBFahsPYkFXsL"

for (run_my in rmj.rmf.apis) {
  # run_my <- 'runmyfinance'
  # run_my <- 'runmyjobs'
  # contracting.api.key <- "ylMKsntx4LU4oqoRRRqrTyua29UEhgwSxMLo6ytYqvyKL4FF3aoUn6AvMCAKeJ1k"
  
  response.activity <- GET(paste0("https://portal.",run_my,".cloud/api/monitoring/activity-types"),
                           add_headers(accept = "application/json",
                                       `X-API-TOKEN` = activity.api.key))
  
  fromjson.activity <- fromJSON(content(response.activity,'text', encoding = "UTF-8"),flatten = TRUE)
  fromjson.activity$type <- run_my
  # 
  response.customers <- GET(paste0("https://portal.",run_my,".cloud/api/monitoring/customers"),
                            add_headers(accept = "application/json",
                                        `X-API-TOKEN` = activity.api.key))
  
  fromjson.customers <- fromJSON(content(response.customers,'text', encoding = "UTF-8"),flatten = TRUE)
  fromjson.customers$type <- run_my
  
  activity.years <- c(2021,2022,2023) # add 2023 to this on Jan 1 2023. 
  
  for (i in unique(fromjson.customers$customerid)) {
    # i <- "john-deere-and-company"  # unique(customers$rmjPortalId)[5]
    # i <- "richemont-international-sa"
    print(i)
    # for each customer
    # run through unique activities and pull them from the API
    for (a in unique(fromjson.activity$name)) {
      for(y in unique(activity.years)){
        print(paste0(a,":",y))
        # a <- unique(fromjson.activity$name)[4]
        api.a <- gsub(" ","%20",a)
        
        response.activity <- 
          GET(paste0("https://portal.",run_my,".cloud/api/monitoring/activities/",
                     i,"/", # customer ID
                     api.a,"/",
                     y#,"/",
                     # {month},"/",
                     # {day}
          ),
          add_headers(accept = 'application/json',
                      `X-API-TOKEN` = activity.api.key))
        
        usage <- fromJSON(content(response.activity,'text', encoding = "UTF-8"),flatten = TRUE)
        rm(response.activity)
        usage$activity <- a
        usage$rmjPortalId <- i
        usage$year <- y
        usage$type <- run_my
        if(exists("customer.activity")){
          customer.activity <- bind_rows(customer.activity,usage)
        }else{
          customer.activity <- usage
        }
      } # close year
    } # close activity
  } # close customer
  
  if(exists("customer.master")){
    customer.master <- bind_rows(customer.master,fromjson.customers)
    activity.master <- bind_rows(activity.master,fromjson.activity)
  }else{
    customer.master <- fromjson.customers
    activity.master <- fromjson.activity
  }
  
} # close run_my portal type

c2 <- pivot_longer(customer.activity,
                   cols = starts_with("data."),
                   names_to = "string.date",
                   names_prefix = "data\\.",
                   values_to = "count",
                   values_drop_na = FALSE)

c2$month <- as.Date(paste0(c2$string.date,"-",c2$year,"-1"),format = '%b-%Y-%d')
c2[,c("string.date","year")] <- NULL

c2 <- c2[order(c2$rmjPortalId,c2$envid,c2$activity,c2$month),]

upload.to.bigquery(c2,'ContractServer','Activity')
upload.to.bigquery(customer.master,'ContractServer','Activity_Customers')

source("Upload Usage to SFDC.R")
