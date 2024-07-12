# The purpose of this program is to query the billing-test API
# and return a data frame of the usage by year for all clients
source('ActiveCo Functions.R')
#source('ActiveCo Functions.R')
# ylMKsntx4LU4oqoRRRqrTyua29UEhgwSxMLo6ytYqvyKL4FF3aoUn6AvMCAKeJ1k

# library(httr2)
print(paste0("Contracts Server to Bigquery ",Sys.Date()))
library(dplyr)
library(httr)
library(jsonlite)
library(lubridate)

# get usage for 2021
start.time <- format(as.numeric(as.POSIXct('2021-01-01')) * 1000,scientific = FALSE) # *1000 is to convert to miliseconds
end.time <- format(as.numeric(as.POSIXct('2021-12-31')) * 1000,scientific = FALSE)

response.products <- GET("https://contracting.runmyjobs.cloud/api/v1.0/products",
                         add_headers(accept = "application/json",
                                     `X-Api-Key` = "ylMKsntx4LU4oqoRRRqrTyua29UEhgwSxMLo6ytYqvyKL4FF3aoUn6AvMCAKeJ1k"))
fromjson.products <- fromJSON(content(response.products,'text', encoding = "UTF-8"),flatten = TRUE)
products <- as.data.frame(fromjson.products$products)
rm(fromjson.products,response.products)

for (p in 1:nrow(products)) {
  # p <- 1
  product_packages <- as.data.frame(products$packages[p])
  product_packages$ProductId <- products$id[p]
  if(!exists('packages')){
    packages <- product_packages
  }else{
    packages <- rbind(packages,product_packages)
  }
}

# remove list variables
products$contractItemTypes <- NULL
products$packages <- NULL
packages$items <- NULL

# upload to bigquery
names(products) <- clean.names(names(products))
names(packages) <- clean.names(names(packages))

upload.to.bigquery(products,'ContractServer','products')
upload.to.bigquery(packages,'ContractServer','product_packages')


# get contracts
response.contract <- GET("https://contracting.runmyjobs.cloud/api/v1.0/contracts",
                         add_headers(accept = "application/json",
                                     `X-Api-Key` = "ylMKsntx4LU4oqoRRRqrTyua29UEhgwSxMLo6ytYqvyKL4FF3aoUn6AvMCAKeJ1k")
)

fromjson.contracts <- fromJSON(content(response.contract,'text', encoding = "UTF-8"),flatten = TRUE)
contracts <- as.data.frame(fromjson.contracts$contracts)
contracts[,grepl("opportunity\\.",names(contracts))] <- NULL
rm(fromjson.contracts,response.contract)

# fill 1900-01-01 for contracts without a start date or provision date
# contracts$term.startDateInclusive.year[which(is.na(contracts$term.startDateInclusive.year))] <- '1900'
# contracts$term.startDateInclusive.month[which(is.na(contracts$term.startDateInclusive.month))] <- "01"
# contracts$term.startDateInclusive.dayOfMonth[which(is.na(contracts$term.startDateInclusive.dayOfMonth))] <- "01"


contracts$provisionDate.year[which(is.na(contracts$provisionDate.year))] <- '1900'
contracts$provisionDate.month[which(is.na(contracts$provisionDate.month))] <- "01"
contracts$provisionDate.dayOfMonth[which(is.na(contracts$provisionDate.dayOfMonth))] <- "01"


# contracts$human.start <- as.Date(paste(contracts$term.startDateInclusive.year,
#                                               contracts$term.startDateInclusive.month,
#                                               contracts$term.startDateInclusive.dayOfMonth
#                                               ,sep = "-"))
# contracts$human.end <- as.Date(paste(contracts$term.endDateExclusive.year,
#                                             contracts$term.endDateExclusive.month,
#                                             contracts$term.endDateExclusive.dayOfMonth
#                                             ,sep = "-"))

contracts$provision.date <- as.Date(paste(contracts$provisionDate.year,
                                       contracts$provisionDate.month,
                                       contracts$provisionDate.dayOfMonth
                                       ,sep = "-"))

contracts$DeploymentType <- NA
contracts$human.start <- as.Date('1900-01-01')
contracts$human.end <- as.Date('1900-01-01')

for (o in 1:nrow(contracts)) { # this requires opportunity ID to be a unique field
  # o <- 1
  opp.id <- contracts$opportunityId[o]
  contract.id <- contracts$id[o]
  customerId <- contracts$customerId[o]
  rmj.portal.id <- contracts$customer.rmjPortalId[o]
  print(paste0("Contract: ",rmj.portal.id))
  
  contract.periods <- as.data.frame(contracts$contractPeriods[o])
  contract.periods$opportunityId <- opp.id
  contract.periods$human.start <- as.Date(paste(contract.periods$startDate.year,
                                                contract.periods$startDate.month,
                                                contract.periods$startDate.dayOfMonth
                                                ,sep = "-"))
  contract.periods$human.end <- as.Date(paste(contract.periods$endDate.year,
                                              contract.periods$endDate.month,
                                              contract.periods$endDate.dayOfMonth
                                              ,sep = "-"))
  
  contract.periods$usage_period <- contracts$usagePeriod[o]
  
  # add the contract deployment type to the contract object this is not a usage period
  contracts$DeploymentType[o] <- paste(unique(contracts$locationCounts[[o]]$deploymentType),sep = ',')
  contracts$human.start[o] <- as.Date(min(contract.periods$human.start, na.rm = T))
  contracts$human.end[o]   <- as.Date(max(contract.periods$human.end, na.rm = T))
  
  # make which contract.periods are active today
  contract.periods$active_today <- FALSE
  contract.periods$active_today[which(contract.periods$human.start<=Sys.Date()
                         & contract.periods$human.end > Sys.Date())] <- TRUE
  
  
  # rbind everyone's contract periods into one file to upload
  if(!exists('periods')){
    periods <- contract.periods
  }else{
    periods <- bind_rows(periods,contract.periods)
  }
  
  # separate the usage fee information for calculating job teir pricing
  for(p in 1:nrow(contract.periods)){
    contract.period.id <- contract.periods$id[p]
    print(paste0("Period: ",contract.period.id))
    tiers <- as.data.frame(contract.periods$jobTiers[p])
    tiers$ContractPeriodId <- contract.period.id
    tiers$opportunityId <- opp.id
  }
  
  # rbind all contract tiers into one data frame for upload
  if(!exists('job.tiers')){
    job.tiers <- tiers
  }else{
    job.tiers <- bind_rows(job.tiers,tiers)
  }
  
# get the contract overage amounts for each contract
# contract.id <- 11257
contract.id <- contracts$id[o]
print(paste0("Usage ContractID: ",contract.id))
  response.contract.usage <- GET(paste0("https://contracting.runmyjobs.cloud/api/v1.0/contract/",
                                        contract.id,"/usage-periods"),
                                 add_headers(accept = "application/json",
                                             `X-Api-Key` = "ylMKsntx4LU4oqoRRRqrTyua29UEhgwSxMLo6ytYqvyKL4FF3aoUn6AvMCAKeJ1k")
  )
  
  fromjson.contract.usage <- fromJSON(content(response.contract.usage,'text', encoding = "UTF-8"),flatten = TRUE)
  usage.period <- as.data.frame(fromjson.contract.usage$usagePeriods)
  if(nrow(usage.period >0)){
  
  usage.period$opportunityId <- opp.id
  usage.period$contract.id   <- contract.id
  usage.period$customerId    <- customerId
  usage.period$rmj.portal.id <- rmj.portal.id
  
  usage.period$human.start <- as.Date(paste(usage.period$startDate.year,
                                            usage.period$startDate.month,
                                            usage.period$startDate.dayOfMonth
                                                ,sep = "-"))
  usage.period$human.end <- as.Date(paste(usage.period$endDate.year,
                                              usage.period$endDate.month,
                                              usage.period$endDate.dayOfMonth
                                              ,sep = "-"))

  usage.period$id <- paste(usage.period$contract.id,usage.period$customerId,usage.period$human.start,usage.period$human.end,sep = "-")
  
  if(!exists('usage.periods')){
    usage.periods <- usage.period
  }else{
    usage.periods <- bind_rows(usage.periods,usage.period)
  }
  } # end usage period >0
  
}

contracts$provision.date[which(contracts$provision.date == '1900-01-01')] <- NA
contracts$human.start[which(contracts$human.start == '1900-01-01')] <- NA
contracts$human.end[which(contracts$human.end == '1900-01-01')] <- NA

contracts[,c("locationCounts","contractItems","contractPeriods")] <- NULL

periods$jobTiers <- NULL

names(contracts) <- clean.names(names(contracts))
names(periods) <- clean.names(names(periods))
names(job.tiers) <- clean.names(names(job.tiers))
names(usage.periods) <- clean.names(names(usage.periods))
job.tiers <- job.tiers[order(job.tiers$opportunityId,job.tiers$start),]

upload.to.bigquery(contracts,'ContractServer','Contracts')
upload.to.bigquery(periods,'ContractServer','Contract_Periods')
upload.to.bigquery(job.tiers,'ContractServer','Contract_Job_Tiers')
upload.to.bigquery(usage.periods,'ContractServer','Usage_Periods')

# write_sheet(contracts,"https://docs.google.com/spreadsheets/d/1Sjil00dR-gGuW_VkkbIpqMKJqeAP9RPuk0aqJcRNb0g/edit#gid=1988424249",
#             sheet = 'Contracts Export')
# write_sheet(periods,"https://docs.google.com/spreadsheets/d/1Sjil00dR-gGuW_VkkbIpqMKJqeAP9RPuk0aqJcRNb0g/edit#gid=1988424249",
#             sheet = 'Contract Periods')
rm(list = ls())
# can comment / uncomment here to skip activity API upload because it's slow
# source('Activity_API_to_Bigquery.R')
source("Upload Usage to SFDC.R")



