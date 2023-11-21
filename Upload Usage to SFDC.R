start.time <- Sys.time()
print(paste0("Starting at: ",start.time))

source('ActiveCo Functions.R')
library(tidyr)
library(DBI)
library(RMySQL)

# library(bigQueryR)

# on prem customer examples:
# Border State Industries, Inc.	0013t00002LzTIxAAN	RunMyJobs	0063t000010lGe3AAE
print(paste0("Upload Usage to SFDC ",Sys.Date()))

print('Uploading Usage Periods')

usage <- query.bq("
select 
c.opportunityId               as Opportunity__c,
up.rmj_portal_id              as RMJ_Portal_Id__c,
up.overage                    as Overage__c,
up.usage                      as Usage__c,
up.expectedEndCredit          as Expected_End_Credit__c,
up.expectedEndUsage           as Expected_End_Usage__c,
up.contract_id                as Contract_Server_Contract_ID__c,
c.customer_id                 as Customer_ID__c,
up.human_start                as Usage_Period_Start_Date__c,
up.human_end                  as Usage_Period_End_Date__c,
up.expectedOverage            as Expected_Overage__c,
up.expectedOverageAmount      as Expected_Overage_Amount__c,
c.usagePeriod                 as Usage_Period__c,
c.human_start                 as Contract_Start_Date__c,
c.human_end                   as Contract_End_Date__c,
up.overageAmount              as Overage_Amount__c,
up.id                         as R_Program_Id__c,
cast(null as string)          as Type__c,
up.newCredit + up.startCredit as Current_Allotted_Usage__c,
c.DeploymentType              as Deployment_Type__c,
p.displayName                 as Product__c
from `ContractServer.Usage_Periods` up
left join `ContractServer.Contracts` c on c.id = up.contract_id
left join `ContractServer.products` p on c.productId = p.id
                  ")
################### questions for Jan Dirk #####################
# why do all the procuts show RMJ? Product ID 102
# is alloted usage = new credit + start credit?

SFDC.usage <- query.bq("select * from `skyvia.Usage__c`")

# table(SFDC.usage$R_Program_Id__c %in% usage$R_Program_Id__c)
# View(SFDC.usage[which(!(SFDC.usage$R_Program_Id__c %in% usage$R_Program_Id__c)),])
sf_auth(cache = ".httr-oauth-salesforcer-asci")

delete.records <- SFDC.usage[which(!(SFDC.usage$R_Program_Id__c %in% usage$R_Program_Id__c)),]

if(nrow(delete.records) > 0){
print(paste0(Sys.Date()," Deleting Records: ",nrow(delete.records)))
x <- sf_delete(
  delete.records$Id,
  "Usage__c"
)}

update.cols <- c(
"Opportunity__c", # 
"RMJ_Portal_Id__c", #
"Overage__c", # 
"Usage__c", # 
"Expected_End_Credit__c", # 
"Expected_End_Usage__c", # 
"Contract_Server_Contract_ID__c", # 
"Customer_ID__c", # 
"Usage_Period_Start_Date__c", #
"Usage_Period_End_Date__c", # 
"Expected_Overage__c", # 
"Expected_Overage_Amount__c", # 
"Usage_Period__c", # 
"Contract_Start_Date__c", # 
"Contract_End_Date__c", #
"Overage_Amount__c", #
"Type__c", # can push blank value
"R_Program_Id__c",
"Current_Allotted_Usage__c",
"Deployment_Type__c",
"Product__c"
)

# compare and see which records need updating
print('Uploading Usage Periods to Salesforce')
usage.upsert <- sf_upsert(usage,
          "Usage__c",
          verbose = FALSE,
          api_type = "Bulk 2.0",
          external_id_fieldname = "R_Program_Id__c")

table(usage.upsert$sf__Error)

print('Getting Daily Usage')

og.account.usage <- query.bq("select distinct Id,R_Program_Id__c from `skyvia.Daily_Usage__c`")

### This commented out portion includes inactive contracts

# account.usage <- query.bq(
#   paste0(
#     "
# select
# -- a.CurrencyIsoCode,
# u.rmjPortalId as RMJ_Portal_Id__c,
# environment as Environment__c,
# jobExecutions as Job_Executions__c,
# cast(human_start as Date) as Date__c,
# 'RMJ' as Product__c,
# c.salesforceAccountId as Account__c,
# contracts.opportunityId as Opportunity__c,
# 
# from activeco.ContractServer.SaaS_Usage u
# left join `ContractServer.Customers` c on u.rmjPortalId = c.rmjPortalId
# left join(SELECT customer_salesforceAccountId, opportunityId, human_end
# FROM (
#   SELECT customer_salesforceAccountId, opportunityId, human_end,
#     ROW_NUMBER() OVER (PARTITION BY customer_salesforceAccountId ORDER BY human_end DESC) as rn
#   FROM `ContractServer.Contracts`
# )
# WHERE rn = 1) contracts on c.salesforceAccountId = contracts.customer_salesforceAccountId
# where date(human_start) >= DATE_SUB(DATE_TRUNC(DATE(CURRENT_DATE()), MONTH), INTERVAL 3 YEAR);
#     "
#   ))

####### ^^^^^^^^^^^ Inactive Contracts included

account.usage <- query.bq(
  paste0(
    "
select
-- a.CurrencyIsoCode,
u.rmjPortalId as RMJ_Portal_Id__c,
environment as Environment__c,
jobExecutions as Job_Executions__c,
cast(human_start as Date) as Date__c,
'RMJ' as Product__c,
c.salesforceAccountId as Account__c,
contracts.opportunityId as Opportunity__c,

from activeco.ContractServer.SaaS_Usage u
left join `ContractServer.Customers` c on u.rmjPortalId = c.rmjPortalId
left join(SELECT customer_salesforceAccountId, opportunityId, human_end,status
FROM (
  SELECT customer_salesforceAccountId, opportunityId, human_end, status,
    ROW_NUMBER() OVER (PARTITION BY customer_salesforceAccountId ORDER BY human_end DESC) as rn
  FROM `ContractServer.Contracts`
)
WHERE rn = 1) contracts on c.salesforceAccountId = contracts.customer_salesforceAccountId
where date(human_start) >= DATE_SUB(DATE_TRUNC(DATE(CURRENT_DATE()), MONTH), INTERVAL 3 YEAR)
and contracts.status = 'ACTIVE'; -- Only Active Contracts
    "
  ))

## get ASCI Usage Data
# create a database connection object to ASCI Health Service
print("Connecting to ASCI health service")
con <- dbConnect(MySQL(), user=AB_HEALTH_USER, password=AB_HEALTH_PASS,
                 dbname='activebatchusagedatastore-prod', host='productionmysql.clnjnlh3g1dz.eu-west-1.rds.amazonaws.com',
                 port = 3306 )

ab.daily.usage <- dbGetQuery(con,
                             "-- instance data by account
SELECT 
concat('NA - ',a.AccountName) as RMJ_Portal_Id__c,
concat('JssId - ',js.RegisteredJssId) as Environment__c,
sum(s.TotalSucceeded)+sum(s.TotalFailed)+sum(s.TotalAborted) as Job_Executions__c,
CAST(DATE_FORMAT(s.StartTimeUTC, '%Y-%m-%d') as date) as Date__c,
'AB' as Product__c,
a.SalesforceID as Account__c,
null as Opportunity__c
FROM accounts a
    INNER JOIN jobschedulers js ON js.AccountID = a.AccountID
    INNER JOIN captures c ON c.RegisteredJssId = js.RegisteredJssId
    INNER JOIN instancesnapshots s ON s.CaptureID = c.CaptureID
WHERE  s.StartTimeUTC >= DATE_SUB(CURDATE(), INTERVAL 3 YEAR)
group by 1,2,4,5,6;"
)
ab.daily.usage$Date__c <- as.Date(ab.daily.usage$Date__c)
# join AB usage with RMJ Usage
account.usage <- bind_rows(account.usage,ab.daily.usage)

buffer <- 1
# account.usage %>%
#   group_by(Product__c) %>%
#   summarise(acts = length(unique(Account__c)))

last.60 <- (Sys.Date()-61)-buffer # allow all trends to be set back by x(buffer) days
last.30 <- (Sys.Date()-31)-buffer
measure.date <- (Sys.Date()-1)-buffer # do not include today in the calculation because the data may be incomplete

max.usage.dates <- account.usage %>%
  group_by(Account__c) %>%
  summarise(last.submission = max(Date__c,na.rm = T)) %>%
  mutate(last_60 = (last.submission - 61)-buffer,
         last_30 = (last.submission - 31)-buffer
         )

# Merge the max usage data for MoM into the Account Usage data
account.usage <- merge(account.usage,max.usage.dates,
                       by = 'Account__c', all = T)

# Upload null for customers not on a contract
# add a date field for customers not reporting usage
# zero out customers that are not on a contract

print('Uploading Usage Change to Account Level')
usage.change <- account.usage %>%
  group_by(Account__c) %>% # is grouping products togethere ,Product__c
  summarise(last.60.usage = sum(Job_Executions__c[which(Date__c>=last_60 & Date__c < last_30)],na.rm = T),
            last.30.usage = sum(Job_Executions__c[which(Date__c>=last_30 & Date__c < last.submission)], na.rm = T),
            last.submission = max(Date__c,na.rm = T)) %>%
  mutate(pct.change = (last.30.usage - last.60.usage) / last.60.usage)

# x <- account.usage %>%
#   filter(Date__c >= '2023-05-28' &
#          Date__c < '2023-06-27' &
#            Account__c == '0013800001DoUaaAAF')

# clean up errors
usage.change$pct.change[is.infinite(usage.change$pct.change)] <- 1
usage.change$pct.change[is.nan(usage.change$pct.change)] <- 0
usage.change$upload.change <- round(usage.change$pct.change,2) * 100

# upload usage change info to SFDC
upload.usage.change <- usage.change[,c("Account__c","upload.change",'last.30.usage',"last.submission")]
names(upload.usage.change) <- c('id','Usage_Change_MoM__c','Usage_Reported_Last_30_Days__c','Last_Usage_Reported_Date__c')

# Make sure to zero out old records
og.sfdc.usage <- query.bq(paste0(
"select
Id as id,
Usage_Change_MoM__c,
Usage_Reported_Last_30_Days__c,
Last_Usage_Reported_Date__c
from `skyvia.Account`
where Usage_Reported_Last_30_Days__c is not null
and id not in (
",string.in.for.query(upload.usage.change$id),"  
)"
))
if(nrow(og.sfdc.usage)>0){
# mark zero for all accounts not in upload
og.sfdc.usage <- og.sfdc.usage[which(!(og.sfdc.usage$id %in% upload.usage.change$id)),]
og.sfdc.usage$Usage_Change_MoM__c <- NA
og.sfdc.usage$Usage_Reported_Last_30_Days__c <- NA

upload.usage.change <- bind_rows(upload.usage.change,og.sfdc.usage)
}

update_results_usage_change <- sf_update(
  upload.usage.change,
  'Account',
  verbose = FALSE,
  api_type = "Bulk 2.0"
)

# make a key of the merged or deleted accounts that need to be updated to capture usage
errors <- update_results_usage_change[which(!is.na(update_results_usage_change$sf__Error)),]

if(nrow(errors)>0){
  unique.accounts <- account.usage[,c("Account__c","RMJ_Portal_Id__c","Product__c")]
  unique.accounts <- unique.accounts[!duplicated(unique.accounts),]
  errors <- merge(errors,unique.accounts,by.x = 'Id',by.y = 'Account__c',all.x = TRUE)
  errors <- errors[,c("Id","sf__Error","Last_Usage_Reported_Date__c","RMJ_Portal_Id__c","Product__c")]
  names(errors) <- c("Portal_Account_ID",'SF_Error','Last_Reported_Usage','Portal_ID','Product')
  errors$Portal_ID <- gsub("NA - ","",errors$Portal_ID)
  errors <- errors[order(errors$Product),]
    
  write_sheet_keep_sheet_format(errors,
              "https://docs.google.com/spreadsheets/d/1B59CrHYX14q35lB5CrFtZd_EGFDtNNA21yOrUJ6gUTs/edit#gid=0",
              'Usage DB Accounts not in Salesforce (Auto Update)')
}


# test plot to view customer usage
# library(ggplot2)
# aid <- c('0013800001I9wRCAAZ')
# ggplot(account.usage[which(account.usage$Account__c %in% aid & 
#                              account.usage$Date__c >= '2023-03-01'),], aes(x = Date__c, y = Job_Executions__c)) +
#   geom_line() +
#   labs(x = "Date", y = "Value") +
#   ggtitle("Line Graph Over Time")
# 
# ggplot(account.usage[which(account.usage$Account__c %in% aid),], aes(x = as.Date(Date__c), y = Job_Executions__c, group = cut(Date__c, "month"))) +
#   geom_line() +
#   labs(x = "Date", y = "Value") +
#   ggtitle("Line Graph Over Time Grouped by Month") +
#   scale_x_date(date_breaks = "1 month", date_labels = "%b %Y")


# table(update_results_usage_change$sf__Error)

print('Uploading Daily usage to SFDC')
# account.usage$Name <- paste0(account.usage$Date__c)
account.usage$R_Program_Id__c <- paste0(account.usage$RMJ_Portal_Id__c,account.usage$Environment__c,
                             account.usage$Date__c,account.usage$Job_Executions__c,account.usage$Product__c)

# delete any records that are not in the usage upload
delete.records <- og.account.usage[which(!(og.account.usage$R_Program_Id__c %in% unique(account.usage$R_Program_Id__c))),]

if(nrow(delete.records) > 0){
  print(paste0(Sys.Date()," Deleting Records: ",nrow(delete.records)))
  x <- sf_delete(
    delete.records$Id,
    "Daily_Usage__c",
    verbose = FALSE,
    api_type = "Bulk 2.0"
  )
  table(x$sf__Error)
}

# why are some IDs duplicated?
# duplicated.usage.ids <- unique(account.usage$R_Program_Id__c[which(duplicated(account.usage$R_Program_Id__c))])
# View(account.usage[which(account.usage$R_Program_Id__c %in% duplicated.usage.ids),])
account.usage.upload <- account.usage[which(!duplicated(account.usage$R_Program_Id__c)),]
account.usage.upload <- account.usage.upload[,c(
  "Account__c",
  "RMJ_Portal_Id__c",
  "Environment__c",
  "Job_Executions__c",
  "Date__c",
  "Product__c",
  "Opportunity__c",
  "R_Program_Id__c"
)]
# only upload the records needed
account.usage.upload <- account.usage.upload[which(!(account.usage.upload$R_Program_Id__c %in% og.account.usage$R_Program_Id__c)),]

# test.upload.accoutns <- c(
#   "edison-international-southern-california-edison",
#   "link-snacks",
#   "energy-transfer",
# )
# 
# test.upload <- account.usage[which(account.usage$RMJ_Portal_Id__c %in% test.upload.accoutns),]

# only upload non missing records

upsert_results <- sf_upsert(account.usage.upload,
                            "Daily_Usage__c",
                            api_type = "Bulk 2.0",
                            verbose = FALSE,
                            external_id_fieldname = "R_Program_Id__c")

table(upsert_results$sf__Error)


end.time <- Sys.time()
script.time <- round(difftime(end.time,start.time,units = 'mins'),2)
print(paste0("Ended at at: ",end.time," ",script.time,"Minutes"))



dbDisconnect(con)

source("Upload SFDC JSCAPE Health Service.R")

