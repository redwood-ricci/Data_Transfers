source('ActiveCo Functions.R')
library(tidyr)
library(lubridate)
library(bigQueryR)
library(googlesheets4)

ff.tables <- query.bq("select * from activeco.Snapshots.INFORMATION_SCHEMA.TABLES")

# opp.tables <- ff.tables[which(ff.tables$base_table_name == "Opportunity"),]
opp.tables <- ff.tables[which(grepl("^Opportunity_",ff.tables$table_name)),]
opp.tables <- opp.tables[order(opp.tables$creation_time),]

# get max opp date
max.date <- query.bq("
select max(Snapshot_Time) from `activeco.R_Data.Opportunity_History`         
         ")
# used for restarting
# max.date <- as.Date('1970-01-01')
# opp.tables <- opp.tables[which(as.Date(opp.tables$creation_time) > max.date),]
opp.tables <- opp.tables[which(as.Date(opp.tables$creation_time) > as.Date(max.date$f0_[1])),]

# opp.tables$quarter <- quarter(opp.tables$snapshot_time_ms,with_year = TRUE)
# # only opp tables after Q3
# opp.tables <- opp.tables[which(opp.tables$snapshot_time_ms >= snapshot.anchor
#                                & opp.tables$snapshot_time_ms < q.end.date),]
# opp.tables <- opp.tables[order(opp.tables$snapshot_time_ms),]


####### Operationalize this section into it's own script to create the table in bigquery ######
for (t in 1:nrow(opp.tables)) {
  # t <- 2
  table <- opp.tables$table_name[t]
  snapshot.time <- opp.tables$creation_time[t]
  print(paste0(t," of ",nrow(opp.tables)))
  print(paste0(table," Reading"))
  forecast.opps <- query.bq(paste0("
  select
  distinct
  o.Id,
  o.Name,
  o.CloseDate,
  o.Billing_Period_1_Amount__c / ct.ConversionRate as FYV_USD,
  o.ACV_Bookings__c / ct.ConversionRate as qb_usd,
  o.Manager_s_Forecast__c / ct.ConversionRate as forecast_usd,
  o.StageName,
  o.Type,
  o.Renewal_Category__c,
  o.Renewal_Type__c,
  o.Expansion_Type__c,
  o.Product__c,
  o.Warboard_Category__c,
  o.SAO_Date__c
  from `Snapshots.",table,"` o 
  left join `skyvia.CurrencyType` ct on o.CurrencyIsoCode = ct.IsoCode
  where Test_Account__c = false"))
  
  forecast.opps$Snapshot_Time <- snapshot.time
  # upload.to.bigquery(forecast.opps,'R_Data','Opportunity_History') # first upload
  print(paste0("Writing"))
  bq_table_upload('activeco.R_Data.Opportunity_History',forecast.opps,write_disposition ='WRITE_APPEND')
  
}
rm(forecast.opps)

# upload.to.bigquery(ff.upload,"Forecast_Files",as.character(Sys.Date()))
# 
# upload.to.bigquery(forecasted.opps,"Snapshots","OppRoll")