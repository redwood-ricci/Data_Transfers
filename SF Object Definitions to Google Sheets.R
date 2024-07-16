source('ActiveCo Functions.R')
library(googlesheets4)
library(dplyr)
# opp.fields <- sf_describe_object_fields("Opportunity")
sheet.link <- "https://docs.google.com/spreadsheets/d/10bdc0g2Mro38r3kfm1WkW4zxRixeCRj8MViWgxi_Puc/edit#gid=2058635077"

# x <- get.sf.object()
query.objects <- c(
  'OpportunityStage',
  'Lead',
  'Opportunity',
  'Account',
  'RecordType',
  'Opp_Stage_Activity__c',
  'UserRole',
  'User',
  'Product2',
  'OpportunityLineItem',
  'PricebookEntry',
  'Opp_Stage_Activity__History',
  'Contact',
  'Contract',
  'Campaign',
  'CampaignMember',
  'Task',
  'Event',
  'Invoice__c',
  'LoginHistory',
  'Usage__c',
  'Daily_Usage__c',
  'Case',
  'Order',
  'SBQQ__Quote__c',
  'SBQQ__QuoteLine__c',
  'SBQQ__Subscription__c',
  'OrderItem'
)


# sf_auth(cache = ".httr-oauth-salesforcer-redwood")
# for(o in query.objects){
#   print(paste0("Getting Fields: ",o))
#   object_fields <- sf_describe_object_fields(o)
#   object_fields <- df.lists.to.string(object_fields)
#   
#   print(paste0("Writing ",o," to Google Sheets"))
#   write_sheet(object_fields,"https://docs.google.com/spreadsheets/d/1PP5DxiImlk56lcGI5dDferT-DMYy_fywB7WpeFs5dL8/edit#gid=0",
#               sheet = o)
# }


sf_auth(cache = ".httr-oauth-salesforcer-asci")
for(o in query.objects){
  print(paste0("Getting Fields: ",o))
  object_fields <- sf_describe_object_fields(o)
  object_fields <- df.lists.to.string(object_fields)
  
  if(o == "RecordType"){ # record type picklist values are too long for google sheets, truncate to under 5k characters
    object_fields$`_picklistValues` <- str_trunc(object_fields$`_picklistValues`,4990)
  }
  
  print(paste0("Writing ",o," to Google Sheets"))
  write_sheet(object_fields,sheet.link,
              sheet = o)
}
