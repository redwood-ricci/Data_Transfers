# load library for accessing odbc connections
source("ActiveCo Functions.R")
library(odbc)
library(googlesheets4)

# set connection to SSMS
ssms.con <- dbConnect(odbc(),
                      Driver = "SQL Server",
                      Server = "WIN-6PDIPKC0EER",
                      Database = "Source_Production_SALESFORCE",
                      UID = SSMS_USR,
                      PWD = SSMS_PASS)

# paste a query to execute against SSMS
query <- "
select
CurrencyIsoCode
,Name
,acv_bookings__c as qb
,acv_bookings__c / ct.conversionrate as qb_usd
from SALESFORCE.CData.Salesforce.Opportunity o
left join SALESFORCE.CData.Salesforce.CurrencyType ct on o.CurrencyIsoCode = ct.isocode
where o.acv_bookings__c is not null
and closedate > '2024-01-01'
and StageName = 'Closed Won'
"

# execute the query and store the results in a data frame
results <- dbGetQuery(ssms.con, query)

# set authorization to write to google sheets
# gs4_auth()

# Make a variable for the sheet to write to
sheet.link <- "https://docs.google.com/spreadsheets/d/1VB-pBbWah4IEbOjRaK-Av-K_we_Wc-HbJFqVAQmjEP0/edit#gid=0"

# write to the sheet
write_sheet(
  data = results,
  ss = sheet.link,
  sheet = 'My New Data'
)

# disconnect from Database
dbDisconnect(ssms.con)
