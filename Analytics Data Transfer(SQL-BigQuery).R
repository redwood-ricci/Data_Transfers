source('ActiveCo Functions.R')
library(RMySQL)
library(odbc)

print("Connecting to SSMS")
ssms.con <- dbConnect(odbc(),
                      Driver = "SQL Server",
                      Server = "WIN-6PDIPKC0EER",
                      Database = "Analytics_Working",
                      UID = SSMS_USR,
                      PWD = SSMS_PASS)


print("Query Data")
data <- dbGetQuery(ssms.con,"select * from dbo.SFDC_LOST_RENEWALS")

names(data) <- clean.names(names(data))
print("Upload to Bigquery")
upload.to.bigquery(data,'Analytics','SQL_SFDC_LOST_RENEWALS')