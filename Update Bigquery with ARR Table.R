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

# Run the stored procedure
print("Running Stored Procedure")
result <- dbExecute(ssms.con, "execute dbo.sp_ARR_OPPORTUNITY_DATA")

# Print the result    
# print(result)
print("Query Data")
data <- dbGetQuery(ssms.con,"select * from dbo.ARR_OPPORTUNITY_DATA")
    
names(data) <- clean.names(names(data))
print("Upload to Bigquery")
upload.to.bigquery(data,'R_Data','Customer_ARR')


# data <- dbGetQuery(ssms.con,
#                    "
# /****** Script for SelectTopNRows command from SSMS  ******/
# SELECT [Run Date]
#       --,[Account ID]
#       ,[Opportunity ID]
#       ,[Account Name]
#       ,[Account Type]
#       ,[Account Segment]
#       ,[ARR Region]
#       ,[Country]
#       ,[Sub Region]
#       ,[Account Owner]
#      -- ,[CM Company Size by Emp]
#       --,[CM Company Size by Rev]
# 	    ,[CM Company Size by Rev New]
#       ,[CM Company Size by Emp New]
#       ,[Booking Type]
#       ,[ARR Product]
#       ,[Freshman Flag]
#       ,[ARR Price Band]
#       ,[Cohort Close Date]
#       ,[Cohort Year]
#       ,[Contract Start Date]
#       ,[Contract End Date]
#       ,[Subscription Start Date]
#       ,[Subscription End Date]
#       ,[Contract Term]
#       ,[Current Year Contract]
# 	  ,[ARR Contract End Date]
# 	  ,[ARR Contract End Year]
# 
#      -- ,[ARR(SF)]
#       ,[ARR_Period_1]
#       ,[ARR_Period_2]
#       ,[ARR_Period_3]
#       ,[ARR_Period_4]
#       ,[ARR_Period_5]
#       ,[ARR_Period_6]
#       ,[Overall_ARR]
#     
#   FROM [Analytics_Working].[dbo].[ARR_OPPORTUNITY_DATA]
#   where [Overall_ARR] > 0
# order by [Account Name] ASC
#                    ")

