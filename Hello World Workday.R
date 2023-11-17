source("ActiveCo Functions.R")
library(odbc)
library(httr)
# Endpoint, Credentials, and Other Variables
# AP endpoint
# endpoint_url <- "https://services1.myworkday.com/ccx/service/customreport2/redwood/bhanuj.pant@redwood.com/Analytics_-__AP_Suppliers__Details?IRS_1099_Supplier=0&format=csv"


est_now <- with_tz(Sys.time(), "America/New_York")

endpoint_url <- c("https://services1.myworkday.com/ccx/service/customreport2/redwood/bhanuj.pant@redwood.com/Analytics_-__AP_Suppliers__Details?IRS_1099_Supplier=0&format=csv",
                  "https://services1.myworkday.com/ccx/service/customreport2/redwood/bhanuj.pant@redwood.com/Analytics_-_AP_All_Invoices?Invoice_Date=2022-01-01-08:00&format=csv")

report_names <- c("R_Analytics_AP_Supplier_Details",
                  "R_Analytics_AP_All_Invoices")

username <- "Report_ISU_TestUser"
password <- "1wVYA8rY1Y57nE@W"

ssms.con <- dbConnect(odbc(),
                      Driver = "SQL Server",
                      Server = "WIN-6PDIPKC0EER",
                      Database = "Analytics_Source",
                      UID = SSMS_USR,
                      PWD = SSMS_PASS)

for (i in 1:length(report_names)){
  
# API Request
response <- GET(url = endpoint_url[i], authenticate(username, password, type = "basic"))

# Check if request was successful and content type is CSV
if (http_type(response) == "text/csv" && status_code(response) == 200) {
  csv_content <- content(response, "text")
  report_df <- read.csv(textConnection(csv_content), header = TRUE)
  
#   head(df)
} else {
  stop("Failed to retrieve CSV data from Workday!")
}

report_df$now <- est_now

dbWriteTable(ssms.con,report_names[i],report_df, append = TRUE) #overwrite = TRUE)
print(report_names[i])
}
#sql_query <- "ALTER TABLE your_table ADD new_column VARCHAR(255)"
#sqlQuery(conn, sql_query)
