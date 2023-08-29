source("ActiveCo Functions.R")
library(odbc)
library(httr)
# Endpoint, Credentials, and Other Variables
# AP endpoint
# endpoint_url <- "https://services1.myworkday.com/ccx/service/customreport2/redwood/bhanuj.pant@redwood.com/Analytics_-__AP_Suppliers__Details?IRS_1099_Supplier=0&format=csv"


est_now <- with_tz(Sys.time(), "America/New_York")

endpoint_url <- "https://services1.myworkday.com/ccx/service/customreport2/redwood/bhanuj.pant@redwood.com/Analytics_-__AP_Suppliers__Details?IRS_1099_Supplier=0&format=csv"
# xlsx.url <- "https://services1.myworkday.com/ccx/service/customreport2/redwood/bhanuj.pant%40redwood.com/Analytics_-__AP_Suppliers__Details?IRS_1099_Supplier=0&format=csv"
username <- "Report_ISU_TestUser"
password <- "1wVYA8rY1Y57nE@W"

# API Request
response <- GET(url = endpoint_url, authenticate(username, password, type = "basic"))

# Check if request was successful and content type is CSV
if (http_type(response) == "text/csv" && status_code(response) == 200) {
  csv_content <- content(response, "text")
  df <- read.csv(textConnection(csv_content), header = TRUE)
  
#   head(df)
} else {
  stop("Failed to retrieve CSV data from Workday!")
}

df$now <- est_now

ssms.con <- dbConnect(odbc(),
                      Driver = "SQL Server",
                      Server = "WIN-6PDIPKC0EER",
                      Database = "Analytics_Source",
                      UID = SSMS_USR,
                      PWD = SSMS_PASS)

dbWriteTable(ssms.con,'R_Analytics_AP_Supplier_Details',df, append = TRUE) #overwrite = TRUE)



