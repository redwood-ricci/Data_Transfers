source('ActiveCo Functions.R')
library(httr)
library(jsonlite)

# Set the API endpoint URL
url <- "https://api.clari.com/v4/forecast/"
export.url <- "https://api.clari.com/v4/export/forecast/"
api_key <- "f7ad3871-2233-4b85-b81e-8b1209551f66474998-0"
forecastId <- "forecast_new_summary"
# Set your API key or token for authentication

# Add headers for authentication and content type. This might vary based on the API's requirements.
# headers <- c(
#   'apikey' = paste(api_key),
#   #   'Content-Type' = 'application/json',
#   'forecastId' = forecastId
# )
# 
# # Make the GET request (adjust parameters and method as needed)
# response <- POST(
#   paste0(export.url,forecastId),
#   add_headers(.headers=headers)
# )
# 
# content(response,'text', encoding = "UTF-8")
send.headers <- 
  headers <- c(
    'apikey' = paste(api_key),
    'forecastId' = forecastId,
    "includeHistorical"= 'true',
    "timePeriod" = "2024_Q2",
    "scopeId" = "332801::MGR",
    "exportFormat"= "CSV"
  )
make.forecast <- POST(
  paste0("https://api.clari.com/v4/export/forecast/",forecastId),
  add_headers(.headers = send.headers)
)
jobid <- fromJSON(content(make.forecast,'text',encoding = 'UTF-8'))
# jobid$jobId
response.headers <- c(
  'apikey' = api_key
)

forecast.response <- GET(
  paste0("https://api.clari.com/v4/ingest/job/",jobid$jobId,"/results"),
  add_headers(.headers = response.headers)
)
# zzz <- content(forecast.response)
# x <- zzz$entries
# fcst.rpt <- fromJSON(content(forecast.response))


headers <- c(
  'apikey' = paste(api_key),
   'Content-Type' = 'application/json',
    forecastId = 'forecast_new_summary',
    typesToExport = 'forecast_updated',
    scopeId = '1905::MGR'
)
# Define the request body
# body <- list(

# )

# Convert body to JSON if needed
# body_json <- toJSON(body)

# Make the GET request with a body
response <- GET(
  paste0(url,forecastId),
  add_headers(.headers=headers)
)

  # 'scopeId' = '{type:FORECAST ROLE , userId:332801, userType:MGR}'
  # 'scopeId' = "{\\\"type\\\":\\\"FORECAST_ROLE\\\",\\\"userId\\\":332801,\\\"userType\\\":\\\"MGR\\\"}"
  # 'scopeId' = "{\"type\":\"FORECAST_ROLE\",\"userId\":332801,\"userType\":\"MGR\"}"
  # 'scopeId' = "{type:FORECAST_ROLE,userId:332801,userType:MGR}"
  #'timePeriod' = '2024_Q1'

# Make the GET request (adjust parameters and method as needed)
# response <- GET(
#   paste0(url,forecastId),
#   add_headers(.headers=headers),verbose()
#   )

# print(response$request$headers)

# Check the status code of the response
if (status_code(response) == 200) {
  # Parse the response body from JSON format
  data <- content(response, "parsed")
  print(data)
} else {
  print(paste("Error in API request. Status code:", status_code(response)))
}
content(response,'text', encoding = "UTF-8")

f.cast <- fromJSON(content(response,'text', encoding = "UTF-8"),flatten = TRUE)

deals <- f.cast$entries
reps <- f.cast$users
field.map <- f.cast$fields
time.map <- f.cast$timePeriods
timeframe.map <- f.cast$timeFrames






