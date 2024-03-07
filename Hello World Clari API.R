source('ActiveCo Functions.R')
library(httr)
library(jsonlite)

# Set the API endpoint URL
url <- "https://api.clari.com/v4/forecast/"
export.url <- "https://api.clari.com/v4/export/forecast/"
api_key <- "f7ad3871-2233-4b85-b81e-8b1209551f66474998-0"
forecastId <- "forecast"
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



headers <- c(
  'apikey' = paste(api_key),
   'Content-Type' = 'application/json',
  'forecastId' = forecastId,
  'typesToExport' = 'forecast_updated'
  #'timePeriod' = '2024_Q1'
)

# Make the GET request (adjust parameters and method as needed)
response <- GET(
  paste0(url,forecastId),
  add_headers(.headers=headers)
  )

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


