source('ActiveCo Functions.R')
library(httr)
library(jsonlite)

# Set the API endpoint URL
url <- "https://api.clari.com/v4/forecast/"
export.url <- "https://api.clari.com/v4/export/forecast/"
api_key <- "f7ad3871-2233-4b85-b81e-8b1209551f66474998-0"
# forecastId <- "forecast_new_summary"
# forecastId <- "renewals"
# forecastId <- "forecast"
forecastId <- c("renewals",
                "forecast")
# Set your API key or token for authentication
headers <- c(
  'apikey' = paste(api_key)
)

# list of report parameters
query.params <- list(
  includeHistorical = TRUE,
  # timePeriod = "2024_Q2", # clari defaults to current quarter
  typesToExport = c('forecast','quota','forecast_updated','adjustment','crm_total','crm_closed')
  )
# convert params to JSON for API call
query.json <- toJSON(query.params,auto_unbox = TRUE)

# Make the GET request (adjust parameters and method as needed)
response <- POST(
  paste0(export.url,forecastId),
  add_headers(.headers=headers),
  body = query.json
)
  # read the forecast export Job ID
jobid <- fromJSON(content(response,'text',encoding = 'UTF-8'))

# send a request for the status of the export job
minutes.to.wait <- 5
print("Pausing for 10 seconds for report to process")
Sys.sleep(10) # pause for 10 seconds
for( i in 1:minutes.to.wait){
  status <- GET(paste0("https://api.clari.com/v4/export/jobs/",jobid[[1]]),
                add_headers(.headers=headers)
  )
  status.response <- fromJSON(content(status,'text',encoding = 'UTF-8'))
  
  if(status.response$job$status != "DONE"){
    print("Report not ready, waiting another minute")
    Sys.sleep(60) # pause for one minute if the job is not done
  }else{
    print("Forecast Export Ready for Pickup! Sending GET Request")
    break
  } # else break the waiting loop
}
# get the content of the forecast export
forecast.report.response <- GET(paste0("https://api.clari.com/v4/export/jobs/",jobid[[1]],"/results"),
                                add_headers(.headers=headers)
)

f.cast <- fromJSON(content(forecast.report.response,'text',encoding = 'UTF-8'),flatten = TRUE)

entries <- f.cast$entries
users <- f.cast$users
field.map <- f.cast$fields
time.period.map <- f.cast$timePeriods
time.frame.map <- f.cast$timeFrames

# scrub column names for BigQuery
names(entries) <- clean.names(names(entries))
names(users) <- clean.names(names(users))
names(field.map) <- clean.names(names(field.map))
names(time.period.map) <- clean.names(names(time.period.map))
names(time.frame.map) <- clean.names(names(time.frame.map))



upload.to.bigquery(entries,"Clari","Entries")
upload.to.bigquery(users,"Clari","Users")
upload.to.bigquery(field.map,"Clari","Field_Map")
upload.to.bigquery(time.period.map,"Clari","Time_Period_Map")
upload.to.bigquery(time.frame.map,"Clari","Time_Frame_Map")







