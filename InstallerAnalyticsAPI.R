source('ActiveCo Functions.R')
library(httr)
library(jsonlite)
library(googlesheets4)
library(salesforcer)


###### GET API KEY #######
# Define the URL you want to send the POST request to
url <- "https://accounts.advancedinstaller.com/api/599dd5ca5bbae77e269de6b2/user/login"
# Create a named list or a JSON object with the data you want to send in the request body
payload <- list(
  email = "Ron.Hagag@cerberusFTP.com",
  password = "99#B$b^6qz0otyirM#KciS42*x^$p!!z5eZhaC!Q5$65",
  remember = "true"
)

# Send the POST request
response <- POST(url, body = payload, encode = "json")

# Get the response status code
status_code <- status_code(response)

# Get the response content
content <- content(response)

# Print the response
print(content)
installer.key <- content$jwt
# installer.key <- "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJfaWQiOiI2MjU2ZTkwMWZmNzY4NjRhMGY1ZDFlNzEiLCJpYXQiOjE2ODY3NzA2OTgsImV4cCI6MTY4OTM2MjY5OH0.3JUBjSJc9Heil4UZm1T6gJ7I_-t0L5pLinZ_SdqDfvM"
tracking.code <- "586d207db4224f4362e5ed00"

# url <- paste0(
#   "https://installeranalytics.com/api/apps/",
#   tracking.code,"/reports/installs?hitFilters=all"
# )

url <- paste0(
  "https://installeranalytics.com/api/apps/",tracking.code,"/reports/custom-properties?hitFilters=all"
)

# Create a named list or a JSON object with the headers, including the bearer token
headers <- c("Authorization" = paste("Bearer", installer.key))

# Send the GET request with the headers
response <- GET(url, add_headers(headers))
# Get the response status code
status_code <- status_code(response)

# Get the response content
fromjson.response <- fromJSON(content(response,'text', encoding = "UTF-8"),flatten = TRUE)
fromjson.response$tracking_token <- NA
for (t in 1:nrow(fromjson.response)) {
  # t <- 500
  fromjson.response$tracking_token[t] <- as.character(fromjson.response$properties[t][[1]][2])
  
}
fromjson.response$properties <- NULL


# get matching downloads from Cerberus
leads <- query.bq(
"
select
 Download_Identifier__c,
 Id as LeadId,
 FirstName,
 LastName,
 CreatedDate
from `CerberusSFDC.Lead`
where Installation_Verified__c = FALSE
and Download_Identifier_Present__c = TRUE
"
)

installs <- fromjson.response[which(fromjson.response$type == 'install'),]
leads$install <- NA
table(nchar(leads$Download_Identifier__c))

# # remove regex special characters from leads tracking token
# library(stringr)
# # List of regex characters
# regex_chars <- c("\\", ".", "*", "+", "?", "[", "]", "(", ")", "{", "}", "^", "$", "|")
# leads$tracking_token_match <- leads$Download_Identifier__c
# for (s in regex_chars) {
#   # s <- regex_chars[8]
#   r <- paste0("\\",s)
#   r2 <- paste0("\\",r)
#   leads$tracking_token_match <- gsub(r,r2,leads$tracking_token_match)
# }


for (l in 1:nrow(leads)) {
  # l <- 1
  uuid <- leads$Download_Identifier__c[l]
  if(nchar(uuid) == 6){
    g.lookup <- paste0("-",uuid,"\\.")
    leads$install[l] <- paste0(unique(installs$tracking_token[grepl(g.lookup,installs$tracking_token)]),collapse = ',')
  }else {
    leads$install[l] <- paste0(unique(installs$tracking_token[grepl(uuid,installs$tracking_token)]),collapse = ',')
  }
  
}

leads$install[which(leads$install == "")] <- NA
table(is.na(leads$install))
matches <- leads[which(!is.na(leads$install)),]

# only upload if there are matches
if(nrow(matches)>0){
upload <- matches[,"LeadId"]
names(upload) <- 'Id'
upload$Installation_Verified__c <- TRUE
og <- upload
og$Installation_Verified__c <- FALSE

# sheet.link <- "https://docs.google.com/spreadsheets/d/19qdr4xxMc0WNzC0iGjsJQK60jnwUZ6ccF9UE8iMSleo/edit#gid=0"
# 
# write_sheet(fromjson.response,
#             sheet.link,
#             sheet = 'InstallAnalytics')
# write_sheet(matches,
#             sheet.link,
#             sheet = 'Matched Leads')



# upload back to CERB
# sf_auth(cache = TRUE)
sf_auth(cache = '.httr-oauth-cerberus')
sf_update(
  upload,
  'Lead'
)

# make.sfdc.update(
#   "Cerberus Installer Analytics Lead Update",
#   og,
#   upload,
#   'Lead',
#   'Updating the Install Verified on the Lead Object at Cerberus. For more info see InstallerAnalyticsAPI.R'
# )

}
