source('ActiveCo Functions.R')
sf_auth(cache = ".httr-oauth-salesforcer-asci") # authorize connection
a.dates <- sf_run_report('00O3t000008KqQ5EAK')

a.dates$`Last Activity Date` <- as.Date(a.dates$`Last Activity Date`)

names(a.dates) <- clean.names(names(a.dates))

upload.to.bigquery(a.dates,'R_Data','BPA_Activity')
