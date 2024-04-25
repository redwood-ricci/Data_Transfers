source('ActiveCo Functions.R')
library(RMySQL)

# create a database connection object
con <- dbConnect(MySQL(), user=AB_HEALTH_USER, password=AB_HEALTH_PASS,
                 dbname='activebatchusagedatastore-prod', host='productionmysql.clnjnlh3g1dz.eu-west-1.rds.amazonaws.com',
                 port = 3306 )

cerb.con <- dbConnect(MySQL(), user=AB_HEALTH_USER, password=AB_HEALTH_PASS,
                 dbname='cerberus_hs_prod', host='productionmysql.clnjnlh3g1dz.eu-west-1.rds.amazonaws.com',
                 port = 3306 )

jscape.con <- dbConnect(MySQL(), user=AB_HEALTH_USER, password=AB_HEALTH_PASS,
                      dbname='jscape_hc_prod', host='productionmysql.clnjnlh3g1dz.eu-west-1.rds.amazonaws.com',
                      port = 3306 )

# check the connection is working
# dbListTables(con)

upload_asci_to_bigquery <- function(connection,Table_Name,object.name,date.cols=NA,upload.name = NA){
  # object.name <- 'instancesnapshots'
  # date.cols <- c("StartTimeUTC","EndTimeUTC")
  # connection <- con
  # Table_Name <- 'ASCI_Health_Service'
  # object.name <- 'api_key'
  # date.cols <- 'creationDate'
  # upload.name <- NA'api_key'
  z <- dbGetQuery(connection,paste0('select * from ',object.name))
  
  if(all(!is.na(date.cols))){
    for(d in date.cols){
      # d <- date.cols[1]
      z[,d] <- as_datetime(z[,d])
      z[which(is.na(z[,d])),d] <- as_datetime('1970-01-01 00:00:00.00')
  }}
  
  if(is.na(upload.name)){
    upload.name <- object.name
  }
  
  upload.to.bigquery(z,Table_Name,upload.name)
  
}


######### ACCOUNTS #######
# acts <- dbGetQuery(con,'select * from accounts')
upload_asci_to_bigquery(con,'ASCI_Health_Service','accounts','FirstInstallDate','Accounts')

######### Schedulers #########
# schedulers <- dbGetQuery(con,'select * from jobschedulers')
upload_asci_to_bigquery(con,'ASCI_Health_Service','jobschedulers','LastHealthReportDate','JobSchedulers')

######### All Captures #########
# allcaptures <- dbGetQuery(con,'select * from allcaptures')
upload_asci_to_bigquery(con,'ASCI_Health_Service','allcaptures','CaptureDateTime','AllCaptures')

######### Latest Captures #########
# LatestCap <- dbGetQuery(con,'select * from latestcapturedata')
upload_asci_to_bigquery(con,'ASCI_Health_Service','latestcapturedata','CaptureDateTime','LatestCaptures')

######### Job Step Reference Counts #########
upload_asci_to_bigquery(con,'ASCI_Health_Service','JobStepReferenceCounts')

######### Captures #########
upload_asci_to_bigquery(con,'ASCI_Health_Service','Captures','CaptureDateTime')

######### Job Step Types #########
upload_asci_to_bigquery(con,'ASCI_Health_Service','JobStepTypes')
  
######### AB UI Stats #########
upload_asci_to_bigquery(con,'ASCI_Health_Service','generaluistats')

######### UI Commands #########
upload_asci_to_bigquery(con,'ASCI_Health_Service','abcommands')

######### Instance Snapshots #########
upload_asci_to_bigquery(con,'ASCI_Health_Service','instancesnapshots',c("StartTimeUTC","EndTimeUTC"))

###### Execution Templates ###############
upload_asci_to_bigquery(con,'ASCI_Health_Service','executiontemplatedata')

######## Products ############
upload_asci_to_bigquery(con,'ASCI_Health_Service','licensedproducts')

######## Scheduler Data ############
upload_asci_to_bigquery(con,'ASCI_Health_Service','schedulerdata')

upload_jscape_to_bigquery <- function(connection,Table_Name,object.name,date.cols,upload.name = NA){
  # object.name <- 'accounts'
  # date.cols <- c('FirstInstallDate')
  # connection <- jscape.con,
  # Table_Name <- 'JSCAPE_Health_Service'
  # object.name <- 'api_key'
  # date.cols <- 'creationDate'
  # upload.name <- NA'api_key'
  z <- dbGetQuery(connection,paste0('select * from ',object.name))
  
  for(d in date.cols){
    # d <- date.cols[1]
    z[,d] <- as_datetime(z[,d]/1000)
    z[which(is.na(z[,d])),d] <- as_datetime('1970-01-01 00:00:00.00')
  }
  
  if(is.na(upload.name)){
    upload.name <- object.name
  }
  
  upload.to.bigquery(z,Table_Name,upload.name)
  
}


upload_jscape_to_bigquery(jscape.con,'JSCAPE_Health_Service','api_key','creationDate','api_key')
upload_jscape_to_bigquery(jscape.con,'JSCAPE_Health_Service','eula_confirmation','confirmationDate','eula_confirmation')
# upload_jscape_to_bigquery(jscape.con,'JSCAPE_Health_Service','product_statistics','creationDate','product_statistics')

# convert JSON data from Product Statisticts to Data Frames for Upload
zzz <- dbGetQuery(jscape.con,paste0('select * from product_statistics'))
zzz$creationDate <- as_datetime(zzz$creationDate/1000)

library(jsonlite)

convert.if.can <- function(x){
  # this function covnerts an object to a data frame if it exists, otherwise it returns an empty df
  # x <- master.data
  # x <- server.data
  # x <- server.data.domain
  # x <- gateway.data
  if(!is.null(x) & length(x) != 0){
    r <- as.data.frame(do.call(cbind, x))
    # convert any list columns to data frame
    list_cols <- names(r)[sapply(r, is.list)]
    for (l in list_cols) {
      r[,l] <- tryCatch({
        r[,l] <- lapply(r[,l], as.character)
      }, error = function(e) {
        # If an error occurs, convert the first column to character using as.character
        message("Error occurred, using as.character instead: ", e$message," row",i)
        r[,l] <- as.character(r[,l])
      })
    }
    
  }else{
    r <- data.frame()
  }
return(r)
}



# i <- 3
# rm(df.master,df.server,df.server.domain,df.gateway)
print('Flattening JSCAPE Data')
for (i in 1:nrow(zzz)) {
# i <- 2
  if(i %% 10000 == 0) print(paste0(i," of ",nrow(zzz)))

# print("fromJson")
master.data <- fromJSON(zzz$data[i],flatten = TRUE)

server.data <- master.data$serverData
server.data.domain <- server.data$domainData
gateway.data <- master.data$gatewayData

master.data$serverData <- NULL
server.data$domainData <- NULL
master.data$gatewayData <- NULL

# print("Converting DFs")
df.master.data <- convert.if.can(master.data)
df.server.data <- convert.if.can(server.data)
df.server.data.domain <- convert.if.can(server.data.domain)
df.gateway.data <- convert.if.can(gateway.data)

# print("Binding")
if(nrow(df.master.data)>0){
  df.master.data$product_statistics_id <- zzz$id[i]
  if(exists("df.master")){
    df.master <- bind_rows(df.master,df.master.data)
  }else{
    df.master <- df.master.data
  }
}
if(nrow(df.server.data)>0){
  df.server.data$product_statistics_id <- zzz$id[i]
  if(exists("df.server")){
    df.server <- bind_rows(df.server,df.server.data)
  }else{
    df.server <- df.server.data
  }
}
if(nrow(df.server.data.domain)>0){
  df.server.data.domain$product_statistics_id <- zzz$id[i]
  if(exists("df.server.domain")){
    df.server.domain <- bind_rows(df.server.domain,df.server.data.domain)
  }else{
    df.server.domain <- df.server.data.domain
  }
}
if(nrow(df.gateway.data)>0){
  df.gateway.data$product_statistics_id <- zzz$id[i]
  if(exists("df.gateway")){
    df.gateway <- bind_rows(df.gateway,df.gateway.data)
  }else{
    df.gateway <- df.gateway.data
  }
}

} # close loop
zzz$data <- NULL

df.master$creationDate <- as_datetime(as.numeric(df.master$creationDate)/1000)
upload.to.bigquery(df.master,'JSCAPE_Health_Service','PS_Data')
upload.to.bigquery(df.gateway,'JSCAPE_Health_Service','PS_Gateway')
upload.to.bigquery(df.server,'JSCAPE_Health_Service','PS_Server')
upload.to.bigquery(df.server.domain,'JSCAPE_Health_Service','PS_Server_Domain')
upload.to.bigquery(zzz,'JSCAPE_Health_Service','product_statistics')
# upload.to.bigquery(df.server.data,'JSCAPE_Health_Service','PS_Server_Data')
rm(df.master,df.gateway,df.server,df.server.domain,master.data,zzz)

# upload the cerberus data
upload_jscape_to_bigquery(cerb.con,'Cerberus_Health_Service','api_key','creationDate','api_key')

zzz <- dbGetQuery(cerb.con,paste0('select * from product_statistics'))
zzz$creationDate <- as_datetime(zzz$creationDate)
convert.unix.to.date <- function(x){
  # x <- 1680201640
  x <- as.numeric(x)
  t <- as.POSIXct(x, origin = "1970-01-01")
  return(t)
}
# i <- 3
rm(df.sync,df.status,df.interfaces,df.events.rules,df.events.rules,df.config,df.auth.list,df.auth,df.master)
print("Flattening Cerberus Telemetry")
############ chat gpt version

# Lists to store the intermediate data
master_list <- list()
auth_list <- list()
auth_list_data_list <- list()
config_list <- list()
events_list <- list()
events_rules_list <- list()
interfaces_list <- list()
status_list <- list()
sync_list <- list()

for (i in seq_len(nrow(zzz))) {
  # i <- 1
  # i <- 2
  if(i %% 10000 == 0) print(paste0(i," of ",nrow(zzz)))  # print every 1000 iterations
  
  id <- zzz$id[i]
  master.data <- fromJSON(zzz$data[i], flatten = TRUE)
  
  auth.data <- master.data$auth
  auth.list.data <- convert.if.can(auth.data$authList)
  auth.data$authList <- NULL
  auth.data <- convert.if.can(auth.data)
  master.data$auth <- NULL
  
  config.data <- convert.if.can(master.data$config)
  master.data$config <- NULL
  
  events.data <- master.data$events
  events.rules.data <- convert.if.can(events.data$rules)
  events.data$rules <- NULL
  events.data <- convert.if.can(events.data)
  master.data$events <- NULL
  
  interfaces.data <- convert.if.can(master.data$interfaces)
  master.data$interfaces <- NULL
  
  status.data <- convert.if.can(master.data$status)
  master.data$status <- NULL
  
  sync.data <- convert.if.can(master.data$sync)
  master.data$sync <- NULL
  
  master.data <- convert.if.can(master.data)
  
  if(nrow(master.data) > 0) {
    master.data$product_statistics_id <- id
    master.data$creationDate <- convert.unix.to.date(master.data$creationDate)
    master_list[[i]] <- master.data
  }
  
  if(nrow(auth.data) > 0) {
    auth.data$product_statistics_id <- id
    auth_list[[i]] <- auth.data
  }
  
  if(nrow(auth.list.data) > 0) {
    auth.list.data$product_statistics_id <- id
    auth_list_data_list[[i]] <- auth.list.data
  }
  
  if(nrow(config.data) > 0) {
    config.data$product_statistics_id <- id
    config.data$general.installedDate <- convert.unix.to.date(config.data$general.installedDate)
    config_list[[i]] <- config.data
  }
  
  if(nrow(events.data) > 0) {
    events.data$product_statistics_id <- id
    events.data$sinkCount <- as.integer(events.data$sinkCount)
    events_list[[i]] <- events.data
  }
  
  if(nrow(events.rules.data) > 0) {
    events.rules.data$product_statistics_id <- id
    events_rules_list[[i]] <- events.rules.data
  }
  
  if(nrow(interfaces.data) > 0) {
    interfaces.data$product_statistics_id <- id
    interfaces_list[[i]] <- interfaces.data
  }
  
  if(nrow(status.data) > 0) {
    status.data$product_statistics_id <- id
    status.data$intervalEndTime <- convert.unix.to.date(status.data$intervalEndTime)
    status.data$intervalStartTime <- convert.unix.to.date(status.data$intervalStartTime)
    status.data$serverStartedTime <- convert.unix.to.date(status.data$serverStartedTime)
    status_list[[i]] <- status.data
  }
  
  if(nrow(sync.data) > 0) {
    sync.data$product_statistics_id <- id
    sync_list[[i]] <- sync.data
  }
}

# Bind rows at the end
df.master <- bind_rows(master_list)
df.auth <- bind_rows(auth_list)
df.auth.list <- bind_rows(auth_list_data_list)
df.config <- bind_rows(config_list)
df.events <- bind_rows(events_list)
df.events.rules <- bind_rows(events_rules_list)
df.interfaces <- bind_rows(interfaces_list)
df.status <- bind_rows(status_list)
df.sync <- bind_rows(sync_list)

names(df.sync) <- clean.names(names(df.sync))
names(df.status) <- clean.names(names(df.status))
names(df.interfaces) <- clean.names(names(df.interfaces))
names(df.events) <- clean.names(names(df.events))
names(df.events.rules) <- clean.names(names(df.events.rules))
names(df.config) <- clean.names(names(df.config))
names(df.auth.list) <- clean.names(names(df.auth.list))
names(df.auth) <- clean.names(names(df.auth))
names(df.master) <- clean.names(names(df.master))

upload.to.bigquery(df.sync,'Cerberus_Health_Service','sync')
upload.to.bigquery(df.status,'Cerberus_Health_Service','status')
upload.to.bigquery(df.interfaces,'Cerberus_Health_Service','interfaces')
upload.to.bigquery(df.events,'Cerberus_Health_Service','events')
upload.to.bigquery(df.events.rules,'Cerberus_Health_Service','event_rules')
upload.to.bigquery(df.config,'Cerberus_Health_Service','config')
upload.to.bigquery(df.auth.list,'Cerberus_Health_Service','auth_list')
upload.to.bigquery(df.auth,'Cerberus_Health_Service','auth')
upload.to.bigquery(df.master,'Cerberus_Health_Service','Product_Statistics')


dbDisconnect(con)
dbDisconnect(jscape.con)
dbDisconnect(cerb.con)

# ##############
# 
# for (i in 1:nrow(zzz)) {
#   print(paste0(i," of ",nrow(zzz),": ",zzz$id[i]))
#   id <- zzz$id[i]
#   # print("fromJson")
#   master.data <- fromJSON(zzz$data[i],flatten = TRUE)
#   auth.data <- master.data$auth
#   auth.list.data <- convert.if.can(auth.data$authList)
#   auth.data$authList <- NULL
#   auth.data <- convert.if.can(auth.data)
#   master.data$auth <- NULL
#   
#   
#   config.data <- convert.if.can(master.data$config)
#   master.data$config <- NULL
#   
#   events.data <- master.data$events
#   events.rules.data <- events.data$rules
#   events.data$rules <- NULL
#   events.data <- convert.if.can(events.data)
#   master.data$events <- NULL
#   events.data$rules <- NULL
#   events.data <- convert.if.can(events.data)
#   
#   interfaces.data <- convert.if.can(master.data$interfaces)
#   
#   # interfaces.data <- data.frame(
#   #   interface = interfaces.data$type,
#   #   active = interfaces.data$active,
#   #   stringsAsFactors = FALSE
#   # )
#   master.data$interfaces <- NULL
#   
#   status.data <- convert.if.can(master.data$status)
#   master.data$status <- NULL
#   
#   sync.data <- convert.if.can(master.data$sync)
#   master.data$sync <- NULL
#   
#   master.data <- convert.if.can(master.data)
#   # convert columns to dates
#   
#   # print("Binding")
#   if(nrow(master.data)>0){
#     master.data$product_statistics_id <- id
#     master.data$creationDate <- convert.unix.to.date(master.data$creationDate)
#     if(exists("df.master")){
#       df.master <- bind_rows(df.master,master.data)
#     }else{
#       df.master <- master.data
#     }
#   }
#   if(nrow(auth.data)>0){
#     auth.data$product_statistics_id <- id
#     if(exists("df.auth")){
#       df.auth <- bind_rows(df.auth,auth.data)
#     }else{
#       df.auth <- auth.data
#     }
#   }
#   if(nrow(auth.list.data)>0){
#     auth.list.data$product_statistics_id <- id
#     if(exists("df.auth.list")){
#       df.auth.list <- bind_rows(df.auth.list,auth.list.data)
#     }else{
#       df.auth.list <- auth.list.data
#     }
#   }
#   if(nrow(config.data)>0){
#     config.data$product_statistics_id <- id
#     config.data$general.installedDate <- convert.unix.to.date(config.data$general.installedDate)
#     if(exists("df.config")){
#       df.config <- bind_rows(df.config,config.data)
#     }else{
#       df.config <- config.data
#     }
#   }
#   if(nrow(events.data)>0){
#     events.data$product_statistics_id <- id
#     events.data$sinkCount <- as.integer(events.data$sinkCount)
#     if(exists("df.events")){
#       df.events <- bind_rows(df.events,events.data)
#     }else{
#       df.events <- events.data
#     }
#   }
#   if(is.list(events.rules.data) ||nrow(events.rules.data)>0){
#     events.rules.data$product_statistics_id <- id
#     if(exists("df.events.rules")){
#       df.events.rules <- bind_rows(df.events.rules,events.rules.data)
#     }else{
#       df.events.rules <- events.rules.data
#     }
#   }
#   if(nrow(interfaces.data)>0){
#     interfaces.data$product_statistics_id <- id
#     if(exists("df.interfaces")){
#       df.interfaces <- bind_rows(df.interfaces,interfaces.data)
#     }else{
#       df.interfaces <- interfaces.data
#     }
#   }
#   if(nrow(status.data)>0){
#     status.data$product_statistics_id <- id
#     status.data$intervalEndTime <- convert.unix.to.date(status.data$intervalEndTime)
#     status.data$intervalStartTime <- convert.unix.to.date(status.data$intervalStartTime)
#     status.data$serverStartedTime <- convert.unix.to.date(status.data$serverStartedTime)
#     if(exists("df.status")){
#       df.status <- bind_rows(df.status,status.data)
#     }else{
#       df.status <- status.data
#     }
#   }
#   if(nrow(sync.data)>0){
#     sync.data$product_statistics_id <- id
#     if(exists("df.sync")){
#       df.sync <- bind_rows(df.sync,sync.data)
#     }else{
#       df.sync <- sync.data
#     }
#   }
# 
# } # close loop


