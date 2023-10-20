source('ActiveCo Functions.R')
library(RMySQL)
library(odbc)

# create a database connection object
con <- dbConnect(MySQL(),
                 user=CERBERUS_ORDER_MANAGER_USR,
                 password=CERBERUS_ORDER_MANAGER_PASS,
                 dbname='cerberusorders', host='aurora.ceoyeksjv4wj.us-east-1.rds.amazonaws.com',
                 port = 3306)

ssms.con <- dbConnect(odbc(),
                 Driver = "SQL Server",
                 Server = "WIN-6PDIPKC0EER",
                 Database = "CerberusOrderManager",
                 UID = SSMS_USR,
                 PWD = SSMS_PASS)

# check the connection is working
# dbListTables(con)

om.tabs <- dbListTables(con)

# run a complete refresh of everything only when Bigquery is 7 days behind
last.update <- query.bq("
SELECT
  TIMESTAMP_MILLIS(last_modified_time) AS last_modified_time
FROM
  `activeco.CerberusOrderManager.__TABLES__`
WHERE
  table_id = 'audit_log'")

if(last.update$last_modified_time > Sys.Date() - 7){
  short.list <- c(
    "assets",
    "contract_renewal_discount",
    "contract_renewal_links",
    "contracts",
    "customers",
    "license_info",
    "line_item",
    "orders",
    "product",
    "quotes",
    "serialnumbers",
    'users'
  )
  sanitized.tabs <- om.tabs[grep("sanitized",om.tabs)]
  short.list <- unique(c(sanitized.tabs,short.list))
  # short.list %in% om.tabs
  om.tabs <- om.tabs[which(om.tabs %in% short.list)]
}

upload_OM_to_bigquery <- function(object.name,upload.name = NA,om.table=NA){
  # object.name <- 'serialnumbers'
  # date.cols <- c('FirstInstallDate')
  if(!is.data.frame(om.table)){
    print("Query MySQL")
    z <- dbGetQuery(con,paste0('select * from ',object.name))
  }else{
    z <- om.table
  }
  
  # for(d in date.cols){
  #   # d <- date.cols[1]
  #   z[,d] <- as_datetime(z[,d])
  #   z[which(is.na(z[,d])),d] <- as_datetime('1970-01-01 00:00:00.00')
  # }
  
  if(is.na(upload.name)){
    upload.name <- object.name
  }
  print("Send to Bigquery")
  upload.to.bigquery(z,'CerberusOrderManager',upload.name)
  
}

upload_OM_to_SQL_Server <- function(sql.server.con,object.name,upload.name = NA,om.table = NA){
  # object.name <- om.tabs[1]
  # date.cols <- c('FirstInstallDate')
  # sql.server.con <- ssms.con
  if(!is.data.frame(om.table)){
    print("Query MySQL")
  z <- dbGetQuery(con,paste0('select * from ',object.name))
  }else{
  z <- om.table
  }
  # for(d in date.cols){
  #   # d <- date.cols[1]
  #   z[,d] <- as_datetime(z[,d])
  #   z[which(is.na(z[,d])),d] <- as_datetime('1970-01-01 00:00:00.00')
  # }
  
  if(is.na(upload.name)){
    upload.name <- object.name
  }
  print("Send to SSMS")
  dbWriteTable(sql.server.con,object.name,z,overwrite = TRUE)
  
}

######### ACCOUNTS #######
for (t in om.tabs) {
  
  # t <- om.tabs[1]
  print(paste0(which(om.tabs == t)," of ",length(om.tabs),": ",t))
  print("Query MySQL")
  ot <- dbGetQuery(con,paste0('select * from ',t))
  
  upload_OM_to_bigquery(t,om.table = ot)
  upload_OM_to_SQL_Server(ssms.con,t,om.table = ot)

}
dbDisconnect(con)
dbDisconnect(ssms.con)


