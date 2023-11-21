source('ActiveCo Functions.R')

JSCAPE.Health <- query.bq(
"
with calls_home as (
SELECT
distinct
  customerId
  ,ps.creationDate as Last_Call_Home
  ,version as latest_server_version
  ,REGEXP_EXTRACT(customerId, r'([A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12})') AS Serial_Number_id
FROM `JSCAPE_Health_Service.product_statistics` ps
left join `JSCAPE_Health_Service.PS_Data` psd on ps.id = psd.product_statistics_id
where psd.productType = 'MFT_SERVER'
and lower(version) not like '%beta%'
and cast(ps.creationDate as date) >= date_sub(current_date(), INTERVAL 1 YEAR)
),

customers as (
select
c.*
,coalesce(a.AccountId,case when length(c.customerId)=18 then c.customerId else null end) as SF_AccountId -- only use customerIds if they are 18 digits (sfdc ids)
from calls_home c
left join `skyvia.Asset` a on a.SerialNumber = c.Serial_Number_id
),

last_call as (
  select
  *
  ,ROW_NUMBER() OVER (PARTITION BY SF_AccountId ORDER BY Last_Call_Home DESC) AS rn
  from customers
)

select * from last_call where rn = 1 and SF_AccountId is not null
")
JSCAPE.Health$rn <- NULL
og <- query.bq(paste0(
"
select
Id
,JSCAPE_Last_Call_Home__c
,JSCAPE_Version__c
from `skyvia.Account`
where id in (
 ",string.in.for.query(JSCAPE.Health$SF_AccountId)," 
)
"
))

missing.customers <- JSCAPE.Health[which(!(JSCAPE.Health$SF_AccountId %in% og$Id)),]

if(length(missing.customers)>0){
  print("uploading missing JSCAPE customers")
  write_sheet_keep_sheet_format(missing.customers,
                              "https://docs.google.com/spreadsheets/d/1B59CrHYX14q35lB5CrFtZd_EGFDtNNA21yOrUJ6gUTs/edit#gid=0",
                              'JSCAPE Mismatched Customers')
}

print("updating SFDC")
upload <- JSCAPE.Health[,c("SF_AccountId","Last_Call_Home","latest_server_version")]
names(upload) <- c("Id","JSCAPE_Last_Call_Home__c","JSCAPE_Version__c")

JSCAPE.Health.Update.Result <- sf_update(
  upload,
  'Account',
  verbose = TRUE,
  api_type = "Bulk 2.0"
)

print(JSCAPE.Health.Update.Result)

View(JSCAPE.Health.Update.Result)