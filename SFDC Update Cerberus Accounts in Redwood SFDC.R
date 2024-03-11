source('ActiveCo Functions.R')

# pull a list of all the active accounts from Cerberus
seed <- query.bq(paste0("
select
r.Id as OneRedwood_Account,
r.Application_Areas__c,
r.Domain_Match__c Domain_Match,
r.Cerberus_Account_Id__c as og_Cerb_Id,
string_agg(c.Id,',') as Cerb_Accounts_Matched,
count(*) as Count_Cerb_Accounts_Matched
from `skyvia.Account` r
 left join `CerberusSFDC.Account` c on r.Domain_Match__c = c.Domain__c
where c.Account_Status__c = 'Active Account'
or r.Application_Areas__c like '%Cerberus%'
or Cerberus_Account_Id__c is not null
group by 1,2,3,4"))

# take a snapshot of the origional data
og <- seed[,c("OneRedwood_Account","Application_Areas__c","Cerb_Accounts_Matched")]

# make a list of OneRedwood Accounts where there is no match in Cerb
up <- og[which(is.na(og$Cerb_Accounts_Matched) | !grepl('Cerberus',og$Application_Areas__c)),]
churned <- seed$OneRedwood_Account[which(is.na(seed$Cerb_Accounts_Matched))]

which.churned <- which(up$OneRedwood_Account %in% churned)

# add cerberus with a semicolon to accounts that already have application areas
up$Application_Areas__c[which(!is.na(up$Application_Areas__c))] <- paste0(up$Application_Areas__c[which(!is.na(up$Application_Areas__c))],';Cerberus')
# add stand alone Cerberus to accounts that do not already have any application areas
up$Application_Areas__c[which(is.na(up$Application_Areas__c))] <- 'Cerberus'
names(up)[1] <- 'id'

up$Application_Areas__c <- gsub(";Cerberus","",up$Application_Areas__c)

# remove Cerberus from the churned accounts
up$Application_Areas__c[which.churned] <- gsub(';Cerberus','',up$Application_Areas__c[which.churned])
up$Application_Areas__c[which.churned] <- gsub('Cerberus','',up$Application_Areas__c[which.churned])
up$Cerb_Accounts_Matched <- NULL




# only upload records that changed
og$helper <- paste0(og$OneRedwood_Account,og$Application_Areas__c)
up$helper <- paste0(up$id,up$Application_Areas__c)

up <- up[which(!(up$helper %in% og$helper)),]
og <- og[which(og$OneRedwood_Account %in% up$id),]

up$Application_Areas__c[which(up$Application_Areas__c == "")] <- NA

up$helper <- NULL
og$helper <- NULL

if(nrow(up)>0){
print("Uploading to SFDC")
  make.sfdc.update(
    "Update Cerberus Customers in OneRedwood",
    og,
    up,
    'Account',
    "Updating Cerberus Customers. See SFDC Update Cerberus Accounts in Redwood SFDC.R"
  )
}

# # upload matched accounts to OneRedwood
# act.up <- seed[,c("OneRedwood_Account","Cerb_Accounts_Matched")]
# og.act.up <- seed[which(seed$OneRedwood_Account %in% act.up$OneRedwood_Account),c("OneRedwood_Account","og_Cerb_Id","Cerb_Accounts_Matched")]
# # only upload new data to SFDC
# og.act.up <- og.act.up[which(is.na(og.act.up$og_Cerb_Id) | (og.act.up$og_Cerb_Id != og.act.up$Cerb_Accounts_Matched)),]
# og.act.up$helper <- paste0(og.act.up$OneRedwood_Account,og.act.up$Cerb_Accounts_Matched)
# act.up$helper <- paste0(act.up$OneRedwood_Account,act.up$Cerb_Accounts_Matched)
# act.up <- act.up[which(!act.up$helper %in% og.act.up$helper),]
# 
# og.act.up <- og.act.up[which(og.act.up$OneRedwood_Account %in% act.up$OneRedwood_Account),]
# 
# act.up <- act.up[,c("OneRedwood_Account","Cerb_Accounts_Matched")]
# names(act.up) <- c('id','Cerberus_Account_Id__c')
# 
# if(nrow(act.up)>0)
# og.act.up <- og.act.up[which(og.act.up$OneRedwood_Account %in% act.up$id),]
# 
# print("Uploading to SFDC")
# if(nrow(act.up)>0){
# make.sfdc.update(
#   "Update Cerberus Customer AccountIds in OneRedwood",
#   og.act.up,
#   act.up,
#   'Account',
#   "Updating Cerberus Customers Matched Accounts. See SFDC Update Cerberus Accounts in Redwood SFDC.R"
# )
# }
# 
# 
