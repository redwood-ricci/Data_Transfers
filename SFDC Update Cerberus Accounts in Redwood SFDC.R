source('ActiveCo Functions.R')

seed <- query.bq(paste0("
select
r.Id as OneRedwood_Account,
r.Application_Areas__c,
r.Domain_Match__c Domain_Match,
string_agg(c.Id,',') as Cerb_Accounts_Matched,
count(*) as Count_Cerb_Accounts_Matched
from `skyvia.Account` r
 left join `CerberusSFDC.Account` c on r.Domain_Match__c = c.Domain__c
where c.Account_Status__c = 'Active Account'
or r.Application_Areas__c like '%Cerberus%'
group by 1,2,3"))

og <- seed[,c("OneRedwood_Account","Application_Areas__c")]
churned <- seed$OneRedwood_Account[which(is.na(seed$Cerb_Accounts_Matched))]
which.churned <- which(up$id %in% churned)

up <- og[which(!grepl('Cerberus',og$Application_Areas__c,ignore.case = TRUE) | is.na(og$)),]

# add cerberus with a semicolon to accounts that already have application areas
up$Application_Areas__c[which(!is.na(up$Application_Areas__c))] <- paste0(up$Application_Areas__c[which(!is.na(up$Application_Areas__c))],';Cerberus')
# add stand alone Cerberus to accounts that do not already have any application areas
up$Application_Areas__c[which(is.na(up$Application_Areas__c))] <- 'Cerberus'
names(up)[1] <- 'id'


up$Application_Areas__c <- gsub(";Cerberus","",up$Application_Areas__c)


print("Uploading to SFDC")
make.sfdc.update(
  "Update Cerberus Customers in OneRedwood",
  og,
  up,
  'Account',
  "Updating Cerberus Customers. See SFDC Update Cerberus Accounts in Redwood SFDC.R"
)
