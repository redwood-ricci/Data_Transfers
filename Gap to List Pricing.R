print("Starting Gap to List")
source('ActiveCo Functions.R')
# source('Update Bigquery with ARR Table.R')
library(tidyr)
library(lubridate)
library(googlesheets4)
library(dplyr)
sf_auth(cache = ".httr-oauth-salesforcer-asci") # authorize connection
sheet.link <- "https://docs.google.com/spreadsheets/d/1aWg0JKEJKGgdfxJsY1eDqL4Pob8ePW7i2MzJuxZnAos/edit#gid=1499690062"

seed <- query.bq(
  paste0(
    "
-- account arr
with arr as (
select
    AccountId,
    ARR_Product,
    sum(Overall_ARR) as ARR
from activeco.R_Data.Customer_ARR arr
left join skyvia.Opportunity o on arr.Opportunity_ID = o.Id
left join skyvia.Account a on o.AccountId = a.Id
group by 1,2
),

-- ab jobs per account
Active_Batch_Jobs as (
    select
    SalesforceID
   , sa.Name
   , round(sum(AvgSucceededPerDay * 365.25)) as AB_jobs_per_year
   , COUNT(distinct js.RegisteredJssId) as AB_Schedulers
    from ASCI_Health_Service.LatestCaptures lc
    left join ASCI_Health_Service.JobSchedulers js on lc.RegisteredJssId = js.RegisteredJssId
    left join ASCI_Health_Service.Accounts a on js.AccountID = a.AccountID
    left join skyvia.Account sa on sa.Id = a.SalesforceID
    group by 1, 2
),
-- rmj jobs per account
RunMyJobs_Jobs as (
select
    c.salesforceAccountId,
    usage.rmjPortalId,
    round(AVG(jobs_per_day) * 365) as RMJ_jobs_per_year-- average per day across all environmen ts
       from (
-- first get sum across environments
                  select rmjPortalId,
                         human_start,
                         sum(jobExecutions) as jobs_per_day
                  from ContractServer.SaaS_Usage usage
                  where human_start >= cast(CURRENT_DATE() - 100 as timestamp)
                    and human_start < cast(CURRENT_DATE() - 3 as timestamp)
                    and jobExecutions > 10 -- usage period in SaaS usage is 1 day
                  group by 1, 2) usage
left join ContractServer.Customers c on usage.rmjPortalId = c.rmjPortalId
group by 1,2
),

ever_flipped as (
    select AccountId
    from skyvia.Opportunity
    where Warboard_Category__c in ('Flips - Term License','Flips - Maintenance')
    and StageName = 'Closed Won'
)

select
    arr.AccountId,
    a.Name,
    arr.ARR_Product,
    arr.ARR,
    case when arr.AccountId in (select AccountId from ever_flipped) then TRUE else FALSE end as Flipped,
    '' as Config_Type,
    ab.AB_Schedulers,
    ab.AB_jobs_per_year,
    rmj.RMJ_jobs_per_year,
from arr
left join Active_Batch_Jobs ab on arr.AccountId = ab.SalesforceID and arr.ARR_Product = 'ActiveBatch'
left join RunMyJobs_Jobs rmj on arr.AccountId = rmj.salesforceAccountId and arr.ARR_Product = 'RMJ'
left join skyvia.Account a on arr.AccountId = a.Id
where ARR_Product in ('ActiveBatch','RMJ')
and arr.ARR > 0
order by 2
    "
  ))


# multiply AB jobs per year by 2 to account for job chains vs 'instances'
seed$AB_jobs_per_year <- seed$AB_jobs_per_year * 2

seed$total_yearly_jobs <- rowSums(seed[,c("AB_jobs_per_year","RMJ_jobs_per_year")],na.rm = T)

tiers <- read_sheet(
  sheet.link,
  sheet = 'Pricing_Format'
)

# 0013t00002LzTRxAAN

closest_pos_idx <- function(x, y) {
  # x <- 1172579
  # y <- tiers$Annual_Executions_Up_To
  diff <- y - x
  pos_diff_idx <- which(diff <= 0)
  if (length(pos_diff_idx) == 0) {
    return(1)
  } else {
    return(pos_diff_idx[which.max(diff[pos_diff_idx])])
  }
}

seed$Tier <- tiers$Tier[sapply(seed$total_yearly_jobs, function(x) closest_pos_idx(x, tiers$Annual_Executions_Up_To))]

# min Tier for RMJ is 4
seed$Tier[which(seed$Tier < 2 & seed$ARR_Product == "RMJ")] <- 3

gaps <- merge(seed,tiers, by = 'Tier',all.x = TRUE)

gaps$job_Overage <-   gaps$total_yearly_jobs - gaps$Annual_Executions_Up_To

gaps$job_Overage[which(gaps$job_Overage<0)] <- 0

editions <- c(
  "Standard",
  "Professional",
  "Enterprise"
)

edition.cols <- c()
for (e in editions) {
# e <- "Professional"

base <- paste0(e,"_Base")
overage <- paste0(e,"_Overage_Charge")
list <- paste0(e,"_List")
gap <- paste0(e,"_Gap_to_List")

gaps[,base] <- gaps[paste0("Base_",e)] 
gaps[,overage] <- gaps$job_Overage * gaps[,paste0("Overage_",e)]
gaps[,list] <- gaps[,base] + gaps[,overage]
gaps[,gap] <- gaps$ARR - gaps[,list]
 
edition.cols <- c(edition.cols,base,overage,list,gap)
}

list.prices <- edition.cols[grepl("Gap_to_List",edition.cols)]

max_vals <- apply(gaps[,list.prices], 1, function(x) {
  max_val <- max(x)
  max_pos <- which(x == max_val)
  return(list(max_val, max_pos))
})

# convert the list to a data frame
max_df <- as.data.frame(do.call(rbind, max_vals))
colnames(max_df) <- c("Lowest Cost Gap To List", "Lowest Cost Edition")
max_df$`Lowest Cost Gap To List` <- as.numeric(max_df$`Lowest Cost Gap To List`)
max_df$`Lowest Cost Edition` <- as.character(max_df$`Lowest Cost Edition`)
max_df$`Lowest Cost Edition`[which(grepl("1",max_df$`Lowest Cost Edition`))] <- "Standard"
max_df$`Lowest Cost Edition`[which(grepl("2",max_df$`Lowest Cost Edition`))] <- "Professional"
max_df$`Lowest Cost Edition`[which(grepl("3",max_df$`Lowest Cost Edition`))] <- "Enterprise"

gaps <- cbind(gaps,max_df)

share <- gaps

share$Name <- asci.hyperlinks(share$AccountId,share$Name)
share <- share[which(share$AccountId!='0013t00002N6imrAAB'),] # remove SAP account


share[,c("AccountId","AB_jobs_per_year","RMJ_jobs_per_year","Base_Standard","Base_Professional","Base_Enterprise","Overage_Standard","Overage_Professional","Overage_Enterprise",
        "Standard_Base","Standard_Overage_Charge","Professional_Base","Professional_Overage_Charge",
        "Enterprise_Base","Enterprise_Overage_Charge")] <- NULL

share <- share[order(share$ARR,decreasing = TRUE),]

names(share) <- gsub("_"," ",names(share))
names(share)[which(names(share) == "Name")] <- "Account Name"
names(share)[which(names(share) == "Tier")] <- "Usage Tier"
names(share)[which(names(share) == "total yearly jobs")] <- "Total Yearly Jobs"
names(share)[which(names(share) == "Annual Executions Up To")] <- "Annual Executions Limit"
names(share)[which(names(share) == "job Overage")] <- "Estimated Overage"

write_sheet_keep_sheet_format(share,
            sheet.link,
            sheet = 'Gap to List')


# upload the gap to list to SFDC
acts <- string.in.for.query(gaps$AccountId)
og <- query.bq(paste0("
select Id, Gap_to_List_Price__c from skyvia.Account
where Id in (
",acts,"
) or Gap_to_List_Price__c > 0
"))

upload <- og
upload$Gap_to_List_Price__c <- NULL
gap.merge <- gaps[,c("AccountId","Professional_Gap_to_List"),] # "Professional Gap to List"
names(gap.merge) <- c("Id","Gap_to_List_Price__c")

upload <- merge(upload,gap.merge, by = 'Id', all.x = TRUE)

# upload$Id[duplicated(upload$Id)]
# customers with more than one product get their gap to list summed together at the account level
upload <- upload %>%
  group_by(Id) %>%
  summarise(Gap_to_List_Price__c = sum(Gap_to_List_Price__c,na.rm = T))

make.sfdc.update(
  "Updating Gap to List Prices",
  og,
  upload,
  'Account',
  "Uploading latest gap to list values. See Gap to List Pricing.R"
)

# probably needs usage numbers to make this make sense.
# also ARR data is mismatched between analytics db and SFDC # needs resolve
