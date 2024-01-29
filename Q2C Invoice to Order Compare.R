# check sandbox transition:
source('ActiveCo Functions.R')

invoices <- query.bq(
  "
  SELECT 
  I.Id as InvoiceId
 ,I.Related_Account__c AS RelatedAccountID
 ,A.Region__c AS Region__c
 ,A.Account_Segment__c
 ,I.Name AS InvoiceNumber
 ,I.InvoiceType__c AS InvoiceType
 ,I.Invoice_Status__c AS InvoiceStatus
 ,case when O.Product__c is null then 'Default RMJ' else O.Product__c end as Product__c
 ,I.Related_Opportunity__c AS RelatedOpportunityId
 ,O.Name AS RelatedOpportunity
 ,I.Order_Form_Number__c AS OrderFormNumber
 ,I.Billing_Period_Start__c AS BillingPeriodStart
 ,I.Billing_Period_End__c AS BillingPeriodEnd
 ,I.Paid_Date__c AS PaidDate
 ,I.Paid_Amount__c AS PaidAmount
 ,round(I.Amount__c / C.ConversionRate) AS value
 ,cast(null as int) as Recurring_Booking_PY__c
 ,I.Amount__c / C.ConversionRate AS Manager_s_Forecast__c
 ,'Srini K' as leader_group
 ,'WW Billings' as leader_product_group
 
FROM `activeco.skyvia.Invoice__c` I
LEFT JOIN  `activeco.skyvia.Account` A ON I.Related_Account__c = A.id
LEFT JOIN  `activeco.skyvia.CurrencyType` C ON I.CurrencyIsoCode = C.IsoCode 
LEFT JOIN `activeco.skyvia.Opportunity` O ON I.Related_Opportunity__c = O.Id 
-- LEFT JOIN `activeco.skyvia.User` U ON I.OwnerId = U.Id 
WHERE I.Test_Account__c IS FALSE
      AND I.Billing_Period_Start__c >= '2022-01-01'
      -- AND I.InvoiceType__c IN ('Annual Installment Billing','Prorated Installments',
      --                       'Annual Booking', 'Quarterly Installment Billing','Monthly Installment Billing', 'Royalty' )
      -- AND (I.Invoice_Status__c IN ('Draft', 'Under Review', 'Reviewed', 'Sent') or I.Invoice_Status__c is null );

-- select case when Order_Migration_id__c is not null then EffectiveDate else Order_Start_date__c end as start_date from `skyvia.Order` where Workday_Contract_Number__c = 'CON-10002368'
      
  "
)

orders <- query.bq(
  "
  select
o.id
,o.AccountId
,o.Account_Name__c
,a.Region__c
,a.Account_Segment__c
,o.Account_Type__c
,o.Workday_Invoice_Id__c
,o.Invoice_Status__c
,o.Status as Order_Status
,case when o.Product__c is null then 'Default RMJ' else o.Product__c end as Product__c
,o.OpportunityId as RelatedOpportunityId
,oppo.Name as RelatedOpportunity
,o.Order_Form_Number__c
,o.Billing_Period_Start__c
,o.Billing_Period_End__c
,o.Order_Start_Date__c
,o.Order_End_Date__c
,o.EffectiveDate
,o.EndDate
,o.ActivatedDate
,null as paid_amount
,round(o.TotalAmount / ct.conversionrate) as value
,o.Invoice_Type__c
,o.Status
,o.Invoice__c
from `skyvia.Order` o
left join `skyvia.CurrencyType` ct on o.CurrencyIsoCode = ct.IsoCode
left join `skyvia.Account` a on o.AccountId = a.Id
left join `skyvia.Opportunity` oppo on o.OpportunityId = oppo.Id
where o.EffectiveDate >= '2022-01-01'
and o.Invoice__c is not null
and o.Test_Account__c = FALSE
-- AND o.Invoice_Type__c IN ('Annual Installment Billing','Prorated Installments',
--                             'Annual Booking', 'Quarterly Installment Billing','Monthly Installment Billing', 'Royalty' )
--       AND (o.Invoice_Status__c IN ('Draft', 'Under Review', 'Reviewed', 'Sent') or o.Invoice_Status__c is null )
-- order by o.OpportunityId
  "
)

# table(orders$Invoice__c %in% invoices$InvoiceId)
# table(invoices$InvoiceId %in% orders$Invoice__c)
# View(invoices[which(!(invoices$InvoiceId %in% orders$Invoice__c)),])

# why are these missing?
# 4 are royalty, only one is legit missing
missing.invoices <- invoices[which(!(invoices$InvoiceId %in% orders$Invoice__c)),]

# drop out the missing ones to get a fair comparison if we can fix the missing rows
invoices <- invoices[which(!(invoices$InvoiceId %in% missing.invoices$InvoiceId)),]

invoices$quarter <- invoices$BillingPeriodStart# quarter(invoices$BillingPeriodStart,with_year = TRUE)
invoices$rpt_group <- "Contracted"
invoices$rpt_group[which(invoices$InvoiceType == 'Royalty')] <- 'SAP - Royalty'
invoices$won_validated <- FALSE
invoices$won_validated[which(invoices$InvoiceStatus == 'Sent')] <- TRUE

orders$quarter <- orders$EffectiveDate# quarter(orders$EffectiveDate,with_year = TRUE)
orders$rpt_group <- "Contracted"
orders$rpt_group[which(orders$Invoice_Type__c == 'Royalty')] <- 'SAP - Royalty'
orders$won_validated <- FALSE
orders$won_validated[which(orders$Invoice_Status__c == 'Sent')] <- TRUE

# make CTB table to check totals

ctb.orders <- orders %>%
  group_by(quarter,Product__c,rpt_group,Region__c,Account_Segment__c,AccountId) %>%
  summarise(
    Order_Won= sum(value[which(won_validated == T)],na.rm = T),
    Order_Forecast = sum(value[which(won_validated == F)],na.rm = T),
    Order_n = length(id)
  ) %>%
  mutate(helper = paste0(quarter,Product__c,rpt_group,Region__c,Account_Segment__c,AccountId))

ctb.orders <- ctb.orders[,c("Order_Won","Order_Forecast","Order_n","helper")]

ctb.invoices <- invoices %>%
  group_by(quarter,Product__c,rpt_group,Region__c,Account_Segment__c,RelatedAccountID) %>%
  summarise(
    Invoice_Won = sum(value[which(won_validated == T)],na.rm = T),
    Invoice_Forecast = sum(value[which(won_validated == F)],na.rm = T),
    Invoice_n = length(InvoiceId)
  ) %>%
  mutate(helper = paste0(quarter,Product__c,rpt_group,Region__c,Account_Segment__c,RelatedAccountID))

ctb.invoices <- ctb.invoices[,c("Invoice_Won","Invoice_Forecast","Invoice_n","helper")]

master.check <- merge(ctb.orders,ctb.invoices,by='helper',all = T)

master.check$won_check <- na.subtraction(master.check$Order_Won,master.check$Invoice_Won)
master.check$forecast_check <- na.subtraction(master.check$Order_Forecast,master.check$Invoice_Forecast)
master.check$n_check <- na.subtraction(master.check$Order_n,master.check$Invoice_n)

orders$helper <- apply(orders[,c("quarter","Product__c","rpt_group","Region__c","Account_Segment__c","AccountId")], 1, function(x) paste0(x, collapse = ""))
invoices$helper <- apply(invoices[,c("quarter","Product__c","rpt_group","Region__c","Account_Segment__c","RelatedAccountID")], 1, function(x) paste0(x, collapse = ""))


sheet.link <- "https://docs.google.com/spreadsheets/d/12WWrWLWhB2tyzpSa8MQu0AEk21rLJjGuQrXAjwIRZFQ/edit#gid=0"

write_sheet(missing.invoices,
            sheet.link,
            "Missing Invoices Prod"
            )
write_sheet(master.check,
            sheet.link,
            "Master Check Prod")

write_sheet(orders,
            sheet.link,
            "Orders Prod")

write_sheet(invoices,
            sheet.link,
            "Invoices Prod")









