# load library for accessing odbc connections
source("ActiveCo Functions.R")
library(odbc)
library(googlesheets4)

# set connection to SSMS
ssms.con <- dbConnect(odbc(),
                      Driver = "SQL Server",
                      Server = "WIN-6PDIPKC0EER",
                      Database = "Source_Production_SALESFORCE",
                      UID = SSMS_USR,
                      PWD = SSMS_PASS)

# paste a query to execute against SSMS
query <- "
--drop table #Deals;
select
	a.AccountId,
	cast(null as varchar(100)) as Account_Owner,
	cast(null as varchar(50)) as In_Production__c,
	b.Name as Owner,
	c.Name as PS_Team_Lead,
	a.id,
	a.Name,
	Related_Opportunity__c,
	cast(CloseDate as date) as CloseDate,
	cast(SAO_Date__C as date) as SAO_Date__C,
	Software_Qualified_Bookings__c,
	PS_Qualified_Bookings__c,
	a.Type as Opp_Type,
	a.Expansion_Category__c,
	Product__c,
	Owner_s_Manager__c,
	PS_Team_Lead__c,
	Is_This_A_Migration__c as PS_Requirements__c,
	cast(null as varchar(75)) as PS_Team_Lead_Name,
	cast(null as varchar(150)) as Related_Opportunity_Name,
	cast(null as varchar(50)) as Related_Opportunity_Stage,
	cast(null as varchar(50)) as Related_Opportunity_type,
	cast(null as numeric) as Related_Opportunity_Amount,	
	cast(null as Date) as Related_Opportunity_CloseDate,	
	cast(null as varchar(18)) as Task_id,
	cast(null as varchar(50)) as Task_Status,
	cast(null as varchar(150)) as Task_Subject,
	cast(null as date) as ActivityDate,
	cast(null as varchar(18)) as Task_OwnerId,
	cast(null as varchar(100)) as Task_Owner__c,
	cast(null as varchar(50)) as Task_Product__C,
	cast(null as varchar(100)) as pse_Name,
	cast(null as varchar(50)) as pse__Project_Status__c,
	cast(null as date) as pse__CreatedDate,
	cast(null as varchar(80)) as pse__Project_Type__c,
	cast(null as varchar(50)) as pse__Stage__c,
	cast(null as date) as pse__Start_Date__C,
	cast(null as varchar(18)) as pse_Project_Owner_Id__c, --OwnerId
	cast(null as varchar(50)) as pse_Project_Owner__c, --Owner_Name
	cast(null as varchar(50)) as Engagement_Manager__c,
	Migration__c
into #Deals
from salesforce.cdata.salesforce.opportunity a
inner join salesforce.cdata.salesforce.[User] b
on a.Ownerid = b.id
left outer join salesforce.cdata.salesforce.[User] c
on a.PS_Team_Lead__c = c.id
where (type = 'New Business' or (type = 'Existing Business' and Expansion_Category__C in ('SAP Flip')))
and StageName = 'Closed Won'
--and (product__C in ('RunMyJobs', 'FA', 'Financial Force')    --> Filter on Product
	--or ARR_Product__c in ('RunMyJobs','Finance Automation')  --> Filter on Product
and cast(closedate as date) > '2021-12-31'
and Product__c not in ('Cerberus')
--and a.Accountid in (select id from salesforce.cdata.salesforce.account where Active_Contract_Products__c is not null)
;

Delete from #Deals where Accountid in (select id from salesforce.cdata.salesforce.account where Active_Contract_Products__c is null);

update a
set a.Related_Opportunity__c = b.id
from #Deals a,
	 salesforce.cdata.salesforce.opportunity b
where a.id = b.Related_Opportunity__c
and b.type = 'Professional Services'
and a.Related_Opportunity__c is null
;

update #Deals set related_opportunity__c = '0063t000012WvIyAAK' where Name = 'Verition Fund Management- RMJ (+JSCAPE) - New Logo - 2022' and related_opportunity__C is null; 
update #Deals set related_opportunity__c = '0063t000013DIqjAAG' where Name = 'Swinkels Family Brewers - RMJ SaaS - New logo - 2022' and related_opportunity__C is null; 


update a
set a.Related_Opportunity_Name = b.Name,
	a.Related_Opportunity_Stage = b.StageName,
	a.Related_Opportunity_type = b.Type,
	a.Related_Opportunity_CloseDate = cast(b.closedate as date),
	a.Related_Opportunity_Amount = b.PS_Qualified_Bookings__C
from #Deals a,
	 salesforce.cdata.salesforce.opportunity b
where a.Related_Opportunity__c = b.id
;


update #Deals 
set Related_Opportunity__c = null,
	Related_Opportunity_Amount = Null,
	Related_Opportunity_Name = Null,
	Related_Opportunity_Stage = Null,
	Related_Opportunity_type = null
where Related_Opportunity_type != 'Professional Services'
;

update a
set Related_Opportunity__c = b.id
from #Deals a,
	 salesforce.cdata.salesforce.opportunity b
where a.AccountId = b.AccountId
and a.Related_Opportunity__c is null
and b.type in ('Professional Services')
and b.closedate > a.closedate
and b.closedate < DATEADD(month, 12, a.closedate)
;


update a
set a.Related_Opportunity_Name = b.Name,
	a.Related_Opportunity_Stage = b.StageName,
	a.Related_Opportunity_type = b.Type,
	a.Related_Opportunity_Amount = b.PS_Qualified_Bookings__C
from #Deals a,
	 salesforce.cdata.salesforce.opportunity b
where a.Related_Opportunity__c = b.id
;



update a
set a.pse_Name = b.Name,
	a.pse__Project_Status__c = b.pse__Project_Status__c,
	a.pse__CreatedDate = cast(b.CreatedDate as date),
	a.pse__Project_Type__c = b.pse__Project_Type__c,
	a.pse__Stage__c = b.pse__Stage__c,
	a.pse__Start_Date__C = cast(b.pse__Start_Date__c as date),
	a.pse_Project_Owner_Id__c = b.OwnerId,
	a.Engagement_Manager__c = b.pse__Project_Manager__c
from #Deals a,
	salesforce.cdata.[Salesforce].[pse__Proj__c] b
where a.Related_Opportunity__c = b.pse__Opportunity__c
;



update a
set a.task_id = b.id,
	a.Task_Status = b.Status,
	a.Task_Subject = b.Subject,
	a.ActivityDate = cast(b.ActivityDate as date),
	a.Task_OwnerId = b.OwnerId,
	a.Task_Product__C = b.Product__C
from #Deals a,
	salesforce.cdata.salesforce.task b 
where a.id = b.Related_Opportunity__c
and b.type = 'Onboarding'
and b.Assigned_to_Role__c like '%success%'
;

update a
set a.pse_Project_Owner__c = b.Name
from #Deals a,
	 salesforce.cdata.salesforce.[User] b
where pse_Project_Owner_Id__c = b.id
;

update a
set a.Task_Owner__c = b.Name
from #Deals a,
	 salesforce.cdata.salesforce.[User] b
where Task_OwnerId = b.id
;

--select * from #Deals;


--drop table #Onboarding;
select 
	a.AccountId,
	a.Owner as Opp_Owner,
	a.Id as OppId,
	a.Name as Opp_Name,
	a.CloseDate as Opp_CloseDate,
	a.Software_Qualified_Bookings__c,
	case when a.Opp_Type = 'Existing Business' then 'SAP Flip' 
		 when a.Opp_Type = 'New Business' and PS_Qualified_Bookings__c > 0 then 'New Business w/PS Bundle'
	else 'New Business' end as Opp_Type,
	a.Product__C as Opp_Product,
	a.Owner_s_Manager__c as Opp_Owner_Manager,
	a.related_Opportunity__C as Related_Opp,
	a.Related_Opportunity_Name,
	a.Related_Opportunity_Stage,
	a.Related_Opportunity_type,
	a.Related_Opportunity_Amount,
	a.Related_Opportunity_CloseDate,
	a.In_Production__c,
	a.PS_Requirements__c,
	--a.PS_Qualified_Bookings__c,
	b.id as TaskID,
	d.Name as Onboarding_Owner_Name,
	b.Subject as Onboarding_Subject,
	--b.OwnerId as Onboarding_OwnerID,
	b.Product__C as Onboarding_Product,
	b.Status as Onboarding_Status,
	c.Name as Project_Name,
	c.pse__Project_Status__c as Project_Status,
	e.name as Project_Record_Owner,
	c.pse__Project_Manager__c as Engagement_Manager,
	cast(null as varchar(150)) as Engagement_Manager_Name,
	c.pse__Stage__c
into #Onboarding
from #Deals a
left outer join salesforce.cdata.[Salesforce].[Task] b
on a.id = b.Related_Opportunity__c and b.type = 'Onboarding' and b.Assigned_to_Role__c like '%success%'
left outer join salesforce.cdata.[Salesforce].[pse__Proj__c] c
on a.Related_Opportunity__c = c.pse__Opportunity__c and c.pse__Opportunity__c is not null
--where b.type = 'Onboarding' or b.type is null
left outer join salesforce.cdata.[Salesforce].[User] d
on b.Ownerid = d.id
left outer join salesforce.cdata.[Salesforce].[User] e
on c.Ownerid = e.id
;

update a
set a.Engagement_Manager_name = b.Name
from #Onboarding a,
	salesforce.cdata.[Salesforce].[Contact] b
where a.Engagement_Manager = b.id
;


update a
set a.In_Production__c = b.In_Production__c
from #Onboarding a,
	salesforce.cdata.[Salesforce].[Account] b
where a.AccountID = b.id
;


select * from #Onboarding;


--select top 100 * from salesforce.cdata.[Salesforce].[pse__Proj__c];
--select * from #Deals where id = '0063t000017k1x2AAA';
--select * from salesforce.cdata.[Salesforce].[Contact];
--select top 100 * from salesforce.cdata.[Salesforce].[Task] where Assigned_to_Role__c like '%success%';
--select top 100 * from salesforce.cdata.[Salesforce].[Account] where In_Production__c is not null;
--select * from #Deals where PS_Qualified_Bookings__c is not null;



"

# execute the query and store the results in a data frame
results <- dbGetQuery(ssms.con, query)

# set authorization to write to google sheets
# gs4_auth()

# Make a variable for the sheet to write to
sheet.link <- "https://docs.google.com/spreadsheets/d/1VB-pBbWah4IEbOjRaK-Av-K_we_Wc-HbJFqVAQmjEP0/edit#gid=0"

# write to the sheet
write_sheet(
  data = results,
  ss = sheet.link,
  sheet = 'My New Data'
)

# disconnect from Database
dbDisconnect(ssms.con)
