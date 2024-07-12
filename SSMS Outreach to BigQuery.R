source('ActiveCo Functions.R')
library(RMySQL)
library(odbc)

print("Connecting to SSMS")
ssms.con <- dbConnect(odbc(),
                      Driver = "SQL Server",
                      Server = "WIN-6PDIPKC0EER",
                      Database = "Analytics_Source",
                      UID = SSMS_USR,
                      PWD = SSMS_PASS)

# Run the stored procedure
print("Getting SSMS")
data <- dbGetQuery(ssms.con,"SELECT * FROM [Analytics_Source].[dbo].[Outreach_Prospects]")

#sanitize names for BQ upload
names(data) <- clean.names(names(data))

# Define your BigQuery project and dataset
project_id <- "activeco"
dataset_id <- "Outreach_Cerberus"
table_id <- "Prospects"

# Create a BigQuery table reference
table_ref <- bq_table(project_id, dataset_id, table_id)

# Split the data frame into chunks
chunk_size <- 10000  # Define your chunk size
n <- nrow(data)
chunks <- split(data, ceiling(seq_len(n) / chunk_size))

# Upload each chunk to BigQuery
for (chunk in chunks) {
  bq_table_upload(table_ref, chunk, write_disposition = "WRITE_APPEND")
}

print("Upload to Bigquery")
upload.to.bigquery(data,'Outreach_Cerberus','Prospects')
all.objects <- "
Prospect
SequenceState
Call
User
Prospect
SequenceState
SequenceStep
Sequence
Role
CallDisposition
Mailing
Mailbox
User
Prospect
SequenceState
SequenceStep
Sequence
Role
"