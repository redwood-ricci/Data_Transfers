source('ActiveCo Functions.R')
library(dplyr)
library(tidyr)
# proof of linear model concept: https://docs.google.com/spreadsheets/d/1EC9xLDisbgQt7jIzpkR83LDCSio0bn7J_LCUX39VoVU/edit#gid=0
sheet.link <- "https://docs.google.com/spreadsheets/d/1rqIfU80NvNESI8OeSmRpsRcf7P6AvSFOeuD7x8fwcPg/edit#gid=81464345"

seed <- read_sheet(sheet.link,
                    sheet = 'R Import',
                    range = "A2:R400")

# remove NA order date rows
seed <- seed[which(!is.na(seed$OrderDate)),]
seed$OrderDate <- as.Date(seed$OrderDate)

seed$quarter <- quarter(seed$OrderDate,with_year = TRUE)

# turn rolling sum into daily rate by subtracting previous days
seed <- seed %>%
  arrange(OrderDate) %>%
  mutate(f_new = `New Forecast` - lag(`New Forecast`,n = 1),
         f_expansion = `Expansion Forecast` - lag(`Expansion Forecast`,n=1),
         f_flip = `Flips Forecast` - lag(`Flips Forecast`,n = 1)
         )

# remove forecasts from the past
# seed$f_flip <-   seed$`Flips Actual` - seed$`Flips Forecast`

seed$f_new[which(seed$`New Forecast` < seed$`New Actual` | seed$OrderDate < Sys.Date())] <- 0
seed$f_expansion[which(seed$`Expansion Forecast` < seed$`Expansion Actual` | seed$OrderDate < Sys.Date())] <- 0
seed$f_flip[which(seed$`Flips Forecast` < seed$`Flips Actual` | seed$OrderDate < Sys.Date())] <- 0

seed$fa_new <- seed$New
seed$fa_new[which(seed$OrderDate >= Sys.Date())] <-seed$f_new[which(seed$OrderDate >= Sys.Date())]
# cross <- as.Date(max(seed$OrderDate[which(seed$fa_new == 0)])) # smooth transition from forecasted to actuals
# which.cross <- which(seed$OrderDate == cross) # smooth transition from forecasted to actuals
# f_diff <- seed$`New Actual`[which.cross] - seed$`New Forecast`[which.cross]
# seed$fa_new[which(seed$OrderDate == cross+1)] <- seed$fa_new[which(seed$OrderDate == cross+1)] - f_diff

seed$fa_expansion <- seed$Expansion
seed$fa_expansion[which(seed$OrderDate >= Sys.Date())] <-seed$f_expansion[which(seed$OrderDate >= Sys.Date())]
# cross <- as.Date(max(seed$OrderDate[which(seed$fa_expansion == 0)])) # smooth transition from forecasted to actuals
# which.cross <- which(seed$OrderDate == cross) # smooth transition from forecasted to actuals
# f_diff <- seed$`Expansion Actual`[which.cross] - seed$`Expansion Forecast`[which.cross]
# seed$fa_expansion[which(seed$OrderDate == cross+1)] <- seed$fa_expansion[which(seed$OrderDate == cross+1)] - f_diff

seed$fa_flips <- seed$`Flips - Maintenance`
seed$fa_flips[which(seed$OrderDate >= Sys.Date())] <-seed$f_flip[which(seed$OrderDate >= Sys.Date())]
# seed$fa_flips[which(is.na(seed$fa_flips))] <-seed$f_flip[which(is.na(seed$fa_flips))]
# cross <- as.Date(max(seed$OrderDate[which(seed$fa_flips == 0)])) # smooth transition from forecasted to actuals
# which.cross <- which(seed$OrderDate == cross) # smooth transition from forecasted to actuals
# f_diff <- seed$`Flips Actual`[which.cross] - seed$`Flips Forecast`[which.cross]
# seed$fa_flips[which(seed$OrderDate == cross+1)] <- seed$fa_flips[which(seed$OrderDate == cross+1)] - f_diff

# check out a yearly view of New
# z <- seed %>%
#   group_by(quarter) %>%
#   summarise(s = sum(new_forecast_actuals,na.rm = T))

# new.checker <- seed[,c("OrderDate","New","New Actual","New Forecast","f_new","fa_new")]
# View(new.checker)

upload <- seed[,c("OrderDate","fa_new","fa_expansion","fa_flips")]
upload <- upload %>%
  pivot_longer(cols = starts_with("fa_"),
               names_to = 'BookingType',
               values_to = 'Forecast')
upload$BookingType[which(upload$BookingType == "fa_expansion")] <- "Expansion"
upload$BookingType[which(upload$BookingType == "fa_new")] <- "New"
upload$BookingType[which(upload$BookingType == "fa_flips")] <- "Flips - Maintenance"
# table(duplicated(upload$OrderDate))

# check distribution totals by quarter
# upload$quarter <- quarter(upload$OrderDate,with_year = TRUE)
# upload %>%
#   group_by(quarter,BookingType) %>%
#   summarise(Forecast = sum(Forecast,na.rm = T)) %>%
#   pivot_wider(names_from = quarter,values_from = Forecast)



upload.to.bigquery(upload,dataset = 'R_Data','CerberusForecast')


# cerberus raw data query
# "select Type, BookingType, FM , FQ, FY, Sum(Amount) as Amount
# from `activeco.Analytics.CerberusBookings_Deferrals`
# group by Type, BookingType, FM , FQ, FY"

# seed <- query.bq(
#   "select * from `activeco.Analytics.CerberusBookings_Deferrals`
#   where Type != 'Deferrals'
#   and OrderDate >= '2021-01-01'
#   and OrderDate <= '2024-01-01'"
# )
# 
# z <- seed %>%
#   group_by(FY,BookingType) %>%
#   arrange(OrderDate) %>%
#   mutate(cum_sum = cumsum(Amount)) 
# 
# types <- c("New","Expansion")
# years <- c("FY23")
# 
# sheet.link <- "https://docs.google.com/spreadsheets/d/1o4FwEqXxUJ20p9_8z2Fa9Je95CdGGHTP7hfexv7APgY/edit#gid=0"
# t <- types[1]
# y <- years[3]
# for (t in types) {
#   for (y in years) {
#     # make a name for the upload sheet
#     s <- paste0(t,"_",y)
#     # subset for one type and year
#     w <- z[which(z$BookingType == t & z$FY == y &z$Type != 'Deferrals'),]
#     # train model
#     l <- lm(cum_sum ~ OrderDate, data = w)
#     # make sure every date is represented in the data
#     d.start <- floor_date(mean(w$OrderDate,na.rm = T),unit = 'year')
#     d.end   <- ceiling_date(mean(w$OrderDate,na.rm = T),unit = 'year') -1
#     d.all <- data.frame(OrderDate = seq.Date(d.start,d.end,'day'))
#     w <- merge(w,d.all,by = 'OrderDate',all = TRUE)
#     w <- w %>% relocate(OrderDate, .after = 'FW')
#     # predict model
#     w$predicted <- predict(l,new = w)
#     # calculate difference
#     w$diff <- w$cum_sum - w$predicted
#     
#     # upload sheet
#     write_sheet(
#       w,
#       sheet.link,
#       s
#     )
#   }
# }

# plot(w$cum_sum ~ w$OrderDate)
# abline(l,col = 'red')
# text(x = 10, y = 50, # Coordinates
#      label = "Text annotation")


# select
# OrderDate,
# case when Type = 'Deferrals' then 'Deferrals' else BookingType end as Type,
# 'Booking Total' as metric,
# sum(Amount) as Amount
# from Analytics.CerberusBookings_Deferrals
# group by 1,2,3

