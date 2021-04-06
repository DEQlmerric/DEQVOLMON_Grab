### This script takes the volmon grab data template that has been updated to match the example data set. 
library(tidyverse)
library(lubridate)
library(RODBC) 
library(readxl)
library(data.table)
library(fuzzyjoin)
library(ggplot2)

# load functions 
source("Format_Template.R")
source("Auto_Grade.R")
source("Plots.R")
source("Database_tables.R")

# set file path for vol data working copy template and enter submission ID 
data_path <- "//deqlab1/Vol_Data/umpqua/2018-2019/Grab_Data_2018-2019/Ump_ref_18_19/WorkingCopy_UmpRef18-19VolWQGrabDataSub.xlsx"
sub_id <- 254

# bring in the data tab of the working copy 
data <- read_excel(data_path, skip =5, 
                            sheet = "Data") %>% filter(!is.na(LASAR_ID)) %>% 
                  mutate(row_ID = 1:n()) %>% #adds a row number 
                  mutate(StartTime = strftime(StartTime, "%H:%M:%S", tz = "UTC"), #had to add UTC to get the time correct - will this harm anything? 
                  DT = paste(StartDate,StartTime),
                  DateTime = ymd_hms(DT))
# bring in the data tab of the working copy 
project <- read_excel(data_path, sheet = "ProjectInfo", skip =5) %>%
  rename(LOQ = 'Limit of Quantitation', Low_QC = 'Low Level QC limit') %>% filter(!is.na(CharID))

#Function to format the data - must confirm that the QC checks are zero - 
format_data(data)
# function to add auto grade and anomalies 
AutoGrade_Anom(res)
# Generate data summaries and plots 
Plots_QCSum(final_DQL)
# creates database tables 
DB_tables(final_DQL)

# saves the project for LAM to upload to VolMon2
save.image(file = "Step1.RData")
