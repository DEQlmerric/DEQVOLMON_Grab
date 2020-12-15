
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

data <- read_excel("//deqlab1/Vol_Data/RogueRK/2020/Rgrab/OriginalCopy_VolWQGrabData_SubmittalFormat3.0_RRK_10.14.2020_4R.xlsx" , skip =5, 
                            sheet = "data") %>% filter(!is.na(LASAR_ID)) %>% 
                  mutate(row_ID = 1:n()) %>% #adds a row number 
                  mutate(StartTime = strftime(StartTime, "%H:%M:%S", tz = "UTC"), #had to add UTC to get the time correct - will this harm anything? 
                  DT = paste(StartDate,StartTime),
                  DateTime = ymd_hms(DT))

project <- read_excel("//deqlab1/Vol_Data/RogueRK/2020/Rgrab/OriginalCopy_VolWQGrabData_SubmittalFormat3.0_RRK_10.14.2020_4R.xlsx", sheet = "ProjectInfo", skip =5) %>%
  rename(LOQ = 'Limit of Quantitation', Low_QC = 'Low Level QC limit') %>% filter(!is.na(CharID))
sub_id <- 253 

#Function to format the data - must confirm that the QC checks are zero - 
format_data(data)

# function to add auto grade and anomalies 
AutoGrade_Anom(res)
# Generate data summaries and plots 
Plots_QCSum(final_DQL)
