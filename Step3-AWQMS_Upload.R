## L. Merrick 
## This script will export a .csv to upload to AWQMS
### THis should only be run after the submission is manually reviewed in the VolMon database 

library(tidyverse)
library(lubridate)
library(RODBC)

VM2.sql <- odbcConnect("VolMon2")
load("Step1.RData")
### this is dependent on the submission 


VM2.sql <- odbcConnect("VolMon2") ### this requires an ODBC connection to the VOLMON2 on DEQLEAD-LIMS/dev
chars <- sqlFetch(VM2.sql,"dbo.tlu_Characteristic")
context <- sqlFetch(VM2.sql, "dbo.tlu_Context")
method <- sqlFetch(VM2.sql, "dbo.tlu_Method")
org <- sqlFetch(VM2.sql, "tlu_Organization")
stations <- sqlFetch(VM2.sql, "dbo.tlu_Stations") 

#builds an SQL query the activity table for this submission    
a_ids <- act$ActivityIDText
a_idsString <- toString(sprintf("'%s'", a_ids))
a_sql_fmt <- "SELECT * FROM t_Activity WHERE ActivityIDText IN (%s)"
qryActt <- sprintf(a_sql_fmt, a_idsString)

### pulls in activity info ###
db_act <- sqlQuery(VM2.sql, qryActt)  %>% 
          select(ActivityID,ActivityTypeId,StnID,StartDateTime,StartDateTimeZoneID,EndDateTime,
          EndDateTimeZoneID,SmplColEquipComment,SmplDepth,SmplDepthUnitID,Org_Comment) %>%
          tidyr::separate(StartDateTime, c("StartDate", "StartTime"), sep = " ") %>%
          tidyr::separate(EndDateTime, c("EndDate", "EndTime"), sep = " ") %>%
          left_join(stations, by ='StnID') %>%
          select(ActivityID,ActivityTypeId,MLocID,StartDate,StartTime,StartDateTimeZoneID,
                 EndDate,EndTime,EndDateTimeZoneID,SmplColEquipComment,SmplDepth,
                 SmplDepthUnitID,Org_Comment)

#builds an SQL query to pull from the results table for this submission  
r_ids <- res_t$ResultIDText
r_idsString <- toString(sprintf("'%s'", r_ids))
r_sql_fmt <- "SELECT * FROM t_Result WHERE ResultIDText IN (%s)"
# build an SQL query
qryRest <- sprintf(r_sql_fmt, r_idsString)

### pulls in result info ###
db_res <- sqlQuery(VM2.sql, qryRest) %>% 
          select(ActivityID,CharID,Result,UnitID,MethodID,MethodSpeciation,RsltTypeID,
                 AnalyticalLaboratoryID,AnalyticalStartTime,AnalyticalStartTimeZoneID,
                 AnalyticalEndTime,AnalyticalEndTimeZoneID,LabCommentCode,LOQ,BiasValue,
                 PrecisionValue,StatisticalBasisID,RsltTimeBasisID,ORDEQ_DQL,Org_RsltComment) %>%
          tidyr::separate(AnalyticalStartTime, c("AnStartDate", "AnStartTime"), sep = " ") %>%
          tidyr::separate(AnalyticalEndTime, c("AnEndDate", "AnEndTime"), sep = " ") %>%
          left_join(chars,by = 'CharID') %>%
          left_join(units, by = 'UnitID') %>%
          left_join(method, by = 'MethodID') %>%
          left_join(org, by = c('AnalyticalLaboratoryID'='OrgID')) %>%
          left_join(db_act, by = 'ActivityID') %>% 
          select(ActivityTypeId,MLocID,StartDate,StartTime,StartDateTimeZoneID,
                 EndDate,EndTime,EndDateTimeZoneID,SmplColEquipComment,SmplDepth,
                 SmplDepthUnitID,Org_Comment,CharAbbr,Result,UnitIdText,ShortName,
                 MethodSpeciation,RsltTypeID,OrgName,AnStartDate,AnStartTime,AnalyticalStartTimeZoneID,
                 AnEndDate,AnEndTime,AnalyticalEndTimeZoneID,LabCommentCode,LOQ,BiasValue,
                 PrecisionValue,StatisticalBasisID,RsltTimeBasisID,ORDEQ_DQL,Org_RsltComment)

# write file for upload
write.csv(db_res,"//deqlab1/Vol_Data/umpqua/2018-2019/Grab_Data_2018-2019/Ump_ref_18_19/AWQMS_UmpRef18-19VolWQGrabDataSub.csv",na = "",row.names = FALSE)

