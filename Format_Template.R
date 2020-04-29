## L. Merrick 
## 4/24/2020
## Rewrite of scripts originally written by Steve Hanson

library(tidyverse)
require(RODBC) #get rid of this
library(odbc)
library(readxl)

### connect to VOLMON2 
library(odbc)
con <- DBI::dbConnect(odbc::odbc(), "VolMon2") ## which one? 


VM2.sql <- odbcConnect("VolMon2") ### 
sub <-    sqlFetch(VM2.sql, "dbo.t_Submission") 
chars <-  sqlFetch(VM2.sql,"dbo.tlu_Characteristic")
odbcClose(VM2.sql)

# bring in dataset 
data <- read_excel("~/DEQVOLMON_Grab/TestingData/Test_SubId0224/Test0224SdcwcGrab_4r.xlsx", sheet = "data") 
  
### restructure the data template 

res <- data %>%
       select(LASAR,DateTime,SampleDepth,SampleDepthUnit,SampleDepth,SampleDepthUnit,EndDateTime,SampleMedium,SampleColMthd,            
              SampleColEquip,SampleColEquipID,SampleColEquipCmnt,DupBatchKey,ends_with("_r")) %>% 
       rename_at(ends_with("_r"))(sub("[.]_r", "")))
       gather()
      
qaul <- select(data,ends_with("_qual"))
dup_res <- select(data,ends_with("d_r"))
dup_qaul <- select(data,ends_with("d_r"))

              