## L. Merrick 
## 4/24/2020
## Rewrite of scripts originally written by Steve Hanson

library(tidyverse)
require(RODBC) #get rid of this
library(odbc)
library(readxl)
library(data.table)

### connect to VOLMON2 ###

VM2.sql <- odbcConnect("VolMon2") ### this requires an ODBC conection to the VOLMON2 on DEQLEAD-LIMS/dev
sub <-    sqlFetch(VM2.sql, "dbo.t_Submission") 
chars <-  sqlFetch(VM2.sql,"dbo.tlu_Characteristic")
type <- sqlFetch(VM2.sql,"dbo.tlu_Type")
odbcClose(VM2.sql)

# bring in dataset - 
data <- read_excel("~/DEQVOLMON_Grab/TestingData/Test_SubId0224/Test0224SdcwcGrab_4r.xlsx", sheet = "data") 
project <- read_excel("~/DEQVOLMON_Grab/TestingData/Test_SubId0224/Test0224SdcwcGrab_4r.xlsx", sheet = "Project_Info", skip =5) 
sub_id <- 224

### restructure the data template ####
### currently does not include the columns ending with _qual, _PREC, _ACC, _DQL
#pull out comments first 
comments <- data %>% 
   select(LASAR,DateTime,ends_with("_cmnt")) %>% 
   gather(key = "Char", value = "Res_comment",-c(LASAR,DateTime)) %>%
   filter(!is.na(Res_comment)) %>%
   mutate(CharIDText = str_remove_all(Char,"_cmnt")) %>% 
   select(-Char)

res <- data %>%
       select(LASAR,DateTime,SampleDepth,SampleDepthUnit,SampleMedium,SampleColMthd,samplers,
              SampleColEquip,SampleColEquipID,SampleColEquipCmnt,DupBatchKey,Org_Comment,DEQ_Comment,ends_with("_r")) %>% 
       gather(key = "Char", value = "Result",-c(LASAR,DateTime,SampleDepth,SampleDepthUnit,SampleMedium,SampleColMthd,samplers,
                                                SampleColEquip,SampleColEquipID,SampleColEquipCmnt,DupBatchKey,Org_Comment,DEQ_Comment)) %>%
       filter(!is.na(Result)) %>%
       mutate(sample_type = ifelse(Char %like% "*d_r","dup","sample")) %>% 
       mutate(CharIDText = case_when(sample_type == "dup"~ str_remove_all(Char,"d_r"),
                                     sample_type == "sample" ~ str_remove_all(Char,"_r"))) %>% 
       left_join(comments, by = c('LASAR','DateTime','CharIDText')) %>% 
       left_join(project, by = c('CharIDText'='CharID')) %>%
       mutate(subid = sub_id,
              act_type = case_when(sample_type == "dup" & FieldOrLab %like% 'Field'~ 313, ##what does this leave out?
                                   sample_type == "sample" & FieldOrLab %like% 'Field'~ 149,
                                   sample_type == "dup" & FieldOrLab %like% 'Lab'~ 313,
                                   sample_type == "sample" & FieldOrLab %like% 'Lab'~ 233)) %>%
       left_join(type, by = c('act_type' = 'TypeID')) %>%# this doesn't call out field primaries 
       left_join(sub, by = c('subid' = 'SubID')) %>%
       mutate(Date4Id = strftime(DateTime, format = '%Y%m%d%I%M', tz = 'UTC'), 
              act_id = paste(subid,Date4Id,LASAR,TypeIDText,sep = "-"), # fix date format
              act_group = paste(subid,Date4Id,samplers,sep = "-"),
              result_id = paste(act_id,CharIDText,sep = "-")) # this doesn't nat change names Char names 


#### QC checks for char and duplicates#### 
### should error be corrected in the 4r file? 

# check for results that are duplicates (same location, sample date/time, QC type, and results)
dup_results <-res %>% 
              group_by(LASAR,DateTime,CharIDText,sample_type,Result) %>% 
              mutate(dup_res = ifelse(n() > 1, 1, 0)) %>%
              ungroup() %>% 
              filter(dup_res == 1) 

# check for duplicate location, sample date/time, QC type, with different results 
dup_dif_result <- res %>% 
                  group_by(LASAR,DateTime,CharIDText,sample_type) %>% 
                  mutate(dup_res = ifelse(n() > 1, 1, 0)) %>%
                  ungroup() %>% 
                  filter(dup_res == 1) 

# verify charIDtext in characteristic table   
name_check <- res %>% 
              left_join(chars, by = 'CharIDText') %>% 
              filter(is.na('CharID'))
              