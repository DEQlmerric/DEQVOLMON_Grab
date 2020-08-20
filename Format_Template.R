## L. Merrick 
## 4/24/2020
## Rewrite of scripts originally written by Steve Hanson

library(tidyverse)
library(lubridate)
library(RODBC) 
library(odbc)
library(readxl)
library(data.table)
#library(DBI)

### connect to VOLMON2 ###
### attempted to use DBI, but got an error when trying to bring in submission and type tables  
### google search looks like it is related to the use of nvarchar(max) in these tables 
#VM2.sql <- DBI::dbConnect(odbc::odbc(), "VolMon2") 
#dbListTables(VM2.sql)
#VM2.sql <- odbcConnect("VolMon2") ### this requires an ODBC conection to the VOLMON2 on DEQLEAD-LIMS/dev
VM2T.sql <- odbcConnect("VolMon_testload")
sub <- sqlFetch(VM2T.sql,"dbo.t_Submission") 
chars <- sqlFetch(VM2T.sql,"dbo.tlu_Characteristic")
type <- sqlFetch(VM2T.sql,"dbo.tlu_Type")
odbcClose(VM2T.sql)

# bring in dataset - 
data <-  read_excel("//deqlab1/Vol_Data/Powder/2018/OriginalCopy_2018_VolWQGrabDataSub_PBWC_4R.xlsx", sheet = "data", skip =5) %>% filter(!is.na(LASAR_ID))
# add a row ID and formats datatime column
data <- data %>% mutate(row_ID = 1:n()) %>%
        mutate(StartTime = strftime(StartTime, "%H:%M:%S"),
               DT = paste(StartDate,StartTime),
               DateTime = ymd_hms(DT))

          
#project <- read_excel("E:/DEQVOLMON_Grab/TestingData/Test_SubID0026/pbwc20154r.xlsx", sheet = "ProjectInfo", skip =5) 
project <- read_excel("//deqlab1/Vol_Data/Powder/2018/OriginalCopy_2018_VolWQGrabDataSub_PBWC_4R.xlsx", sheet = "ProjectInfo", skip =5) 
sub_id <- 253

### restructure the data template ####
#pull out comments first 
act_comments <- data %>% 
   select(row_ID,DEQ_Comment,Org_Comment) %>% 
   filter(!is.na(Org_Comment)|!is.na(DEQ_Comment))

res_comments <- data %>% 
   select(row_ID,ends_with("_cmnt")|ends_with("_COMMENTS")) %>% 
   gather(key = "Char_Comment", value = "Res_comment",-c(row_ID)) %>%
   filter(!is.na(Res_comment)) %>%
   mutate(CharIDText = str_remove_all(Char_Comment,"_cmnt"))

# does this column always exist? Does it need to be added
res_qual <- data %>% 
   select(row_ID,ends_with("_qual")) %>% # LASAR,DateTime,
   gather(key = "char_Qual", value = "RsltQual",-c(row_ID)) %>%
   filter(!is.na(RsltQual)) %>%
   mutate(CharIDText = str_remove_all(char_Qual,"_qual"))

res_ACC <- data %>% 
   select(row_ID,ends_with("_ACC")) %>% 
   gather(key = "char_ACC", value = "OG_ACC",-c(row_ID)) %>%
   filter(!is.na(OG_ACC)) %>%
   mutate(CharIDText = str_remove_all(char_ACC,"_ACC"))

res_PREC <- data %>% 
   select(row_ID,ends_with("_PREC")) %>% 
   gather(key = "char_PREC", value = "OG_PREC",-c(row_ID)) %>%
   filter(!is.na(OG_PREC)) %>%
   mutate(CharIDText = str_remove_all(char_PREC,"_PREC"))

res_DQL <- data %>% 
   select(row_ID,ends_with("_DQL")) %>% 
   gather(key = "char_DQL", value = "OG_DQL",-c(row_ID)) %>%
   filter(!is.na(OG_DQL)) %>%
   mutate(CharIDText = str_remove_all(char_DQL,"_DQL"))

res <- data %>%
       select(row_ID,LASAR_ID,samplers,DateTime,SampleDepth,SampleDepthUnit,SampleMedium,
              SampleColMthd,SampleColEquip,SampleColEquipID,SampleColEquipCmnt,ends_with("_r")|ends_with("_RESULTS")) %>% 
       gather(key = "Char", value = "Result",-c(row_ID,LASAR_ID,DateTime,samplers,SampleDepth,SampleDepthUnit,
                                                SampleMedium,SampleColMthd,SampleColEquip,SampleColEquipID,SampleColEquipCmnt)) %>%
       filter(!is.na(Result)) %>%
       mutate(sample_type = ifelse(Char %like% "*d_r","dup","sample")) %>% 
       mutate(CharIDText = case_when(sample_type == "dup"~ str_remove_all(Char,"d_r"),
                                     sample_type == "sample" ~ str_remove_all(Char,"_r"))) %>% 
       left_join(res_comments, by = c('row_ID','CharIDText')) %>% 
       left_join(res_qual, by = c('row_ID','CharIDText')) %>% # Add depth 
       left_join(res_ACC, by = c('row_ID','CharIDText')) %>%
       left_join(res_PREC, by = c('row_ID','CharIDText')) %>%
       left_join(res_DQL, by = c('row_ID','CharIDText')) %>%
       left_join(act_comments, by = 'row_ID') %>%
       left_join(project, by = c('CharIDText'='CharID')) %>% # I had to update template
       mutate(subid = sub_id,
              act_type = case_when(sample_type == "dup" & FieldOrLab %like% 'Field'~ 313, ##what does this leave out? - I had to update this column name too 
                                   sample_type == "sample" & FieldOrLab %like% 'Field'~ 149,
                                   sample_type == "dup" & FieldOrLab %like% 'Lab'~ 313,
                                   sample_type == "sample" & FieldOrLab %like% 'Lab'~ 233)) %>%
       left_join(type, by = c('act_type' = 'TypeID')) %>%# this doesn't call out field primaries 
       left_join(sub, by = c('subid' = 'SubID')) %>%
       mutate(Date4Id = strftime(DateTime, format = '%Y%m%d%H%M', tz = 'UTC'), 
              Date4group = strftime(DateTime, format = '%Y%m%d', tz = 'UTC'), 
              act_id = paste(subid,Date4Id,LASAR_ID,TypeIDText,sep = "-"), # fix date format
              act_group = paste(subid,Date4group,sep = "-"), # add samplers for other batch type?? - how will this be entered in? # add an if statement for batchdupkey
              result_id = paste(act_id,CharIDText,sep = "-"),
              sub_char = paste(sub_id, CharIDText, sep = "-"),
              actgrp_char = paste(act_group, CharIDText, sep = "-")) 


#### QC checks for char and duplicates#### 
### should error be corrected in the 4r file? 

# check for results that are duplicates (same location, sample date/time, QC type, and results)
dup_results <-res %>% 
              group_by(row_ID,LASAR_ID,DateTime,CharIDText,sample_type,Result) %>% 
              mutate(dup_res = ifelse(n() > 1, 1, 0)) %>%
              ungroup() %>% 
              filter(dup_res == 1) 

# check for duplicate location, sample date/time, QC type, with different results 
dup_dif_result <- res %>% 
                  group_by(row_ID,LASAR_ID,DateTime,CharIDText,sample_type) %>% 
                  mutate(dup_res = ifelse(n() > 1, 1, 0)) %>%
                  ungroup() %>% 
                  filter(dup_res == 1) 

# verify charIDtext in characteristic table  - should be zero  
name_check <- res %>% 
              left_join(chars, by = 'CharIDText') %>% 
              filter(is.na('CharID'))
              