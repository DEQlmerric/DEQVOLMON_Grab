## L. Merrick 
## 4/24/2020
## Rewrite of scripts originally written by Steve Hanson

library(tidyverse)
library(odbc)
library(readxl)
library(data.table)
library(sqldf)

#This prevents scientific notation from being used and forces all imported fields to be character or numeric
options('scipen' = 50, stringsAsFactors = FALSE)

### Connect to VolMon DB 
VM2.sql <- odbcConnect("VolMon2") ### this requires an ODBC conection to the VOLMON2 on DEQLEAD-LIMS/dev
QCcrit <- sqlFetch(VM2.sql,"dbo.tlu_Qccrit") %>% select(-UnitID,-Units,-Source,-RcharAlias)
chars <- sqlFetch(VM2.sql,"dbo.tlu_Characteristic")
anom_crit <- sqlFetch(VM2.sql,"dbo.tlu_AnomCrit")
type <- sqlFetch(VM2.sql,"dbo.tlu_Type")
odbcClose(VM2.sql)

 #### Precision Checks & QC summaries ####
prec_grade <- res %>% 
        left_join(chars, by = 'CharIDText') %>% 
        left_join(QCcrit, by = c('CharID'= 'charid','CharIDText'='charidText')) %>% # something funning with do charid= 9 in this table 
        group_by(row_ID,CharIDText) %>% 
        mutate(Dup = ifelse(n() > 1, 1, 0)) %>%
        ungroup() %>% 
        filter(Dup == 1) %>% 
        select(row_ID,LASAR,DateTime,Char,CharIDText,actgrp_char,sub_char,sample_type,Result,result_id,QCcalc,DQLA,DQLB) %>%
        group_by(row_ID,CharIDText) %>%
        arrange(desc(sample_type), .by_group = TRUE) %>%
        mutate(prec_val = case_when(QCcalc == "LogDiff" ~
                                    log10(Result) - log10(lag(Result, default = first(Result))),
                                    QCcalc == "AbsDiff" ~ abs(Result - lag(Result, default = first(Result))),
                                    QCcalc == "RPD" ~ 
                                    abs((Result - lag(Result, default = first(Result)))/mean(Result)))) %>%
        mutate(prec_DQL = case_when(prec_val <= DQLA ~ "A",
                                    prec_val > DQLA & prec_val <= DQLB ~ "B",
                                    TRUE ~ "C")) %>%
        filter(sample_type == 'dup') %>%
        select(row_ID,CharIDText,result_id,prec_val,prec_DQL)

# add prec grade to result 
res_prec_grade <- res %>% left_join(prec_grade, by = c('row_ID','result_id','CharIDText')) 


## calculate the %QC and %DQL 
Prelim_DQL_AG_char <- res_prec_grade %>% 
                      select(row_ID,LASAR,DateTime,subid,act_type,sample_type,TypeIDText,TypeShortName,Date4Id,Date4group,act_id,
                      act_group,CharIDText,result_id,sub_char,actgrp_char,Result,prec_val,prec_DQL) %>%
                      mutate(A = ifelse(prec_DQL %in% 'A',1,0),
                             B = ifelse(prec_DQL %in%  'B',1,0),
                             C = ifelse(prec_DQL %in%  'C', 1, 0),
                             QC = ifelse(sample_type == 'dup', 1, 0),
                             nonQC = ifelse(sample_type == 'sample', 1, 0)) %>%
                      group_by(actgrp_char) %>%
                      summarize(grp_count = n(),
                              sumofQC = sum(QC),
                              sumofNonQC = sum(nonQC),
                              sumofA = sum(A),
                              sumofB = sum(B),
                              sumofC = sum(C)) %>%
                      mutate(pctQC = (sumofQC/grp_count),
                           pctQCtononQC = (sumofQC/sumofNonQC),
                           pctA = (sumofA / sumofQC),
                           pctB = (sumofB / sumofQC),
                           pctC = (sumofC / sumofQC)) %>%
                      mutate(prelim_dql = case_when(pctQCtononQC < 0.1 ~ paste0("QC ", as.integer(pctQCtononQC*100),"%"),
                                                    pctA == 1 ~ "A", #100% QC samples = A assign A
                                                    pctB == 1 ~ "B", #100% QC samples = B assign B
                                                    pctC == 1 ~ "C", #100% QC samples = B assign B
                                                    pctA < 1 | pctB < 1 | pctC < 1 ~ "Mixed",))

# Spread the Prelim grades to activity groups that don't have mixed DQLs  
Prelim_DQL_nonMixed <- res %>% 
  left_join(Prelim_DQL_AG_char, by = 'actgrp_char') %>%
  filter(!prelim_dql == 'Mixed')

# Spread the Prelim grades to the results to mixed results 
Prelim_DQL_Mixed <- res %>% 
  left_join(Prelim_DQL_AG_char, by = 'actgrp_char')  %>%
  filter(prelim_dql == 'Mixed') 
 

Grade_mixed_QC <- Prelim_DQL_Mixed  %>%
  left_join(prec_grade, by = c('row_ID','result_id','CharIDText')) %>%
  filter(sample_type == 'dup') %>%
  arrange(actgrp_char) 
 

QC_Mod <- Grade_mixed_QC %>%
  group_by(actgrp_char) %>%
  mutate(groupposition = 1:n(),
         IsMaxDate = ifelse(DateTime == max(DateTime),1,0),
         MaXDate = max(DateTime)) %>%
  mutate(ApplicableAfter =if_else(prec_DQL > lead(prec_DQL, n = 1) & groupposition == 1, DateTime,
                                  if_else(prec_DQL > lag(prec_DQL, n = 1), lag(DateTime),
                                          if_else(prec_DQL > lead(prec_DQL, n = 1), DateTime, 
                                                  if_else(DateTime == MaXDate, DateTime, DateTime + 100))))) %>%
  ungroup() %>%
  as.data.frame()


# Join the data.frames to find DQL value
# I don't know how this works. I pulled it off the internet
# This should perform "grade spreading"
  
grade_mixed_result <-
    sqldf(
      "SELECT Prelim_DQL_Mixed.actgrp_char, Prelim_DQL_Mixed.result_id, 
            Prelim_DQL_Mixed.Result,  Prelim_DQL_Mixed.DateTime ,
            QC_Mod.prec_DQL as prelim_dql_m
    FROM Prelim_DQL_Mixed, QC_Mod
    WHERE Prelim_DQL_Mixed.actgrp_char == QC_Mod.actgrp_char AND
    (
    (Prelim_DQL_Mixed.DateTime <= QC_Mod.DateTime AND QC_Mod.groupposition == 1) 
       OR
           (Prelim_DQL_Mixed.DateTime <= QC_Mod.DateTime AND QC_Mod.ApplicableAfter IS NULL) 
       OR
          (QC_Mod.ApplicableAfter IS NOT NULL AND Prelim_DQL_Mixed.DateTime > QC_Mod.ApplicableAfter) 
       OR
    (Prelim_DQL_Mixed.DateTime > QC_Mod.DateTime AND QC_Mod.IsMaxDate == 1)
    )"
    ) %>% 
  select(result_id,prelim_dql_m) %>% 
  right_join(Prelim_DQL_Mixed, by = 'result_id') %>%
  mutate(prelim_dql = if_else(prelim_dql== 'Mixed',prelim_dql_m,prelim_dql)) %>%
  select(-prelim_dql_m) 

Prelim_DQL_All <- rbind(Prelim_DQL_nonMixed,grade_mixed_result)

#### generate a table of anomolies for each result #### 
sub_char_precen <- Prelim_DQL_All %>%
                group_by(CharIDText) %>% 
                summarise(percen_5th = quantile(Result, probs = .05),
                          percen_10th = quantile(Result, probs = .10), 
                          percen_90th = quantile(Result, probs = .90),
                          percen_95th = quantile(Result, probs = .95))
station_char_percen <- Prelim_DQL_All%>%
                       group_by(LASAR, CharIDText) %>% 
                       summarise(S_percen_5th = quantile(Result, probs = .05), 
                       S_percen_95th = quantile(Result, probs = .95))
                
anom <- Prelim_DQL_All %>% 
        left_join(anom_crit, by = c('CharIDText' = 'charidText')) %>%
        left_join(sub_char_precen, by= 'CharIDText') %>%
        left_join(station_char_percen, by= c('LASAR','CharIDText')) %>%
        select(LASAR,DateTime,CharIDText,subid,TypeIDText,Result,act_id,act_group,result_id,AnomCritID,charid,LOQ,RealRngL,RealRngU,
               amb01L,amb05L,amb95U,amb99U,stdL,stdU,stdLlp,stdUlp,percen_5th,percen_10th,percen_90th,percen_95th,
               S_percen_5th,S_percen_95th) %>%
        mutate(expected_range = ifelse(Result > RealRngU | Result < RealRngL,"OutOfRange",NA),
               LOQ_anom = ifelse(Result < LOQ-(LOQ*0.1),"BelowLOQ", NA),
               ambient_01 = ifelse(Result < amb01L,"Lowest1%AmbientValues",NA),
               ambient_05 = ifelse(Result < amb05L,"Lowest5%AmbientValues", NA),
               ambient_95 = ifelse(Result > amb95U,"Highest95%AmbientValues", NA),
               ambient_99 = ifelse(Result > amb99U,"Highest99%AmbientValues", NA),
               WQS = ifelse(Result > stdU | Result < stdL, "ViolatesWQStandard",NA),
               WQS_LP = ifelse(Result > stdUlp | Result < stdLlp, "ViolatesLessProtectiveWQStandard",NA),
               sub_5 = ifelse(Result < percen_5th,"Lowest5%SubmValues",NA),
               sub_10 = ifelse(Result < percen_10th,"Lowest10%SubmValues",NA),
               sub_90 = ifelse(Result > percen_90th,"Highest90%SubmValues",NA),
               sub_95 = ifelse(Result > percen_95th,"Highest95%SubmValues",NA),
               station_5 = ifelse(Result < S_percen_5th,"Lowest5%SubmStnValues",NA),
               station_95 = ifelse(Result > S_percen_95th,"Highest95%SubmStnValues",NA)) %>%
               #QC_mismatch = ifelse(OG_PREC == prelim_dql, NA, "OrgandDEQPrecDQLDifferent")) 
        gather(key = "Anom_type", value = "Anom",-c(LASAR,DateTime,CharIDText,subid,TypeIDText,
                                                    Result,act_id,act_group,result_id,AnomCritID,charid,LOQ,RealRngL,RealRngU,
                                                    amb01L,amb05L,amb95U,amb99U,stdL,stdU,stdLlp,stdUlp,percen_5th,percen_10th,
                                                    percen_90th,percen_95th,S_percen_5th,S_percen_95th)) %>%
        filter(!is.na(Anom))

#Create table that counts selected anomalies per resultID
# only counts anomalies that would trigger a review
#Create table that combines anomaly data with activity groups
res_anom <- anom %>%
            mutate(AnomTypeCode = case_when(Anom == "Highest99%AmbientValues"|Anom == "Lowest1%AmbientValues" ~ "ambient_99",
                                            Anom == "Lowest5%SubmValues" | Anom == "Highest95%SubmValues" ~ "submit_95",
                                            Anom == "Lowest10%SubmValues" | Anom == "Highest90%SubmValues" ~ "submit_90",
                                            Anom == "Lowest5%SubmStnValues" | Anom == "Highest95%SubmStnValues" ~ "station_95",
                                            Anom == "Lowest5%AmbientValues" | Anom == "Highest95%AmbientValues" ~ "ambient_95",
                                            Anom == "OutOfRange"  | Anom == "OverUpRange" ~ "outofrange",
                                            Anom == "BelowLOQ" ~ "BelowLOQ", 
                                            Anom == "ViolatesLessProtectiveWQStandard" | Anom == "ViolatesWQStandard" ~ "ViolateWQS")) %>%
          select(result_id,AnomTypeCode) %>%
          distinct()

#Create table that counts selected anomalies per resultID
# only counts anomalies that would trigger a review
anom_res_sum = res_anom %>%
  group_by(result_id) %>%
  summarise(num_anom = n(),
            anom_sub_95 = sum(AnomTypeCode == "submit_95"),
            anom_amb_99 = sum(AnomTypeCode == 'ambient_99'),
            anom_amb_95 = sum(AnomTypeCode == 'ambient_95'),
            anom_WQS = sum(AnomTypeCode =='ViolateWQS'),
            anom_BelowLOQ = sum(AnomTypeCode == 'BelowLOQ')) %>%
  replace(is.na(.), 0)

final_DQL <- Prelim_DQL_All %>%
  left_join(anom_res_sum, by = "result_id") %>%
           mutate(final_DQL = case_when(prelim_dql < "C" & anom_amb_99 > 0 ~ "Anom", 
                                        prelim_dql < "C" & anom_sub_95 > 0 & anom_amb_95 > 0 ~ "Anom",
                                        prelim_dql < "C" & anom_amb_95 > 0 & anom_WQS > 0 ~ "Anom",
                                        prelim_dql < "C" & anom_WQS > 0 ~ "Anom",
                                        prelim_dql > "H"  ~ "Anom", # this pulls out QC less than 10% - not sure why %like% didn't work
                                        TRUE ~ as.character( prelim_dql))) %>%
  mutate(DQLCmt = "") 

