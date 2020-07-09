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
QCcrit <- sqlFetch(VM2.sql,"dbo.tlu_Qccrit") 
chars <- sqlFetch(VM2.sql,"dbo.tlu_Characteristic")
anom_crit <- sqlFetch(VM2.sql,"dbo.tlu_AnomCrit")
type <- sqlFetch(VM2.sql,"dbo.tlu_Type")
odbcClose(VM2.sql)


 #### Precision Checks & QC summaries ####

prec_grade <- res %>% 
        left_join(chars, by = 'CharIDText') %>% 
        left_join(QCcrit, by = c('CharID'= 'charid')) %>%
        distinct() %>%
        group_by(LASAR,DateTime,CharIDText) %>% 
        mutate(Dup = ifelse(n() > 1, 1, 0)) %>%
        ungroup() %>% 
        filter(Dup == 1) %>% 
        select(LASAR,DateTime,Char,CharIDText,actgrp_char,sub_char,sample_type,Result,result_id,QCcalc,DQLA,DQLB) %>%
        filter(sample_type == 'dup') %>%
        group_by(LASAR,DateTime,CharIDText) %>%
        arrange(sample_type, .by_group = TRUE) %>%
        mutate(prec_val = case_when(QCcalc == "LogDiff" ~
                                    log10(Result) - log10(lag(Result, default = first(Result))),
                                    QCcalc == "AbsDiff" ~ abs(Result - lag(Result, default = first(Result))),
                                    QCcalc == "RPD" ~ 
                                    abs((Result - lag(Result, default = first(Result)))/mean(Result)))) %>%
        #filter(prec_val == max(prec_val)) %>%
        mutate(prec_DQL = case_when(prec_val <= DQLA ~ "A",
                                    prec_val > DQLA & prec_val <= DQLB ~ "B",
                                    TRUE ~ "C")) %>%
        distinct()

## Add precision grade to results 
res_prec_grade <- prec_grade %>% ungroup() %>% select(result_id,prec_val,prec_DQL) %>% 
                  right_join(res, by = 'result_id') 
## calucal
pct_QC_actgrp_char <- res_prec_grade %>% 
                      select(LASAR,DateTime,subid,act_type,sample_type,TypeIDText,TypeShortName,Date4Id,Date4group,act_id,
                      act_group,CharIDText,result_id,sub_char,actgrp_char,Result,prec_val,prec_DQL) %>%
                      mutate(A = ifelse(prec_DQL== 'A', 1, 0),
                           B = ifelse(prec_DQL == 'B', 1, 0),
                           C = ifelse(prec_DQL == 'C', 1, 0),
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
                           pctA = (sumofA / grp_count),
                           pctB = (sumofB / grp_count),
                           pctC = (sumofC / grp_count))
            
prelim_grade_actgrp_char <- pct_QC_actgrp_char %>%
  mutate(prelim_dql = case_when(pctQCtononQC < 0.1 ~ paste0("QC ", as.integer(pctQCtononQC*100),"%"),
                                pctA == 1 ~ "A", #100% QC samples = A assign A
                                pctB == 1 ~ "B", #100% QC samples = B assign B
                                pctC == 1 ~ "C")) #100% QC samples = C assign C
                                
                      
                             
                             ifelse(Count_of_sub_QC > 5 & subpctA + subpctB == 1 & subpctA >= 0.90, "B", "E"), 
                      ifelse(pctA < 1 | pctB < 1 | pctC < 1 | pctE < 1, "Mixed", #if grades are mixed, assign mixed
                                                                "not yet graded"))))))) %>% #Should never be "not yet graded" 
  select(actgrp_char, sub_char, prelim_dql)  #pare down table to make more manageable 

mixed <- grade_Act_grp_prelim_grade %>%
  filter(prelim_dql == "Mixed")

#Push act_grp prelim DQLs to results
grade_res_prelim_DQL <- res %>%
  select(LASAR,DateTime,subid,act_type,TypeIDText,TypeShortName,Date4Id,Date4group,act_id,
         act_group,CharIDText,result_id,sub_char,actgrp_char,Result) %>%
  full_join(grade_Act_grp_prelim_grade, by = "actgrp_char")


#Combine number of QAQC samples with precision grades
Act_grp_char_QAQC <- QAQC_num_dup_by_act_grp %>%
  full_join(QAQC_grade_count, by = "actgrp_char") %>%
  full_join(QAQC_grades_per_sub, by = "sub_char") %>%
  replace(is.na(.), 0)


#### generate a table of anomolies for each result #### 
sub_char_precen <- res %>%
                group_by(CharIDText) %>% 
                summarise(percen_5th = quantile(Result, probs = .05),
                          percen_10th = quantile(Result, probs = .10), 
                          percen_90th = quantile(Result, probs = .90),
                          percen_95th = quantile(Result, probs = .95))
station_char_percen <- res %>%
                       group_by(LASAR, CharIDText) %>% 
                       summarise(S_percen_5th = quantile(Result, probs = .05), 
                       S_percen_95th = quantile(Result, probs = .95))
                
anom <- res %>% 
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
               sub_90 = ifelse(Result < percen_90th,"Lowest90%SubmValues",NA),
               sub_95 = ifelse(Result < percen_95th,"Lowest95%SubmValues",NA),
               station_5 = ifelse(Result < S_percen_5th,"Lowest5%SubmStnValues",NA),
               station_95 = ifelse(Result < S_percen_95th,"Lowest5%SubmStnValues",NA)) %>%
        gather(key = "Anom_type", value = "Anom",-c(LASAR,DateTime,CharIDText,subid,TypeIDText,
                                                    Result,act_id,act_group,result_id,AnomCritID,charid,LOQ,RealRngL,RealRngU,
                                                    amb01L,amb05L,amb95U,amb99U,stdL,stdU,stdLlp,stdUlp,percen_5th,percen_10th,
                                                    percen_90th,percen_95th,S_percen_5th,S_percen_95th)) %>%
        filter(!is.na(Anom))

#### Grading####
#assign preliminary DQLs to an Activity group characteristic

grade_Act_grp_prelim_grade <- Act_grp_char_QAQC %>%
  mutate(prelim_dql = ifelse(Count_of_act_QC / Count_of_act_noQC < 0.1, paste0("QC ", as.integer((Count_of_act_QC / Count_of_act_noQC)*100),"%"),
                      ifelse(pctA == 1, "A", #100% QC samples = A assign A
                      ifelse(pctB == 1, "B", #100% QC samples = B assign B
                      ifelse(pctC == 1, "C", #100% QC samples = C assign C
                      ifelse(pctA == 0 & pctB == 0 & pctC == 0, 
                      #default QC method if no QC samples
                      #if submission has >5 QC samples, 100% of those are A or B, and 
                      #=> 90% are A, then use B. Else, E
                      ifelse(Count_of_sub_QC > 5 & subpctA + subpctB == 1 & subpctA >= 0.90, "B", "E"), 
                      ifelse(pctA < 1 | pctB < 1 | pctC < 1 | pctE < 1, "Mixed", #if grades are mixed, assign mixed
                      "not yet graded"))))))) %>% #Should never be "not yet graded" 
  select(actgrp_char, sub_char, prelim_dql)  #pare down table to make more manageable 

mixed <- grade_Act_grp_prelim_grade %>%
  filter(prelim_dql == "Mixed")

#Push act_grp prelim DQLs to results
grade_res_prelim_DQL <- res %>%
                        select(LASAR,DateTime,subid,act_type,TypeIDText,TypeShortName,Date4Id,Date4group,act_id,
                        act_group,CharIDText,result_id,sub_char,actgrp_char,Result) %>%
                        full_join(grade_Act_grp_prelim_grade, by = "actgrp_char")


#pull out results with mixed grades
grade_mixed_res_prelim_dql <- grade_res_prelim_DQL %>%
  filter(prelim_dql == "Mixed") 

#pull out QC samples with mixed grades
Grade_mixed_QC <- grade_res_prelim_DQL %>%
  filter(prelim_dql == "Mixed") %>%
  filter(sample_type == "dup") %>%
  arrange(actgrp_char)
# NOT sure I need these 
#sort out some date issues 
#Grade_mixed_QC$StartDateTime = as.POSIXct(Grade_mixed_QC$StartDateTime, format = "%m/%d/%Y")
#grade_mixed_res_prelim_dql$StartDateTime = as.POSIXct(grade_mixed_res_prelim_dql$StartDateTime, format = "%m/%d/%Y")

QC_Mod <- prec_grade %>%
  group_by(actgrp_char) %>%  
  mutate(groupposition = 1:n(),#this won't run if there are no mixed results
         IsMaxDate = ifelse(DateTime == max(DateTime),1,0),
         MaXDate = max(DateTime)) %>%
  mutate(ApplicableAfter =if_else(prec_DQL > lead(prec_DQL, n = 1) & groupposition == 1, DateTime,
                          if_else(prec_DQL > lag(prec_DQL, n = 1), lag(DateTime),
                          if_else(prec_DQL > lead(prec_DQL, n = 1), DateTime, 
                          if_else(DateTime == MaXDate, DateTime, DateTime + 100))))) %>%
  ungroup() %>%
  group_by(actgrp_char, DateTime) %>%
  #if more than 1 QC sample exists for a day, choose the lowest value 
  #(max is used because A < B )
  mutate(Mod_DEQ_PREC = max(prec_DQL)) %>%
  as.data.frame()

# QCmodtest <- QC_Mod %>%
#   filter(actgrp_char == "0050-20130813-ec") %>%
#   select(ResultID, StartDateTime, DEQ_PREC,IsMaxDate, ApplicableAfter, MaXDate, Mod_DEQ_PREC, groupposition )

# Join the data.frames to find DQL value
# I don't know how this works. I pulled it off the internet
# This should perform "grade spreading"

grade_mixed_result <-
  sqldf(
    "SELECT grade_mixed_res_prelim_dql.actgrp_char, grade_mixed_res_prelim_dql.result_id, 
            grade_mixed_res_prelim_dql.Result,  grade_mixed_res_prelim_dql.DateTime ,
            QC_Mod.prec_DQL as prelim_dql
    FROM grade_mixed_res_prelim_dql, QC_Mod
    WHERE grade_mixed_res_prelim_dql.actgrp_char == QC_Mod.actgrp_char AND
    ( 
    (grade_mixed_res_prelim_dql.DateTime <= QC_Mod.DateTime AND QC_Mod.groupposition == 1) 
       OR
           (grade_mixed_res_prelim_dql.DateTime <= QC_Mod.DateTime AND QC_Mod.ApplicableAfter IS NULL) 
       OR
          (QC_Mod.ApplicableAfter IS NOT NULL AND grade_mixed_res_prelim_dql.DateTime > QC_Mod.ApplicableAfter) 
       OR
    (grade_mixed_res_prelim_dql.DateTime > QC_Mod.DateTime AND QC_Mod.IsMaxDate == 1)
    )"
  )

 
# below is the prelim grade table
#merge mixed reults back into table
grade_prelim_DQL <- merge(grade_res_prelim_DQL, grade_mixed_result, by = "result_id", all.x = T ) %>%
  #if grade is mixed, assign new spread out grade, else use original grade
  mutate(Auto_DQL = ifelse(!is.na(prelim_dql.y), prelim_dql.y, prelim_dql.x)) %>% 
  #If result is a QC sample, use the grade from QC sample. Some days have more than 1 QC sample 
  #and grade_mixed_result assigns lowest grade for the day. This ensures QC results retain
  #original grade 
  mutate(Auto_DQL = ifelse(prelim_dql.x == "" | is.na(prelim_dql.x), Auto_DQ, prelim_dql.x)) %>%
  #below here is just cleanup from the merge process
  mutate(Result = Result.x) %>%
  mutate(DateTime = DateTime.x) %>%
  mutate(actgrp_char = actgrp_char.x) %>%
  mutate(sub_char = sub_char.x) %>%
  select(
    -Result.x,
    -Result.y,
    -actgrp_char.x,
    -actgrp_char.y,
    -prelim_dql.x,
    -prelim_dql.y,
    -DateTime.x,
    -DateTime.y,
    -sub_char.x,
    -sub_char.y
  ) %>%
  mutate(resactgrp = paste(result_id, actgrp_char, sep = "-")) #%>%
  #distinct()


