## L. Merrick 
## 4/24/2020
## Rewrite of scripts originally written by Steve Hanson

library(tidyverse)
library(odbc)
library(readxl)
library(data.table)

### Connect to VolMon DB 
VM2.sql <- odbcConnect("VolMon2")
QCcrit <- sqlFetch(VM2.sql,"dbo.tlu_Qccrit")
anom_crit <- sqlFetch(VM2.sql,"dbo.tlu_AnomCrit")
  
odbcClose(VM2.sql)
#### Precision Checks & QC summaries ####

prec_grade <- res %>% # source from the Format_template fxn?
        left_join(QCcrit, by = c('CharIDText'= 'charidText')) %>%
        group_by(LASAR,DateTime,CharIDText) %>% 
        mutate(Dup = ifelse(n() > 1, 1, 0)) %>%
        ungroup() %>% 
        filter(Dup == 1) %>% 
        select(LASAR,DateTime,Char,CharIDText,actgrp_char,sub_char,sample_type,Result,QCcalc,DQLA,DQLB) %>%
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
                                    TRUE ~ "C"))

#Create a table that counts Grade types by actgrp_char
QAQC_grade_count <- prec_grade %>%
  mutate(A = ifelse(prec_DQL== 'A', 1, 0)) %>%
  mutate(B = ifelse(prec_DQL == 'B', 1, 0)) %>%
  mutate(C = ifelse(prec_DQL == 'C', 1, 0)) %>%
  mutate(E = ifelse(prec_DQL == 'E', 1, 0)) %>%
  group_by(actgrp_char) %>%
  summarize(countofQAQC = n(),
            sumofA = sum(A),
            sumofB = sum(B),
            sumofC = sum(C),
            sumofE = sum(E)) %>%
  mutate(pctA = (sumofA / countofQAQC),
         pctB = (sumofB / countofQAQC),
         pctC = (sumofC / countofQAQC),
          pctE = (sumofE / countofQAQC))

#QC grades per sub, used for deault method
QAQC_grades_per_sub <-  prec_grade%>%
  mutate(A = ifelse(prec_DQL == 'A', 1, 0)) %>%
  mutate(B = ifelse(prec_DQL == 'B', 1, 0)) %>%
  mutate(C = ifelse(prec_DQL== 'C', 1, 0)) %>%
  mutate(E = ifelse(prec_DQL == 'E', 1, 0)) %>%
  group_by(sub_char) %>%
  summarize(countofQAQC = n(),
    sumofA = sum(A),
    sumofB = sum(B),
    sumofC = sum(C),
    sumofE = sum(E)) %>%
  mutate(subpctA = (sumofA / countofQAQC),
    subpctB = (sumofB / countofQAQC),
    subpctC = (sumofC / countofQAQC),
    subpctE = (sumofE / countofQAQC)) %>%
  select(sub_char, countofQAQC, subpctA, subpctB, subpctC, subpctE)

# count number of duplicates per submission ID
QAQC_num_dup_by_sub <- res %>%
  mutate(Count_of_sub_Q = if_else(sample_type == "dup", 1, 0)) %>%
  group_by(sub_char) %>%
  summarise(Count_of_sub_QC = sum(Count_of_sub_Q))

#Table for number of QAQC Samples Per activity group
QAQC_num_dup_by_act_grp <- res %>%
  mutate(Count_of_act_Q = if_else(sample_type == "dup", 1, 0),
         Count_of_act_non_Q = if_else(sample_type == "dup", 0, 1)) %>%
  group_by(actgrp_char, sub_char) %>%
  summarise(Count_of_act_QC = sum(Count_of_act_Q),
            Count_of_act_noQC = sum(Count_of_act_non_Q)) %>%
  full_join(QAQC_num_dup_by_sub, by = "sub_char")

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
grade_res_prelim_DQL <- full_join(res, grade_Act_grp_prelim_grade, by = "actgrp_char")


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

QC_Mod <- Grade_mixed_QC %>%
  group_by(actgrp_char) %>%  
  mutate(groupposition = 1:n(), #this won't run
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

QCmodtest <- QC_Mod %>%
  filter(actgrp_char == "0050-20130813-ec") %>%
  select(ResultID, StartDateTime, DEQ_PREC,IsMaxDate, ApplicableAfter, MaXDate, Mod_DEQ_PREC, groupposition )

