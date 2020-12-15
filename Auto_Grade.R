## L. Merrick 
## 4/24/2020
## Rewrite of scripts originally written by Steve Hanson

#library(tidyverse)
#library(odbc) # is this the right package?  it did not support the odbcConnect function.
#library(readxl)
#library(fuzzyjoin)
#library(RODBC) # added by sjh this is the package needed to run odbcConnect

#This prevents scientific notation from being used and forces all imported fields to be character or numeric
AutoGrade_Anom <- function(res) {
  
options('scipen' = 50, stringsAsFactors = FALSE)

### Connect to VolMon DB 
VM2.sql <- odbcConnect("VolMon2") ### this requires an ODBC conection to the VOLMON2 on DEQLEAD-LIMS/dev
#VM2T.sql <- odbcConnect("VolMon_testload") ### remove test database
QCcrit <- sqlFetch(VM2.sql,"dbo.tlu_Qccrit") %>%  
           select(-UnitID,-Units,-Source,-RcharAlias)
chars <- sqlFetch(VM2.sql,"dbo.tlu_Characteristic") %>% select(-PickList, -SampleFractionRequired) #sjh could drop columns 5-7
anom_crit <- sqlFetch(VM2.sql,"dbo.tlu_AnomCrit") #sjh..should be converted to tidy table in database see comments
type <- sqlFetch(VM2.sql,"dbo.tlu_Type") %>% select(TypeID,TypeIDText) #sjh could drop columns 5-9
odbcClose(VM2.sql)

 #### Precision Checks & QC summaries ####
#res$LOQ[is.na(res$LOQ)] <- 0  # sjh remove NA's from LOQ field  - LAM 11/19/2020 - is this correct

### This step determines if RPD or absolute difference should be used based the Low Level QC value
{QC_calc_M <- res %>% 
  left_join(chars, by = 'CharIDText') %>% # join char table to get charid
  left_join(QCcrit, by = c('CharID'= 'charid','CharIDText'='charidText')) %>% 
  filter(sample_type == 'sample') %>%
  group_by(row_ID,CharIDText) %>% 
  mutate(n = n(),  
         USE = case_when(n>1 & Result < Low_QC & QCcalc == 'RPD' ~ 0, 
                         n>1 & Result > Low_QC & QCcalc == 'AbsDiff' ~ 0,
                         TRUE ~ 1)) %>% 
  filter(USE == 1) %>% 
  select(row_ID,CharIDText,QCcalc,DQLA,DQLB,USE) 

# Adds a precision grade to duplicate samples 
prec_grade <- res %>% 
        left_join(QC_calc_M, by = c('row_ID','CharIDText')) %>% 
        group_by(row_ID,CharIDText) %>% 
        mutate(n = n(),
               Dup = ifelse(n() > 1, 1, 0)) %>%
        ungroup() %>% 
        filter(Dup == 1) %>% # pulls out primary and dups pairs 
        select(row_ID,LASAR_ID,DateTime,Char,CharIDText,actgrp_char,sub_char,sample_type,Result,result_id,QCcalc,DQLA,DQLB,LOQ) %>%
        group_by(row_ID,CharIDText) %>%
        arrange(desc(sample_type), .by_group = TRUE) %>% # arranges the dataset so that the primary is the first value
        mutate(prec_val = case_when(QCcalc == "LogDiff" ~
                                    log10(Result) - log10(lag(Result, default = first(Result))),
                                    QCcalc == "AbsDiff" ~ abs(Result - lag(Result, default = first(Result))),  # using abs here may mess up the charts and statisitcs.
                                    QCcalc == "RPD" ~ 
                                    abs((Result - lag(Result, default = first(Result)))/mean(Result))),
               use_DQLA = as.numeric(ifelse(QCcalc == 'AbsDiff' & DQLA < LOQ, LOQ ,DQLA)), # Change QC criteria when the abslolute difference criteria is less than the limit of quantitation 
               use_DQLB = as.numeric(ifelse(QCcalc == 'AbsDiff' & DQLB < LOQ, LOQ*2,DQLB)), # Change QC criteria when the abslolute difference criteria is less than the limit of quantitation
               prec_DQL = case_when(prec_val <= use_DQLA ~ "A", 
                                    prec_val > use_DQLA & prec_val <= use_DQLB ~ "B",
                                    TRUE ~ "C")) %>%
        filter(sample_type == 'dup') %>% # filter dup to get the prec_value 
        select(row_ID,CharIDText,result_id,QCcalc,prec_val,use_DQLA,use_DQLB,prec_DQL)  # 

# add prec grade to result, 
res_prec_grade <- res %>% left_join(prec_grade, by = c('row_ID','result_id','CharIDText')) #%>% # This seems to assign DQL to only dup rather than FP and FD
# removed for now - select(row_ID,LASAR_ID,DateTime,subid,act_type,sample_type,TypeIDText,TypeShortName,Date4Id,Date4group,act_id,
# removed for now  - act_group,CharIDText,result_id,sub_char,actgrp_char,Result,use_DQLA,use_DQLB,prec_val,prec_DQL)

## calculate the %QC and %DQL 
Prelim_DQL_AG_char <- res_prec_grade %>% 
                      select(row_ID,LASAR_ID,DateTime,subid,act_type,sample_type,TypeIDText,TypeShortName,Date4Id,Date4group,act_id,
                      act_group,CharIDText,result_id,sub_char,actgrp_char,Result,use_DQLA,use_DQLB,prec_val,prec_DQL) %>%
                      mutate(A = ifelse(prec_DQL %in% 'A',1,0),
                             B = ifelse(prec_DQL %in%  'B',1,0),
                             C = ifelse(prec_DQL %in%  'C', 1, 0),
                             QC = ifelse(sample_type == 'dup', 1, 0), # Is this different
                             nonQC = ifelse(sample_type == 'sample', 1, 0)) %>%
                      group_by(actgrp_char) %>%  # I think with new approach to sampler batches this will work b/c no longer many to many
                      summarize(grp_count = n(),
                              sumofQC = sum(QC),
                              sumofNonQC = sum(nonQC),
                              sumofA = sum(A),
                              sumofB = sum(B),
                              sumofC = sum(C)) %>%
                      mutate(pctQC = (sumofQC/grp_count), # seen
                           pctQCtononQC = (sumofQC/sumofNonQC), # I'm not sure 
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
  filter(!prelim_dql == 'Mixed') #%>%
  #select(LASAR_ID,CharIDText,act_id,act_group,result_id,actgrp_char,prec_DQL,prelim_dql,QCcalc)

# Spread the Prelim grades to the results to mixed results 
Prelim_DQL_Mixed <- res %>% 
  left_join(Prelim_DQL_AG_char, by = 'actgrp_char')  %>%
  filter(prelim_dql == 'Mixed') 
 
# This runs when there are mixed QC in the activity 
if(nrow(Prelim_DQL_Mixed)>0){
  Grade_mixed_QC <- Prelim_DQL_Mixed  %>%
  left_join(prec_grade, by = c('row_ID','result_id','CharIDText')) %>% #changed input of Prelim_DQL_Mixed
  group_by(row_ID,CharIDText) %>%
  arrange(desc(sample_type), .by_group = TRUE) %>%
  #mutate(prelim_dql = ifelse(sample_type == 'dup' & !is.na(prec_DQL),prec_DQL,prelim_dql)) %>%
  select(row_ID,LASAR_ID,CharIDText,act_id,act_group,result_id,actgrp_char,sample_type,prec_DQL,prelim_dql, DateTime) %>%
  arrange(actgrp_char) %>%
  filter(sample_type == "dup")
 

# This bit figures out when the DQLs apply
# This sets the start and end dates for when the QC applies. 
# Done by actgrp_char group
  # if first QC in group, set start of that grade's applicability 1 year earlier
    #this makes sure results from before this QC get assigned first QC grade
      #adjust if needed
# when parsing out the below case_when statement, remember that in R, A < B. 


QC_applicability <- Grade_mixed_QC %>%
  ungroup() %>%
  group_by(actgrp_char) %>%
  # mutate(groupposition = 1:n(),
  #        IsMaxDate = ifelse(DateTime == max(DateTime),1,0),
  #        MaXDate = max(DateTime)) %>%
  arrange(actgrp_char, DateTime) %>%
  mutate(ApplicableStart = case_when(DateTime == min(DateTime) ~ DateTime - years(1),
                                     prec_DQL < lag(prec_DQL, n = 1) ~ DateTime,
                                     prec_DQL > lag(prec_DQL, n = 1) ~ lag(DateTime) + seconds(2),
                                     prec_DQL == lag(prec_DQL, n = 1) ~ DateTime,
                                     TRUE ~ DateTime + years(1000)
                                     ) ,
         ApplicableEnd = case_when(DateTime == max(DateTime) ~ DateTime + years(1),
                                   prec_DQL < lead(prec_DQL, n = 1) ~ DateTime + seconds(1),
                                   prec_DQL > lead(prec_DQL, n = 1) ~ lead(DateTime) - seconds(1),
                                   prec_DQL == lead(prec_DQL, n = 1) ~ lead(DateTime) - seconds(1),
                                   TRUE ~ DateTime + years(1000))) %>%
  ungroup() %>%
  select(actgrp_char,prec_DQL,ApplicableStart, ApplicableEnd)


#join the WC dataframe to results dataframe to find DQL value
# fuzzy_left_join will join between dates

 grade_mixed_result <- Prelim_DQL_Mixed %>% 
  fuzzy_left_join(QC_applicability, by = c('actgrp_char' = 'actgrp_char', # ==
                                           'DateTime' = 'ApplicableStart', # DT >= AS
                                           'DateTime' = 'ApplicableEnd'), # DT <= AE
                  match_fun = c( `==`, `>=`, `<=` )) %>%
  mutate(prelim_dql = prec_DQL) %>%
  select(-actgrp_char.y, -ApplicableStart, -ApplicableEnd, -prec_DQL ) %>%
  rename('actgrp_char' = 'actgrp_char.x') 

 Prelim_DQL_All <- rbind(Prelim_DQL_nonMixed,grade_mixed_result)}
  
# This runs if there are isn't mixed QC in the activity 
if(nrow(Prelim_DQL_Mixed) == 0) {
Prelim_DQL_All <- Prelim_DQL_nonMixed }}

#### generate a table of anomolies for each result #### 
{sub_char_precen <- Prelim_DQL_All %>%
                group_by(CharIDText) %>% 
                summarise(percen_5th = quantile(Result, probs = .05),
                          percen_10th = quantile(Result, probs = .10), 
                          percen_90th = quantile(Result, probs = .90),
                          percen_95th = quantile(Result, probs = .95))
station_char_percen <- Prelim_DQL_All%>%
                       group_by(LASAR_ID, CharIDText) %>% 
                       summarise(S_percen_5th = quantile(Result, probs = .05), 
                       S_percen_95th = quantile(Result, probs = .95))
                
anom <- Prelim_DQL_All %>% 
  #rename(LOQ = 'Limit of Quantitation') %>%
        left_join(anom_crit, by = c('CharIDText' = 'charidText')) %>%
        left_join(sub_char_precen, by= 'CharIDText') %>%
        left_join(station_char_percen, by= c('LASAR_ID','CharIDText')) %>%
        select(LASAR_ID,DateTime,CharIDText,subid,TypeIDText,Result,act_id,act_group,result_id,AnomCritID,charid,LOQ,RealRngL,RealRngU,
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
        gather(key = "Anom_type", value = "Anom",-c(LASAR_ID,DateTime,CharIDText,subid,TypeIDText,
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
  replace(is.na(.), 0) }

final_DQL <- Prelim_DQL_All %>%
  left_join(anom_res_sum, by = "result_id") %>%
           mutate(final_DQL = case_when(prelim_dql < "C" & anom_amb_99 > 0 ~ "Anom", 
                                        prelim_dql < "C" & anom_sub_95 > 0 & anom_amb_95 > 0 ~ "Anom",
                                        prelim_dql < "C" & anom_amb_95 > 0 & anom_WQS > 0 ~ "Anom",
                                        prelim_dql < "C" & anom_WQS > 0 ~ "Anom",
                                        prelim_dql > "H"  ~ "Anom", # this pulls out QC less than 10% - not sure why %like% didn't work
                                        TRUE ~ as.character( prelim_dql))) %>%
  mutate(DQLCmt = "") 
 .GlobalEnv$final_DQL <- final_DQL
 .GlobalEnv$QC_calc_M <- QC_calc_M}

