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
### Precision Checks ### 

prec <- res %>% # source from the Format_template fxn?
        left_join(QCcrit, by = c('CharIDText'= 'charidText')) %>%
        group_by(LASAR,DateTime,CharIDText) %>% 
        mutate(Dup = ifelse(n() > 1, 1, 0)) %>%
        ungroup() %>% 
        filter(Dup == 1) %>% 
        select(LASAR,DateTime,Char,CharIDText,sample_type,Result,QCcalc,DQLA,DQLB) %>%
        group_by(LASAR,DateTime,CharIDText) %>%
        arrange(sample_type, .by_group = TRUE) %>%
        mutate(prec_val = case_when(QCcalc == "LogDiff" ~
                                    log10(Result) - log10(lag(Result, default = first(Result))),
                                    QCcalc == "AbsDiff" ~ abs(Result - lag(Result, default = first(Result))),
                                    QCcalc == "RPD" ~ 
                                    abs((Result - lag(Result, default = first(Result)))/mean(Result)))) %>%
        filter(prec_val == max(prec_val)) %>%
        mutate(prec_DQL = case_when(prec_val <= DQLA ~ "A",
                                    prec_val > DQLA & prec_val <= DQLB ~ "B",
                                    TRUE ~ "C")) 
                                            

anom <- res %>% 
        left_join(anom_crit, by = c('CharIDText' = 'charidText')) %>%
        select(LASAR,DateTime,CharIDText,subid,TypeIDText,act_id,act_group,result_id,AnomCritID,charid,RealRngL,RealRngU,
               amb01L,amb05L,amb95U,amb99U,stdL,stdU,stdLlp,stdUlp,AmbDataRange,StdRef) %>%
        mutate(RealRngU = case_when(Result > RealRngU ~ "OverUpRange"))
          

