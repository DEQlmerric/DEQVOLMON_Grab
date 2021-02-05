### Upload to VolMon and AWQMS uploads 
## 
library(tidyverse)
library(RODBC) 
library(odbc)
library(lubridate)
#library(sqldf)
#library(DBI)

VM2.sql <- odbcConnect("VolMon2") ### this requires an ODBC conection to the VOLMON2 on DEQLEAD-LIMS/dev
stations <- sqlFetch(VM2.sql, "dbo.tlu_Stations") 
units <- sqlFetch(VM2.sql, "dbo.tlu_units")
context <- sqlFetch(VM2.sql, "dbo.tlu_Context")
#odbcClose(VM2T.sql)

#### t_ActGrp ####
ActGroup <- final_DQL %>%
            select(act_group,Date4group) %>% 
            distinct() %>% 
            mutate(ActGrpIDText = act_group,  
                   ActGrpTypeID = 311) %>%# I need to understand what the other options are  
            rename(ActGrpComment = Date4group) %>% # what is the comment? 
            select(ActGrpIDText,ActGrpComment,ActGrpTypeID)

write.csv(ActGroup, "ActGroup.csv")
#### write to the database defined by the connection 

#build a vector string of ActGrpIDTexts for use in SQL query
ag_ids <- ActGroup$ActGrpIDText 
ag_idsString <- toString(sprintf("'%s'", ag_ids))
ag_sql_fmt <- "SELECT * FROM t_ActGrp WHERE ActGrpIDText IN (%s)"
# build an SQL query
qryAGt <- sprintf(ag_sql_fmt, ag_idsString)

### MUST CONFIRM THIS IS ZERO BEFORE LOADING DATA ###
QC_actgroup <- sqlQuery(VM2.sql, qryAGt)             

### Write activity group to the DB ###
# creates a temporary table 
sqlSave(VM2.sql, ActGroup, tablename = "TempActGrp", append = TRUE, rownames = FALSE, colnames = FALSE, safer = TRUE)
#SQL insert query
qryAG <- "INSERT INTO t_ActGrp (ActGrpIDText, ActGrpComment, ActGrpTypeID)SELECT ActGrpIDText, ActGrpComment, ActGrpTypeID FROM TempActGrp;"
# run the SQL query 
sqlQuery(VM2.sql, qryAG, max = 0, buffsize = length(ActGroup$ActGrpIDText))
# delete temp table 
sqlDrop(VM2.sql, "TempActGrp")

### MUST CONFIRM THIS NUMBER MATCHES ActGroup ###
QC_actgroup <- sqlQuery(VM2.sql, qryAGt)             

#### t_Activity ####
act <- final_DQL %>% 
       select(act_id,act_type,subid,LASAR_ID,DateTime,SubOrganizationID,
              samplers,SampleDepth,SampleDepthUnit,DEQ_Comment,Org_Comment) %>% 
       left_join(stations, by = c('LASAR_ID' = 'STATION_KEY')) %>%
       left_join(context, by = c('ContextID'= 'CntextIdText')) %>% # what is this for? 
       #as.character(res$SampleDepthUnit) %>%
       #left_join(units, by = c('SampleDepthUnit' = 'UnitIdText')) # this isn't working beacuse of NA
       mutate(SmplDepthUnitID = ifelse(SampleDepthUnit %like% 'meters', 146,
                                ifelse(SampleDepthUnit %like% 'feet', 72, NA)),
              StartDateTimeZoneID = ifelse(dst(DateTime) == T,22, 23), # add mountian time
              EndDateTime = NA,
              EndDateTimeZoneID = NA,
              MediaID = 8, #this is null on my test data set - ifelse? 
              SmplColMthdID = NA,
              SmplColEquip = NA,
              SmplEquipID = NA,
              SmplColEquipComment = NA) %>%
       select(act_id,act_type,subid,StationDes,DateTime,StartDateTimeZoneID,
         EndDateTime,EndDateTimeZoneID,MediaID,SubOrganizationID,SmplColMthdID,SmplColEquip,
         SmplEquipID,SmplColEquipComment,SampleDepth,SmplDepthUnitID,Org_Comment,DEQ_Comment,samplers,StnID) %>%  
       rename(ActivityIDText = act_id, ActivityTypeId =act_type,SubID = subid,
              SiteDescription = StationDes, StartDateTime = DateTime,
              ActivityOrgID = SubOrganizationID,SmplDepth =SampleDepth, Samplers = samplers,fkStnID = StnID) %>%
       distinct()

write.csv(act, "act.csv")

#### Interact with the database ###
#build a vector string of ActivityIDTexts for use in SQL query
a_ids <- act$ActivityIDText
a_idsString <- toString(sprintf("'%s'", a_ids))
a_sql_fmt <- "SELECT * FROM t_Activity WHERE ActivityIDText IN (%s)"
# build an SQL query
qryActt <- sprintf(a_sql_fmt, a_idsString)

### MUST CONFIRM THIS IS ZERO BEFORE LOADING DATA ###
QC_act <- sqlQuery(VM2.sql, qryActt)    

# write to the database defined by the connection 

# define variable types
columnTypes <- list(ActivityIDText = "nvarchar(255)",ActivityTypeId = "int" ,SubID = "int",
                    SiteDescription = "nvarchar(255)", StartDateTime = "datetime",
                    StartDateTimeZoneID = "int",EndDateTime = "datetime",EndDateTimeZoneID = "int", MediaID = "int",
                    ActivityOrgID = "int",SmplColMthdID ="int", SmplColEquip ="nvarchar(255)",SmplEquipID = "nvarchar(255)",
                    SmplColEquipComment= "nvarchar(255)",SmplDepth ="nvarchar(255)",SmplDepthUnitID = "int",
                    Org_Comment = "nvarchar(255)",DEQ_Comment = "nvarchar(255)",Samplers="nvarchar(255)",fkStnID = "int")
# creates a temporary table
VM2.sql <- odbcConnect("VolMon2")
sqlSave(VM2.sql, act, tablename = "TempAct", append = TRUE, rownames = FALSE, colnames = FALSE, safer = TRUE, varTypes = columnTypes)
#SQL insert query

#SQL insert query
qryA <- "INSERT INTO t_Activity 
          SELECT * FROM TempAct;"

# run the SQL query 
sqlQuery(VM2.sql, qryA, max = 0, buffsize = length(act$ActivityIDText))

### confirm load number matched act
QC_act <- sqlQuery(VM2.sql, qryActt)

# delete content of temp table 
qryA_clear <- "DELETE FROM TempAct;"
sqlQuery(VM2.sql, qryA_clear)


#### tjct_ActGrp2Act ####
ActGrp2Act <- final_DQL %>%
              select(act_group,Date4group, act_id) %>%
              left_join(QC_act, by = c('act_id' = 'ActivityIDText'))%>%
              left_join(QC_actgroup, by = c('act_group' = 'ActGrpIDText')) %>%
              select(ActGrpID,ActivityID,Date4group) %>%
              rename(AG2AComment = Date4group)

#build a vector string of ActGrpIDTexts for use in SQL query
act_ids <- ActGrp2Act$ActivityID 
act_idsString <- toString(sprintf("%s", act_ids))
act_sql_fmt <- "SELECT * FROM tjct_ActGrp2Act WHERE ActivityID IN (%s)"
# build an SQL query
qryact2grp <- sprintf(act_sql_fmt, act_idsString)

### MUST CONFIRM THIS IS ZERO BEFORE LOADING DATA ###
QC_act2group <- sqlQuery(VM2.sql, qryact2grp)             

### Write activity group to the DB ###
# creates a temporary table 
sqlSave(VM2.sql, ActGrp2Act, tablename = "TempActGrp2Act", append = TRUE, rownames = FALSE, colnames = FALSE, safer = TRUE)

#SQL insert query
qryA2G <- "INSERT INTO tjct_ActGrp2Act 
          SELECT * FROM TempActGrp2Act;"

# run the SQL query 
sqlQuery(VM2.sql, qryA2G, max = 0, buffsize = length(ActGrp2Act$ActivityID))

### confirm load number matched act
QC_act2group <- sqlQuery(VM2.sql, qryact2grp) 

# delete content of temp table 
qryA2G_clear <- "DELETE FROM TempActGrp2Act;"
sqlQuery(VM2.sql, qryA2G_clear)

#### t_Result ####
# read activity table to get the unique ID
VM2.sql <- odbcConnect("VolMon2")
# activity <- sqlFetch(VM2.sql, "dbo.t_Activity") - trying to use the QC table because it is faster that pulling in the entire table
units <- sqlFetch(VM2.sql, "dbo.tlu_units")
method <- sqlFetch(VM2.sql, "dbo.tlu_Method")
chars <- sqlFetch(VM2.sql,"dbo.tlu_Characteristic")

res_t <- final_DQL  %>% 
       select(result_id,act_id,LASAR_ID,CharIDText,Result,RsltQual,'Method Short Name','MethodSpeciation',UNITS,
              LOQ,FieldOrLab,Res_comment,OG_ACC,OG_PREC,OG_DQL,final_DQL) %>%
       left_join(chars, by = 'CharIDText') %>%
       left_join(units, by = c('UNITS' = 'UnitIdText')) %>%
       left_join(method, by = c('Method Short Name' = 'ShortName')) %>%
       left_join(QC_act, by = c('act_id' = 'ActivityIDText')) %>%  # this reduces having to pull in the whole activity table 
       mutate(RsltTypeID = 19, # have to account for nondetect in ?
              AnalyticalLaboratoryID = NA,  # will these be in the template if FieldOrLab = lab?
              AnalyticalStartTime = NA,
              AnalyticalStartTimeZoneID = NA,
              AnalyticalEndTime = NA,
              AnalyticalEndTimeZoneID = NA,
              LabCommentCode = NA,
              LOQ_UnitID = UnitID, # I don't know about this
              DEQ_ACC = NA,
              BiasValue = NA,
              StatisticalBasisID = NA,
              RsltTimeBasisID = NA,
              DEQ_RsltComment = NA) %>%
       left_join(prec_grade, by = 'result_id') %>%
       mutate(RsltStatusID = 7) %>%  # all data goes in as Prelim?
       select(result_id,ActivityID,CharID,Result,RsltQual,UnitID,MethodID,MethodSpeciation,RsltTypeID, 
              AnalyticalLaboratoryID,AnalyticalStartTime,AnalyticalStartTimeZoneID,AnalyticalEndTime,
              AnalyticalEndTimeZoneID,LabCommentCode,LOQ,LOQ_UnitID,OG_ACC,OG_PREC,OG_DQL,DEQ_ACC,prec_DQL,
              BiasValue, prec_val,final_DQL,StatisticalBasisID,RsltTimeBasisID,StoretUniqueID,Res_comment,
              DEQ_RsltComment,RsltStatusID) %>%
      rename(ResultIDText = result_id,PrecisionValue = prec_val,DEQ_PREC = prec_DQL,ORDEQ_DQL = final_DQL,
             Org_RsltComment = Res_comment, StoretQualID = StoretUniqueID,QCorigACC = OG_ACC, QCorigPREC = OG_PREC,
             QCorigDQL = OG_DQL)

write.csv(res_t, "res_t.csv")

## QC checks for NULLS - Confirm this is zero if not fix ###
# checks for Nulls in not null columns  
res_nulls <- res_t %>% filter(is.null(ActivityID)|is.null(CharID)|is.null(Result)|
                  is.null(UnitID)|is.null(MethodID)|is.null(RsltTypID)|
                  is.null(RsltStatusID))

#### Interact with the database ###
#build a vector string of ActivityIDTexts for use in SQL query
######## Add the part to get 
#left_join(activity, by = c('act_id' = 'ActivityIDText','LASAR' = 'SiteID')) %>%
r_ids <- res_t$ResultIDText
r_idsString <- toString(sprintf("'%s'", r_ids))
r_sql_fmt <- "SELECT * FROM t_Result WHERE ResultIDText IN (%s)"
# build an SQL query
qryRest <- sprintf(r_sql_fmt, r_idsString)

### MUST CONFIRM THIS IS ZERO BEFORE LOADING DATA ###
QC_Res <- sqlQuery(VM2.sql, qryRest)    

# write to the database defined by the connection 

# define variable types
r_columnTypes <- list(ResultIDText = "nvarchar(255)",ActivityID = "int" ,CharID = "int",Result = "nvarchar(255)",
                    RsltQual = "nvarchar(255)", UnitID = "int", MethodID = "int",MethodSpeciation = "nvarchar(255)",
                    RsltTypeID = "int",AnalyticalLaboratoryID = "int", AnalyticalStartTime = "datetime",
                    AnalyticalStartTimeZoneID = "int", AnalyticalEndTime = "datetime", AnalyticalEndTimeZoneID = "int",
                    LabCommentCode = "nvarchar(255)", LOQ = "nvarchar(255)",LOQ_UnitID = "int", QCorigACC = "nvarchar(255)",
                    QCorigPREC = "nvarchar(255)", QCorigDQL = "nvarchar(255)",DEQ_ACC = "nvarchar(255)",DEQ_PREC = "nvarchar(255)",
                    BiasValue = "nvarchar(255)",PrecisionValue = "nvarchar(255)",ORDEQ_DQL = "nvarchar(255)",StatisticalBasisID = "int",
                    RsltTimeBasisID = "int", StoretQualID = "int",Org_RsltComment = "nvarchar(max)",
                    DEQ_RsltComment = "nvarchar(max)",RsltStatusID = "int")

# creates a temporary table
sqlSave(VM2.sql, res_t, tablename = "TempResult", append = TRUE, rownames = FALSE, colnames = FALSE, safer = TRUE, varTypes = r_columnTypes)

#SQL insert query
qryR <- "INSERT INTO t_Result 
          SELECT * FROM TempResult;"

# run the SQL query 
sqlQuery(VM2.sql, qryR, max = 0, buffsize = length(res_t$ResultIDText))

### confirm load number matched act
QC_Res <- sqlQuery(VM2.sql, qryRest)

# clear temp table 
qryR_clear <- "DELETE FROM TempResult;"
sqlQuery(VM2.sql, qryR_clear)


####t_anom ####
#VM2.sql <- odbcConnect("VolMon_testload")
#result <- sqlFetch(VM2.sql, "dbo.t_Result")- using QC_res we don't have to pull in the entire results table 

anom_t <- anom %>% 
          left_join(type, by = c('Anom' = 'TypeIDText')) %>% 
          select(result_id,TypeID) %>% 
          left_join(QC_Res, by = c('result_id' = 'ResultIDText')) %>%
          mutate(AnomalyComment = NA) %>%
          select(ResultID,AnomalyComment,TypeID) %>% 
          rename(AnomalyTypeID = TypeID)

#write.csv(anom_t, "anom_t.csv")
          
#### Interact with the database ###
#build a vector string of ResultIDTexts for use in SQL query # 
anom_ids <- anom_t$ResultID
anom_idsString <- toString(sprintf("'%s'", anom_ids))
anom_sql_fmt <- "SELECT * FROM t_Anomaly WHERE ResultID IN (%s)"
# build an SQL query
qryAnom <- sprintf(anom_sql_fmt, anom_idsString)

### MUST CONFIRM THIS IS ZERO BEFORE LOADING DATA ###
QC_Anom <- sqlQuery(VM2.sql, qryAnom)    

# write to the database defined by the connection 
anom_columnTypes <- list(ResultID = "int", AnomalyComment = "nvarchar(255)", AnomalyTypeID = "int")

# creates a temporary table
sqlSave(VM2.sql, anom_t, tablename = "TempAnomaly", append = TRUE, rownames = FALSE, colnames = FALSE, safer = TRUE, varTypes = anom_columnTypes)

#SQL insert query
qryAn <- "INSERT INTO t_Anomaly
          SELECT * FROM TempAnomaly;"

# run the SQL query 
sqlQuery(VM2.sql, qryAn, max = 0, buffsize = length(anom_t$ResultID))

### confirm load number matched act
QC_Anom <- sqlQuery(VM2.sql, qryAnom)

# delete content of temp table 
qryanom_clear <- "DELETE FROM TempAnomaly;"
sqlQuery(VM2.sql, qryanom_clear)


#### Grab data for Review #### 

gd4r <- final_DQL %>%
        select(result_id,subid,DateTime,prelim_dql,anom_sub_95,anom_amb_99,
               anom_amb_95,anom_WQS,anom_BelowLOQ,DQLCmt) %>%
        left_join(QC_Res, by = c('result_id' = 'ResultIDText')) %>% 
        select(ResultID,ActivityID,subid,CharID,DateTime,
               Result,RsltQual,UnitID,StoretQualID,QCorigDQL,DEQ_PREC,
               prelim_dql,ORDEQ_DQL,anom_sub_95,anom_amb_99,
               anom_amb_95,anom_WQS,anom_BelowLOQ,DQLCmt) %>%
       rename(SubmissionID = subid, StartDateTime = DateTime,  prelim_DQL = prelim_dql,
              final_DQL= ORDEQ_DQL)
              
#### Interact with the database ###
#build a vector string of ResultIDTexts for use in SQL query
gd4r_ids <- gd4r$ResultID
gd4r_idsString <- toString(sprintf("'%s'", gd4r_ids))
gd4r_sql_fmt <- "SELECT * FROM grabdataforreview WHERE ResultID IN (%s)"
# build an SQL query
qrygd4r <- sprintf(gd4r_sql_fmt, gd4r_idsString)

### MUST CONFIRM THIS IS ZERO BEFORE LOADING DATA ###
QC_gd4r <- sqlQuery(VM2.sql, qrygd4r)    

# write to the database defined by the connection 
gd4r_columnTypes <- list(ResultID = "int", ActivityID = "int", SubmissionID = "int",
                         CharID = "int",StartDateTime = "datetime",Result = "float",
                         RsltQual = "nvarchar(255)",UnitID = "int",StoretQualID = "int",
                         QCorigDQL = "nvarchar(255)",DEQ_PREC = "nvarchar(255)", 
                         DEQ_PREC = "nvarchar(255)", prelim_dql = "nvarchar(255)",
                         final_DQL = "nvarchar(255)", anom_sub_95 = "int", anom_amb_99 = "int",
                         anom_amb_95 = "int", anom_WQS = "int", anom_BelowLOQ = "int",
                         DQLCmt = "nvarchar(255)")
write.csv(gd4r,"gd4r.csv")
# creates a temporary table
sqlSave(VM2.sql, gd4r, tablename = "TempGd4r", append = TRUE, rownames = FALSE, colnames = FALSE, safer = TRUE, varTypes = gd4r_columnTypes)

#SQL insert query
qryGD <- "INSERT INTO grabdataforreview
          SELECT * FROM TempGd4r;"

# run the SQL query 
sqlQuery(VM2.sql, qryGD, max = 0, buffsize = length(gd4r$ResultID))

### confirm load number matched act
QC_gd4r <- sqlQuery(VM2.sql, qrygd4r) 

# delete temp table 
qrygd4r_clear <- "DELETE FROM TempGd4r;"
sqlQuery(VM2.sql, qrygd4r_clear)
