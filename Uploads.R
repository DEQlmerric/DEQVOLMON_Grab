### Upload to VolMon and AWQMS uploads 
## 
library(tidyverse)
library(RODBC) 
library(odbc)
library(lubridate)
#library(sqldf)
#library(DBI)

VM2T.sql <- odbcConnect("VolMon_testload") ### this requires an ODBC conection to the VOLMON2 on DEQLEAD-LIMS/dev
#t.ActGrp <- sqlFetch(VM2T.sql, "dbo.t_ActGrp") 
stations <- sqlFetch(VM2T.sql, "dbo.tlu_Stations") # how does this get populated?
units <- sqlFetch(VM2T.sql, "dbo.tlu_units")
context <- sqlFetch(VM2T.sql, "dbo.tlu_Context")
#odbcClose(VM2T.sql)

#### t_ActGrp ####
ActGroup <- Prelim_DQL_All %>%
            select(act_group,Date4group) %>% 
            distinct() %>% 
            mutate(ActGrpIDText = paste(act_group,"Test4",sep = "-"),  #### update me to get rid of test!
            ActGrpTypeID = 311) %>%# I need to understand what the other options are  
            rename(ActGrpComment = Date4group) %>% # what is the comment? 
            select(ActGrpIDText,ActGrpComment,ActGrpTypeID)

#### write to the database defined by the connection 

#build a vector string of ActGrpIDTexts for use in SQL query
ag_ids <- ActGroup$ActGrpIDText 
ag_idsString <- toString(sprintf("'%s'", ag_ids))
ag_sql_fmt <- "SELECT * FROM t_ActGrp WHERE ActGrpIDText IN (%s)"
# build an SQL query
qryAGt <- sprintf(ag_sql_fmt, ag_idsString)

### MUST CONFIRM THIS IS ZERO BEFORE LOADING DATA ###
QC_actgroup <- sqlQuery(VM2T.sql, qryAGt)             

### Write activity group to the DB ###
# creates a temporary table 
sqlSave(VM2T.sql, ActGroup, tablename = "TempActGrp", append = FALSE, rownames = FALSE, colnames = FALSE, safer = TRUE)
#SQL intset query
qryAG <- "INSERT INTO t_ActGrp (ActGrpIDText, ActGrpComment, ActGrpTypeID)
SELECT ActGrpIDText, ActGrpComment, ActGrpTypeID FROM TempActGrp;"
# run the SQL query 
sqlQuery(VM2T.sql, qryAG, max = 0, buffsize = length(ActGroup$ActGrpIDText))
# delete temp table 
sqlDrop(VM2T.sql, "TempActGrp")

### MUST CONFIRM THIS NUMBER MATCHES ActGroup ###
QC_actgroup <- sqlQuery(VM2T.sql, qryAGt)             

#### t_Activity ####
act <- Prelim_DQL_All %>% 
       select(act_id,act_type,subid,LASAR,DateTime,SubOrganizationID,
              samplers,SampleDepth,SampleDepthUnit,DEQ_Comment,Org_Comment) %>% 
       left_join(stations, by = c('LASAR' = 'STATION_KEY')) %>%
       left_join(context, by = c('OrgID'= 'CntextIdText')) %>%
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
       select(act_id,act_type,subid,LASAR,ContextID,StationDes,DateTime,StartDateTimeZoneID,
         EndDateTime,EndDateTimeZoneID,MediaID,SubOrganizationID,SmplColMthdID,SmplColEquip,
         SmplEquipID,SmplColEquipComment,SampleDepth,SmplDepthUnitID,Org_Comment,DEQ_Comment,samplers) %>%  
       rename(ActivityIDText = act_id, ActivityTypeId =act_type,SubID = subid,SiteID = LASAR,
              SiteContextID = ContextID, SiteDescription = StationDes, StartDateTime = DateTime,
              ActivityOrgID = SubOrganizationID,SmplDepth =SampleDepth, Samplers = samplers) %>%
       distinct()
       as.character(act$SiteID)

#### Interact with the database ###
#build a vector string of ActivityIDTexts for use in SQL query
a_ids <- act$ActivityIDText
a_idsString <- toString(sprintf("'%s'", a_ids))
a_sql_fmt <- "SELECT * FROM t_Activity WHERE ActivityIDText IN (%s)"
# build an SQL query
qryActt <- sprintf(a_sql_fmt, a_idsString)

### MUST CONFIRM THIS IS ZERO BEFORE LOADING DATA ###
QC_act <- sqlQuery(VM2T.sql, qryActt)    

# write to the database defined by the connection 

# define variable types
columnTypes <- list(ActivityIDText = "nvarchar(255)",ActivityTypeId = "int" ,SubID = "int",SiteID = "nvarchar(255)",
                    SiteContextID = "int", SiteDescription = "nvarchar(255)", StartDateTime = "datetime",
                    StartDateTimeZoneID = "int",EndDateTime = "datetime",EndDateTimeZoneID = "int", MediaID = "int",
                    ActivityOrgID = "int",SmplColMthdID ="int", SmplColEquip ="nvarchar(255)",SmplEquipID = "nvarchar(255)",
                    SmplColEquipComment= "nvarchar(255)",SmplDepth ="nvarchar(255)",SmplDepthUnitID = "int",
                    Org_Comment = "nvarchar(255)",DEQ_Comment = "nvarchar(255)",Samplers="nvarchar(255)")
# creates a temporary table
sqlSave(VM2T.sql, act, tablename = "TempAct", append = FALSE, rownames = FALSE, colnames = FALSE, safer = TRUE, varTypes = columnTypes)

#SQL insert query
qryA <- "INSERT INTO t_Activity 
          SELECT * FROM TempAct;"

# run the SQL query 
sqlQuery(VM2T.sql, qryA, max = 0, buffsize = length(act$ActivityIDText))

### confirm load number matched act
QC_act <- sqlQuery(VM2T.sql, qryActt)

# delete temp table 
sqlDrop(VM2T.sql, "TempAct")

#### t_Result ####
# read activity table to get the unique ID
activity <- sqlFetch(VM2T.sql, "dbo.t_Activity")
units <- sqlFetch(VM2T.sql, "dbo.tlu_units")
method <- sqlFetch(VM2T.sql, "dbo.tlu_Method")
auto_DQL <- grade_prelim_DQL %>% 
       select(result_id,Auto_DQL)
        
res_t <- Prelim_DQL_All  %>% 
       select(result_id,act_id,LASAR,CharIDText,Result,RsltQual,MethodShortName,MethodSpeciation,UNITS,
              LOQ,FieldOrLab,Res_comment) %>%
       left_join(chars, by = 'CharIDText') %>%
       left_join(units, by = c('UNITS' = 'UnitIdText')) %>%
       left_join(method, by = c('MethodShortName' = 'ShortName')) %>%
       left_join(activity, by = c('act_id' = 'ActivityIDText','LASAR' = 'SiteID')) %>%
       mutate(RsltTyp = 19, # have to account for nondetect in ?
              AnalyticalLaboratoryID = NA,  # will these be in the template if FieldOrLab = lab?
              AnalyticalStartTime = NA,
              AnalyticalStartTimeZoneID = NA,
              AnalyticalEndTime = NA,
              AnalyticalEndTimeZoneID = NA,
              LabCommentCode = NA,
              LOQ_UnitID = UnitID, # I don't know about this
              QCorigACC = NA,
              QCorigPREC = NA,
              QCorigDQL = NA,
              DEQ_ACC = NA,
              BiasValue = NA,
              StatisticalBasisID = NA,
              RsltTimeBasisID = NA,
              DEQ_RsltComment = NA) %>% # where does come from? 
       left_join(prec_grade, by = 'result_id') %>%
       left_join(auto_DQL, by = 'result_id') %>%
       distinct() %>%
       mutate(RsltStatusID = case_when(Auto_DQL == 'A' ~ 6,
                                       Auto_DQL == 'B' ~ 6,
                                       Auto_DQL == 'E'~ 7,
                                       Auto_DQL == 'C'~ 8)) %>%  # change to prelim
       select(result_id,ActivityID,CharID,Result,RsltQual,UnitID,MethodID,MethodSpeciation,RsltTyp, 
              AnalyticalLaboratoryID,AnalyticalStartTime,AnalyticalStartTimeZoneID,AnalyticalEndTime,
              AnalyticalEndTimeZoneID,LabCommentCode,LOQ,LOQ_UnitID,QCorigACC,QCorigPREC,QCorigDQL,DEQ_ACC,
              BiasValue, StatisticalBasisID,RsltTimeBasisID, prec_val,prec_DQL,Auto_DQL,StoretUniqueID,Res_comment,DEQ_RsltComment,RsltStatusID) %>%
      rename(ResultIDText = result_id, PrecisionValue = prec_val,DEQ_PREC = prec_DQL,ORDEQ_DQL = Auto_DQL,
             Org_RsltComment = Res_comment, StoretQualID = StoretUniqueID)
             
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
QC_Res <- sqlQuery(VM2T.sql, qryRest)    

# write to the database defined by the connection 

# define variable types
r_columnTypes <- list(ResultIDText = "nvarchar(255)",ActivityID = "int" ,CharID = "int",Result = "nvarchar(255)",
                    RsltQual = "nvarchar(255)", UnitID = "int", MethodID = "int",MethodSpeciation = "nvarchar(255)",
                    RsltTyp = "int",AnalyticalLaboratoryID = "int", AnalyticalStartTime = "datetime",
                    AnalyticalStartTimeZoneID = "int", AnalyticalEndTime = "datetime", AnalyticalEndTimeZoneID = "int",
                    LabCommentCode = "nvarchar(255)", LOQ = "nvarchar(255)",LOQ_UnitID = "int", QCorigACC = "nvarchar(255)",
                    QCorigPREC = "nvarchar(255)", QCorigDQL = "nvarchar(255)",DEQ_ACC = "nvarchar(255)",DEQ_PREC = "nvarchar(255)",
                    BiasValue = "nvarchar(255)",PrecisionValue = "nvarchar(255)",ORDEQ_DQL = "nvarchar(255)",StatisticalBasisID = "int",RsltTimeBasisID = "int",
                    StoretQualID = "int",Org_RsltComment = "nvarchar(max)",DEQ_RsltComment = "nvarchar(max)",RsltStatusID = "int")
# creates a temporary table
sqlSave(VM2T.sql, res_t, tablename = "TempRes", append = FALSE, rownames = FALSE, colnames = FALSE, safer = TRUE, varTypes = r_columnTypes)

#SQL insert query
qryR <- "INSERT INTO t_Result 
          SELECT * FROM TempRes;"

# run the SQL query 
sqlQuery(VM2T.sql, qryR, max = 0, buffsize = length(res_t$ResultIDText))

### confirm load number matched act
QC_Res <- sqlQuery(VM2T.sql, qryRest)

# delete temp table 
sqlDrop(VM2T.sql, "TempRes")

write.csv(res_t,"res_t.csv")
####t_anom ####
anom_t <- anom %>%
          