## L. Merrick 
## This script will export a .csv to upload to AWQMS
### THis should only be run after the submission is manually reviewed in the VolMon database 

VM2.sql <- odbcConnect("VolMon2")
load("Step1.RData")
### this is dependent on the submission 


#VM2.sql <- odbcConnect("VolMon2") ### this requires an ODBC connection to the VOLMON2 on DEQLEAD-LIMS/dev
stations <- sqlFetch(VM2.sql, "dbo.tlu_Stations") 
type <- sqlFetch(VM2.sql,"dbo.tlu_Type")
chars <- sqlFetch(VM2.sql,"dbo.tlu_Characteristic")
units <- sqlFetch(VM2.sql, "dbo.tlu_units")
context <- sqlFetch(VM2.sql, "dbo.tlu_Context")
method <- sqlFetch(VM2.sql, "dbo.tlu_Method")
tz <- sqlFetch(VM2.sql, "dbo.tlu_Method")

#building an SQL query for this submission    
a_ids <- act$ActivityIDText
a_idsString <- toString(sprintf("'%s'", a_ids))
a_sql_fmt <- "SELECT * FROM t_Activity WHERE ActivityIDText IN (%s)"
qryActt <- sprintf(a_sql_fmt, a_idsString)

### pulls in activity info ###
db_act <- sqlQuery(VM2.sql, qryActt)  %>% 
  filter(SubID == sub_id) %>% 
  select(ActivityTypeId,StnID,StartDateTime,StartDateTimeZoneID,EndDateTime,
         EndDateTimeZoneID,SmplColEquipComment,SmplDepth,SmplDepthUnitID,Org_Comment)    

r_ids <- res_t$ResultIDText
r_idsString <- toString(sprintf("'%s'", r_ids))
r_sql_fmt <- "SELECT * FROM t_Result WHERE ResultIDText IN (%s)"
# build an SQL query
qryRest <- sprintf(r_sql_fmt, r_idsString)

### MUST CONFIRM THIS IS ZERO BEFORE LOADING DATA ###
QC_Res <- sqlQuery(VM2.sql, qryRest)    
