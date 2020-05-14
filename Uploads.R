### Upload to VolMon and AWQMS uploads 
## 

library(lubridate)


VM2T.sql <- odbcConnect("VolMon_testload") ### this requires an ODBC conection to the VOLMON2 on DEQLEAD-LIMS/dev
t.ActGrp <- sqlFetch(VM2T.sql, "dbo.t_ActGrp") 
stations <- sqlFetch(VM2T.sql, "dbo.tlu_Stations") # how does this get populated?
units <- sqlFetch(VM2T.sql, "dbo.tlu_units")
context <- sqlFetch(VM2T.sql, "dbo.tlu_Context")

#### t_ActGrp ####
ActGroup <- res %>%
            select(act_group,Date4group) %>% 
            distinct() %>% 
            mutate(ActGrpIDText = paste(act_group,"Test2",sep = "-"),
            ActGrpTypeID = 311) %>%# I need to understand what the other options are  
            rename(ActGrpComment = Date4group) %>% # what is the comment? 
            select(ActGrpIDText,ActGrpComment,ActGrpTypeID)

# write to the database defined by the connection 
# creates a temporary table 
sqlSave(VM2T.sql, ActGroup, tablename = "TempActGrp", append = FALSE, rownames = FALSE, colnames = FALSE, safer = TRUE)

#SQL intset query
qryAG <- "INSERT INTO t_ActGrp (ActGrpIDText, ActGrpComment, ActGrpTypeID)
SELECT ActGrpIDText, ActGrpComment, ActGrpTypeID FROM TempActGrp;"

# run the SQL query 
sqlQuery(VM2T.sql, qryAG, max = 0, buffsize = length(ActGroup$ActGrpIDText))#2Act$ActGrpID))

# delete temp table 
sqlDrop(VM2T.sql, "TempActGrp")

#### t_Activity ####
act <- res %>% 
       select(act_id,act_type,subid,LASAR,DateTime,SubOrganizationID,SampleDepth,
              SampleDepthUnit,Org_Comment,DEQ_Comment,samplers) %>%
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
              ActivityOrgID = SubOrganizationID,SmplDepth =SampleDepth, Samplers = samplers)

# write to the database defined by the connection 
# creates a temporary table 
sqlSave(VM2T.sql, act, tablename = "TempAct", append = FALSE, rownames = FALSE, colnames = FALSE, safer = TRUE)

#SQL intset query
qryA <- "INSERT INTO t_Activity (ActivityIDText,ActivityTypeId,SubID,SiteID,SiteContextID,SiteDescription,StartDateTime,
                                  StartDateTimeZoneID,EndDateTime,EndDateTimeZoneID,MediaID,ActivityOrgID,SmplColMthdID,      
                                  SmplColEquip,SmplEquipID,SmplColEquipComment,SmplDepth,SmplDepthUnitID,Org_Comment,
                                  DEQ_Comment,Samplers)
          SELECT * FROM TempAct;"

# run the SQL query 
sqlQuery(VM2T.sql, qryAG, max = 0, buffsize = length(ActGroup$ActGrpIDText))

# delete temp table 
sqlDrop(VM2T.sql, "TempAct")
