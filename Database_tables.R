## L. Merrick - 3/29/2021
##generates VolMon database tables 

DB_tables <- function(final_DQL) {
# pulls in database tables 
VM2.sql <- odbcConnect("VolMon2") ### this requires an ODBC conection to the VOLMON2 on DEQLEAD-LIMS/dev
stations <- sqlFetch(VM2.sql, "dbo.tlu_Stations") 
units <- sqlFetch(VM2.sql, "dbo.tlu_units")
context <- sqlFetch(VM2.sql, "dbo.tlu_Context")
method <- sqlFetch(VM2.sql, "dbo.tlu_Method")
chars <- sqlFetch(VM2.sql,"dbo.tlu_Characteristic")
type <- sqlFetch(VM2.sql,"dbo.tlu_Type") %>% select(TypeID,TypeIDText) 

#### t_ActGrp ####
ActGroup <- final_DQL %>%
  select(act_group,Date4group) %>% 
  distinct() %>% 
  mutate(ActGrpIDText = act_group,  
         ActGrpTypeID = 311) %>%# I need to understand what the other options are  
  rename(ActGrpComment = Date4group) %>% # what is the comment? 
  select(ActGrpIDText,ActGrpComment,ActGrpTypeID)
# 
.GlobalEnv$ActGroup <- ActGroup
#write.csv(ActGroup, "ActGroup.csv")

#### t_Activity ####
act <- final_DQL %>% 
  select(act_id,act_type,subid,LASAR_ID,DateTime,SubOrganizationID,
         samplers,SampleDepth,SampleDepthUnit,DEQ_Comment,Org_Comment) %>% 
  left_join(stations, by = c('LASAR_ID' = 'STATION_KEY')) %>%
  left_join(context, by = c('ContextID'= 'CntextIdText')) %>% # what is this for? 
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

.GlobalEnv$act <- act
#write.csv(act, "act.csv")

#### t_Result ####
res_t <- final_DQL  %>% 
  select(result_id,act_id,LASAR_ID,CharIDText,Result,RsltQual,'Method Short Name','MethodSpeciation',UNITS,
         LOQ,FieldOrLab,Res_comment,OG_ACC,OG_PREC,OG_DQL,final_DQL) %>%
  left_join(chars, by = 'CharIDText') %>%
  left_join(units, by = c('UNITS' = 'UnitIdText')) %>%
  left_join(method, by = c('Method Short Name' = 'ShortName')) %>%
  #left_join(QC_act, by = c('act_id' = 'ActivityIDText')) %>%  # this reduces having to pull in the whole activity table 
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
  select(result_id,act_id,CharID,Result,RsltQual,UnitID,MethodID,MethodSpeciation,RsltTypeID, 
         AnalyticalLaboratoryID,AnalyticalStartTime,AnalyticalStartTimeZoneID,AnalyticalEndTime,
         AnalyticalEndTimeZoneID,LabCommentCode,LOQ,LOQ_UnitID,OG_ACC,OG_PREC,OG_DQL,DEQ_ACC,prec_DQL,
         BiasValue, prec_val,final_DQL,StatisticalBasisID,RsltTimeBasisID,StoretUniqueID,Res_comment,
         DEQ_RsltComment,RsltStatusID) %>%
  rename(ResultIDText = result_id,PrecisionValue = prec_val,DEQ_PREC = prec_DQL,ORDEQ_DQL = final_DQL,
         Org_RsltComment = Res_comment, StoretQualID = StoretUniqueID,QCorigACC = OG_ACC, QCorigPREC = OG_PREC,
         QCorigDQL = OG_DQL)

.GlobalEnv$res_t <- res_t
#write.csv(res_t, "res_t.csv")

####t_anom ####
anom_t <- anom %>% 
  left_join(type, by = c('Anom' = 'TypeIDText')) %>% 
  select(result_id,TypeID) %>% 
  #left_join(QC_Res, by = c('result_id' = 'ResultIDText')) %>%
  mutate(AnomalyComment = NA) %>%
  select(result_id,AnomalyComment,TypeID) %>% 
  rename(AnomalyTypeID = TypeID) 
}



