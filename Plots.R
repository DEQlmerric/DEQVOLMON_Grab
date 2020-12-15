### Generates Plots and data summaries 

Plots_QCSum <- function(final_DQL) {
##### generate summaries #### 
# Data summary 
CharSum <- final_DQL %>% 
           group_by(CharIDText) %>% 
           summarise(n = length(Result),
                     Mean = mean(Result),
                     #GeoMean = geometric.mean(r),
                     Min = min(Result),
                     Fifth = quantile(Result,probs=.05),
                     Tenth = quantile(Result,probs=.1),
                     Twentieth = quantile(Result,probs=.2),
                     TwentyFifth = quantile(Result,probs=.25),
                     Median = quantile(Result,probs=.5),
                     SeventyFifth =quantile(Result,probs=.75),
                     Eightieth = quantile(Result,probs=.8),
                     Ninetieth = quantile(Result,probs=.9),
                     NinetyFifth = quantile(Result,probs=.95),
                     Max = max(Result)) %>% 
          mutate(LASAR_ID = "All") %>%
          select(LASAR_ID,CharIDText,n,Mean,Min,Fifth,Tenth,Twentieth,
                 TwentyFifth,Median,SeventyFifth,Eightieth,Ninetieth,NinetyFifth,Max)

SampleSummary <- final_DQL %>% 
                 group_by(LASAR_ID,CharIDText) %>% 
                 summarise(n = length(Result),
                            Mean = mean(Result),
                            #GeoMean = geometric.mean(r),
                            Min = min(Result),
                            Fifth = quantile(Result,probs=.05),
                            Tenth = quantile(Result,probs=.1),
                            Twentieth = quantile(Result,probs=.2),
                            TwentyFifth = quantile(Result,probs=.25),
                            Median = quantile(Result,probs=.5),
                            SeventyFifth =quantile(Result,probs=.75),
                            Eightieth = quantile(Result,probs=.8),
                            Ninetieth = quantile(Result,probs=.9),
                            NinetyFifth = quantile(Result,probs=.95),
                            Max = max(Result))

DataSummary<-rbind(CharSum,SampleSummary) 
names(DataSummary) = c("Station", "Characteristic","Count", "Mean", "Min", "5th %", 
                       "10th %", "20th %","25th %", "Median", "75th %","80th %", "90th %", "95th %", "Max")
write.csv(DataSummary, file = paste0(sub_id,'_DataSummary.csv'))

# QC summary - hmmmm 
prec_grade_plot <- res %>% 
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
                              QCcalc == "AbsDiff" ~ (Result - lag(Result, default = first(Result))),  # using abs here may mess up the charts and statisitcs.
                              QCcalc == "RPD" ~ 
                                ((Result - lag(Result, default = first(Result)))/mean(Result))),
         use_DQLA = as.numeric(ifelse(QCcalc == 'AbsDiff' & DQLA < LOQ, LOQ ,DQLA)), # Change QC criteria when the abslolute difference criteria is less than the limit of quantitation 
         use_DQLB = as.numeric(ifelse(QCcalc == 'AbsDiff' & DQLB < LOQ, LOQ*2,DQLB)), # Change QC criteria when the abslolute difference criteria is less than the limit of quantitation
         prec_DQL = case_when(prec_val <= use_DQLA ~ "A", 
                              prec_val > use_DQLA & prec_val <= use_DQLB ~ "B",
                              TRUE ~ "C")) %>%
  filter(sample_type == 'dup') %>%
  mutate(use_DQLA_Low = use_DQLA*(-1),
         use_DQLB_Low = use_DQLB*(-1)) %>%
  select(row_ID,CharIDText,DateTime,QCcalc,prec_val,use_DQLA,use_DQLB,prec_DQL,use_DQLA_Low,use_DQLB_Low)


qcsum <- prec_grade_plot  %>%
  group_by(CharIDText,QCcalc) %>% 
  summarise(n = length(prec_val),
            Mean = mean(prec_val),
            Min = min(prec_val),
            Fifth = quantile(prec_val,probs=.05),
            Tenth = quantile(prec_val,probs=.1),
            TwentyFifth = quantile(prec_val,probs=.25),
            Median = quantile(prec_val,probs=.5),
            SeventyFifth =quantile(prec_val,probs=.75),
            Ninetieth = quantile(prec_val,probs=.9),
            NinetyFifth = quantile(prec_val,probs=.95),
            Max = max(prec_val),
            cntA = sum(prec_DQL=="A"),
            cntB = sum(prec_DQL=="B"),
            CE = length(prec_DQL)-((sum(prec_DQL=="A"))+(sum(prec_DQL=="B"))),
            sumofQC = n()) %>%
  mutate(pctA = (cntA / sumofQC),
         pctB = (cntB / sumofQC),
         pctC = (CE / sumofQC))
#rename columns
names(qcsum) = c("charid","QCcalc", "Count", "Mean","Min", "5th %", "10th %", "25th %", "Median", "75th %","90th %", "95th %", "Max",
                 "CountA", "CountB","CountCorE", "Count_QC","PercentA", "PercentB", "PercentCorE")

#Generate CSV table to circulate
write.csv(qcsum, file = paste0(sub_id,'_QCSummary.csv'))


##### PLOTS ##########       
#box plot 
##### Box Plots by Parameter ####
box_plots <- final_DQL %>% 
  select(row_ID,LASAR_ID,DateTime,Result,sample_type,CharIDText,act_group,actgrp_char) 
box_plots$LASAR_ID <- as.factor(box_plots$LASAR_ID)

# all in one
p <- 
  ggplot(box_plots, aes(x = LASAR_ID, y = Result)) + 
  geom_boxplot() + theme(axis.text.x = element_text(angle = 90)) +
  facet_grid(rows = vars(CharIDText),scales = "free") +
  labs(x = "Station", y = "Result Value") 

ggsave(paste0(sub_id,'BoxPlot_AllChar.png'))

# individual plots per char 
for (var in unique(box_plots$CharIDText)) {
  #dev.new()
  ggplot(box_plots[box_plots$CharIDText==var,], aes(x = LASAR_ID, y = Result)) +
  geom_boxplot() + theme(axis.text.x = element_text(angle = 90)) +
  ggtitle(var)  + labs(x = "Station", y = "Result Value ")
  ggsave(filename = paste(sub_id,var,'box_plot.png'))
}

#### duplicate Comparison by Parameter - Time Series Charts of Duplicate Differences (prec_value#####
# all in one 
d <- ggplot(data=prec_grade_plot, aes(x = DateTime, y = prec_val)) + geom_point() +
     geom_line(data = prec_grade_plot,aes(y = use_DQLA), color = "red") + 
     geom_line(data = prec_grade_plot,aes(y = use_DQLB), color = "blue") + 
     geom_line(data = prec_grade_plot,aes(y = use_DQLA_Low), color = "red") + 
     geom_line(data = prec_grade_plot,aes(y = use_DQLB_Low), color = "blue") + 
     facet_grid(rows = vars(CharIDText),scales = "free") +
     labs(x = "Sample Date", y = "Precision Value") +
     theme(legend.position="none") 

ggsave(paste0(sub_id,'PrecisionValue_AllChar.png'))

# individual plots per char 
for (var in unique(box_plots$CharIDText)) {
  #dev.new()
  ggplot(data=prec_grade_plot, aes(x = DateTime, y = prec_val)) + geom_point() +
    geom_line(data = prec_grade_plot,aes(y = use_DQLA), color = "red") + 
    geom_line(data = prec_grade_plot,aes(y = use_DQLB), color = "blue") + 
    geom_line(data = prec_grade_plot,aes(y = use_DQLA_Low), color = "red") + 
    geom_line(data = prec_grade_plot,aes(y = use_DQLB_Low), color = "blue") + 
    labs(x = "Sample Date", y = "Precision Value") +
    theme(legend.position="none") 
  ggsave(filename = paste(sub_id,var,'PrecisionValue.png'))
}

#### results primary vs. dup ####
QC_plots <- final_DQL %>% 
            group_by(row_ID,CharIDText) %>% 
            mutate(n = n(),
                  QC = ifelse(n() > 1, 1, 0)) %>%
            ungroup() %>% 
            filter(QC == 1) %>%#  pulls out primary and dups pairs 
            left_join(prec_grade_plot, by = c('row_ID','CharIDText','DateTime')) %>%
            select(row_ID,LASAR_ID,DateTime,Result,sample_type,CharIDText,act_group,actgrp_char,
                       use_DQLA,use_DQLB) %>% 
               spread(sample_type, Result) %>% 
               mutate(A_QC_U = sample + use_DQLA,
                      A_QC_L = sample - use_DQLA,
                      B_QC_U = sample + use_DQLB,
                      B_QC_L = sample - use_DQLB) 

write.csv(QC_plots, file = paste0(sub_id,'_QCplots.csv'))

for (var in unique(QC_plots$CharIDText)) {
  #dev.new()
  ggplot(QC_plots[QC_plots$CharIDText==var,], aes(x = sample, y = dup)) + geom_point() + 
           ggtitle(var) + geom_line(aes(x = sample, y= A_QC_L), color = "orangered1", linetype = "dotted",size = 1) +
           geom_line(aes(x = sample, y= A_QC_U), color = "orangered1", linetype = "dotted", size = 1) +
           geom_line(aes(x = sample, y= B_QC_U), color = "darkblue", linetype = "dotted",size = 1)+ 
           geom_line(aes(x = sample, y= B_QC_L), color = "darkblue", linetype = "dotted",size = 1) +
           labs(x = "Field Primary", y = "Field Duplicate") +
  ggsave(filename = paste(sub_id,var,'FPvFD.png'))
}

}