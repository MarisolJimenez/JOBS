library(RJDBC)
library(RODBC)
library(dplyr)
library(sqldf)
library(xlsx)
library(tidyr)
library(fitdistrplus)
library(data.table)
rm(list = ls())

#########INSUMOS#############
#####RUTA DE TRABAJO
#gmessage("Seleccione la ruta donde desea que se guarde el archivo",title = "Info",icon = "info")
ruta <- "C:\\Users\\jima9001\\Desktop\\NIELSEN\\CAM PROD"
periodos <- read.csv("C:/Users/jima9001/Desktop/NIELSEN/CAM/Shop_sample_periodos.csv")

#sample <- 1
#industry <- 2
periodo_inicio <- 2017025
#periodo_fin <- 2019050

setwd(ruta)

##SMS
servidor_sms<- "acn052cpsms01"        #Servidor SMS
sms_retail<- "smsscancam"                   #Base de Datos Current del SMS RETAIL
sms_retail_hist<- "smsscancamHistory"          #Base de Datos History del SMS RETAIL
sms_scan<- "smsscancam"                   #BASE DE DATOS SMS SCAN CURRENT
sms_scan_history="smsscancamHistory"      #BASE DE DATOS SMS SCAN HISTORY
user<- "usr_read"             #Usuario
pass<- "LEeras31"            #Password

connection_sms<-odbcDriverConnect(paste0("driver={SQL Server};Server=",servidor_sms,";uid=",user,";pwd=",pass))

##TIENDA, ACV, PERIODO
INDEX_PERIOD_SOURCE <- sqlQuery(connection_sms,
                                paste0("SELECT DISTINCT source_id, period_id, source_acv 
                                       FROM ",servidor_sms,".",sms_scan_history,".dbo.index_period_source ",
                                       "WHERE period_id >= ",periodo_inicio,
                                       "AND index_id = 1
                                       AND status_id >= 6"))

#CADENA DE CADA TIENDA
SOURCE_MASTER <- sqlQuery(connection_sms,
                          paste0("SELECT *
                                  FROM ",servidor_sms,".",sms_scan,".dbo.source_master ",
                                 "WHERE cadena_de_tiendas <> 0"))


INDEX_PERIOD_SOURCE_5484 <- INDEX_PERIOD_SOURCE %>% left_join(SOURCE_MASTER, by = "source_id")
#INDEX_PERIOD_SOURCE_5484
index_id<-c(291,292,290,293,288,289)

for (i in 1:6) {
  
  if (i==1) {sample_i<-291}
  if (i==2) {sample_i<-292}
  if (i==3) {sample_i<-290}
  if (i==4) {sample_i<-293}
  if (i==5) {sample_i<-288}
  if (i==6) {sample_i<-289}
  
  if (i==1) {ind_i<-index_id[1]}
  if (i==2) {ind_i<-index_id[2]}
  if (i==3) {ind_i<-index_id[3]}
  if (i==4) {ind_i<-index_id[4]}
  if (i==5) {ind_i<-index_id[5]}
  if (i==6) {ind_i<-index_id[6]}

INDEX_PERIOD_SOURCE_CADENA <- INDEX_PERIOD_SOURCE_5484 %>% filter(pais==i)
ACV_CADENA_PERIODO <- sqldf("SELECT period_id, cadena_de_tiendas, 
                                    COUNT(source_id) AS N, SUM(source_acv) AS ACV
                             FROM INDEX_PERIOD_SOURCE_CADENA
                             GROUP BY period_id, cadena_de_tiendas")

ACV_CADENA_PERIODO_1 <- ACV_CADENA_PERIODO %>% left_join(periodos, by = c("period_id" = "LEGACY_SCANTRACK"))

CELL_INDUSTRY <- ACV_CADENA_PERIODO_1 %>% mutate(SAM_ID = sample_i,
                                                 IND_ID = ind_i,
                                                 CEL_ID = cadena_de_tiendas,
                                                 CEL_TYPE = "CE",
                                                 CEL_NUMBER_OF_SHOPS = N,
                                                 CEL_ACV_TURNOVER = ACV,
                                                 CEL_TPR_DESCRIPTION = "",
                                                 WEEK_EFFECTIVE_FROM = NRSP_DESC,
                                                 WEEK_EFFECTIVE_TO = NRSP_DESC)
FICHERO_CELL_INDUSTRY <- sqldf("SELECT SAM_ID, IND_ID, CEL_ID, CEL_TYPE, CEL_NUMBER_OF_SHOPS, CEL_ACV_TURNOVER, 
                               CEL_TPR_DESCRIPTION, WEEK_EFFECTIVE_FROM, WEEK_EFFECTIVE_TO
                               FROM CELL_INDUSTRY")
fwrite(FICHERO_CELL_INDUSTRY, paste0("CELL_INDUSTRY_missingCR",i,".csv"), na = '', row.names = F, col.names = T, quote = F)
}
