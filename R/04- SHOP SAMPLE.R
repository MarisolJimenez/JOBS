########SCRIPT PARA LOS FICHEROS DE MASSIVE UPLOAD EN MADRAS#########
#######FICHERO_SHOP_SAMPLE_POC##############
########PREAMBULO########
###SÍ TU JAVA NO JALA, PRUEBA EL SIG CODIGO: 
rm(list=ls())

Sys.setenv(JAVA_HOME='C:/Program Files/Java/jre1.8.0_171')
if (!require(data.table)) {install.packages("data.table")}
if (!require(fitdistrplus)) {install.packages("fitdistrplus")}
if (!require(tidyr)) {install.packages("tidyr")}
if (!require(xlsx)) {install.packages("xlsx")}
if (!require(sqldf)) {install.packages("sqldf")}
if (!require(dplyr)) {install.packages("dplyr")}
if (!require(RODBC)) {install.packages("RODBC")}
if (!require(RJDBC)) {install.packages("RJDBC")}
if (!require(rJava)) {install.packages("rJava")}

if (!require(DBI)) {install.packages("DBI")}

library(RJDBC)
library(RODBC)
library(dplyr)
library(sqldf)
library(xlsx)
library(tidyr)
library(fitdistrplus)
library(data.table)
library(gWidgets)
library(gWidgets2)
library(gWidgets2RGtk2)

#gmessage("Seleccione la ruta donde desea que se guarde el archivo",title = "Info",icon = "info")
ruta <- choose.dir() 
periodos <- read.csv("C:/Users/jima9001/Desktop/NIELSEN/CAM/Shop_sample_periodos.csv")


#sample <- 1

country_channel <- "XCSCAN"
periodo_inicio <- 2019064
periodo_fin <-2019067

setwd(ruta)

###SMS

servidor_sms<- "acn052cpsms01"        #Servidor SMS
sms_retail<- "smsscancam"                   #Base de Datos Current del SMS RETAIL
sms_retail_hist<- "smsscancamHistory"          #Base de Datos History del SMS RETAIL

sms_scan<- "smsscancam"                   #BASE DE DATOS SMS SCAN CURRENT
sms_scan_history="smsscancamHistory"      #BASE DE DATOS SMS SCAN HISTORY

user<- "usr_read"             #Usuario
pass<- "LEeras31"            #Password

connection_sms<-odbcDriverConnect(paste0("driver={SQL Server};Server=",servidor_sms,";uid=",user,";pwd=",pass))

#############INDEX_PERIOD_SOURCE CON TIENDAS ACTIVAS##############
INDEX_PERIOD_SOURCE <- sqlQuery(connection_sms,
                                paste0("SELECT DISTINCT source_id, period_id 
                                      FROM ",servidor_sms,".",sms_scan_history,".dbo.index_period_source ",
                                       "WHERE period_id >= ",periodo_inicio,
                                       "AND period_id <= ",periodo_fin,
                                       "AND index_id = 1
                                      AND status_id >= 6"))

SMS_MASTER_SCAN <- sqlQuery(connection_sms,
                            paste0("SELECT *
                                   FROM ",servidor_sms,".",sms_scan,".dbo.source_master "))

TIENDAS <- INDEX_PERIOD_SOURCE %>% left_join(SMS_MASTER_SCAN, by = "source_id")

INDEX_PERIOD_SOURCE_1 <- TIENDAS %>% left_join(periodos, by = c("period_id" = "LEGACY_SCANTRACK"))
index_id<-c(291,292,290,293,288,289) #also sample id in this particular case


for (i in 1:6) {
  
  if (i==1) {sample_i<-index_id[1]}
  if (i==2) {sample_i<-index_id[2]}
  if (i==3) {sample_i<-index_id[3]}
  if (i==4) {sample_i<-index_id[4]}
  if (i==5) {sample_i<-index_id[5]}
  if (i==6) {sample_i<-index_id[6]}
  
  INDEX_PERIOD_SOURCE_5484<- INDEX_PERIOD_SOURCE_1 %>% filter(pais==i)
  INDEX_PERIOD_SOURCE_2 <- sqldf("SELECT source_id, period_id, NRSP_DESC
                                FROM INDEX_PERIOD_SOURCE_5484")
  FICHERO_SHOP_SAMPLE <- INDEX_PERIOD_SOURCE_2 %>% mutate(ACTION = "",
                                                          SSW_SAM_ID = sample_i,
                                                          SSW_CCH_ID = country_channel,
                                                          SHO_EXTERNAL_CODE = source_id + 5700000000,
                                                          TPR_SHORT_DESCRIPTION = NRSP_DESC)
  FICHERO_SHOP_SAMPLE_1 <- sqldf("SELECT ACTION, SSW_SAM_ID, SSW_CCH_ID, SHO_EXTERNAL_CODE, TPR_SHORT_DESCRIPTION
                               FROM FICHERO_SHOP_SAMPLE")
  FICHERO_SHOP_SAMPLE_1 %>% fwrite(paste0("04_SCANTRACK_SHOP_SAMPLE_CAM_",i,".csv"))
}

