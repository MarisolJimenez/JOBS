library(RJDBC)
library(RODBC)
library(dplyr)
library(sqldf)
library(xlsx)
library(tidyr)
library(fitdistrplus)
library(data.table)

#gmessage("Seleccione la ruta donde desea que se guarde el archivo",title = "Info",icon = "info")
ruta <- "C:\\Users\\jima9001\\Desktop\\NIELSEN\\CAM\\1059-1066"

cadenas <- read.csv("C:/Users/jima9001/Desktop/NIELSEN/CAM/1059-1066/FICHEROS/CELL/Cel_rule_cadenas_from sms.csv")

chr_id <- "P1006151"
#sample <- 990852
periodo_inicio <- 2017025
periodo_fin <- 2019045


setwd(ruta)

##SMS
servidor_sms<- "acn052cpsms01"        #Servidor SMS
sms_retail<- "smsscancam"                   #Base de Datos Current del SMS RETAIL
sms_retail_hist<- "smsscancamHistory"          #Base de Datos History del SMS RETAIL
sms_scan<- "smsscancam"                   #BASE DE DATOS SMS SCAN CURRENT
sms_scan_history="smsscancamHistory"      #BASE DE DATOS SMS SCAN HISTORY
user<- "usr_read"             #Usuario
pass<- "LEeras31"             #Password
connection_sms <- odbcDriverConnect(paste0("driver={SQL Server};Server=",servidor_sms,";uid=",user,";pwd=",pass))

#MASTER

INDEX_PERIOD_SOURCE <- sqlQuery(connection_sms,
                                paste0("SELECT DISTINCT source_id
                                       FROM ",servidor_sms,".",sms_scan_history,".dbo.index_period_source ",
                                       "WHERE period_id >= ",periodo_inicio,
                                       "AND period_id <= ",periodo_fin,
                                       "AND index_id = 1
                                       AND status_id >= 6"))
SOURCE_MASTER <- sqlQuery(connection_sms,
                          paste0("SELECT DISTINCT source_id,cadena_de_tiendas,pais
                                      FROM ",servidor_sms,".",sms_scan,".dbo.source_master "))

TIENDAS <- INDEX_PERIOD_SOURCE %>% left_join(SOURCE_MASTER, by = "source_id")

SOURCE_MASTER_1 <- TIENDAS %>% filter(cadena_de_tiendas != 0)
#SOURCE_MASTER_1 <- SOURCE_MASTER %>% filter(cadena_de_tiendas != 0)
SOURCE_MASTER_2 <- SOURCE_MASTER_1 %>% left_join(cadenas, by = "cadena_de_tiendas")


for (i in 1:6) {
SOURCE_MASTER_3 <- SOURCE_MASTER_2 %>% filter(pais==i)

if (i==1) {sample_i<-291}
if (i==2) {sample_i<-292}
if (i==3) {sample_i<-290}
if (i==4) {sample_i<-293}
if (i==5) {sample_i<-288}
if (i==6) {sample_i<-289}

FICHERO_CEL_RULE <- SOURCE_MASTER_3 %>% mutate(SAM_ID = sample_i,
                                               CEL_ID = "",
                                               CEL_TYPE = "CE",
                                               CEL_DESCRIPTION = celda,
                                               FROM_WEEK = "W 2017 01",
                                               TO_WEEK = "",
                                               CEL_MIN_NUMBER_OF_SHOP = 1,
                                               CEL_WORKING = 0,
                                               CEL_BUILT_BY_UNIV_ADDR = 0,
                                               CEL_OPTIMAL_SIZE = "",
                                               CEL_RULE = paste0(chr_id," = '", chv_value,"'"))
FICHERO_CEL_RULE_2 <- sqldf("SELECT SAM_ID, CEL_ID, CEL_TYPE, CEL_DESCRIPTION, FROM_WEEK, TO_WEEK,
                                    CEL_MIN_NUMBER_OF_SHOP, CEL_WORKING, CEL_BUILT_BY_UNIV_ADDR,
                                    CEL_OPTIMAL_SIZE, CEL_RULE
                             FROM FICHERO_CEL_RULE")

FICHERO_CEL_RULE_2 <- FICHERO_CEL_RULE_2[!duplicated(FICHERO_CEL_RULE_2), ]

fwrite(FICHERO_CEL_RULE_2, paste0("06_SCANTRACK_CELL_RULE_CAM_",i,".csv"), na = '', row.names = F, quote = F)
}

