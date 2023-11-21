########SCRIPT PARA LOS FICHEROS DE MASSIVE UPLOAD EN MADRAS#########
#############FICHERO DE NEW_STORES POC###################
########PREAMBULO########
########
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

##EXPANSION DE CAPACIDAD DE R (SI ES DE 64 BITS, PUEDE SER HASTA 9000)
#memory.limit(9000)
memory.limit(NA)

##INSUMOS##
#####RUTA DE TRABAJO
#gmessage("Seleccione la ruta donde desea que se guarde el archivo",title = "Info",icon = "info")


periodo_inicio <- 2019063
#periodo_fin <- 2019050
##CARPETAS DE SALIDA##
###ruta <- choose.dir()
ruta <- ("C:\\Users\\jima9001\\Desktop\\NIELSEN\\CAM PROD\\SALIDAS")
setwd(ruta)

##CONEXIONES A SMS##
servidor_sms<- "acn052cpsms01"        #Servidor SMS
sms_scan<- "smsscancam"                   #BASE DE DATOS SMS SCAN CURRENT
sms_scan_history="smsscancamHistory"      #BASE DE DATOS SMS SCAN HISTORY
user<- "usr_read"             #Usuario
pass<- "LEeras31"            #Password
connection_sms <- odbcDriverConnect(paste0("driver={SQL Server};Server=",servidor_sms,";uid=",user,";pwd=",pass))

##NEW STORES##  
###SOURCE MASTER
SMS_MASTER_SCAN <- sqlQuery(connection_sms,
                            paste0("SELECT *
                                   FROM ",servidor_sms,".",sms_scan,".dbo.source_master "))
#############INDEX_PERIOD_SOURCE CON TIENDAS ACTIVAS##############
####paste0 es un concatenate como en excel 
#Con Fecha límite
# INDEX_PERIOD_SOURCE <- sqlQuery(connection_sms,
#                                 paste0("SELECT *
#                                       FROM ",servidor_sms,".",sms_scan_history,".dbo.index_period_source ",
#                                        "WHERE period_id >= ",periodo_inicio,
#                                        "AND period_id <= ",periodo_fin,
#                                        "AND index_id = 1
                                    #  AND status_id >= 6"))
INDEX_PERIOD_SOURCE <- sqlQuery(connection_sms,
                                paste0("SELECT *
                                      FROM ",servidor_sms,".",sms_scan_history,".dbo.index_period_source ",
                                       "WHERE period_id >= ",periodo_inicio,
                                       "AND index_id = 1
                                      AND source_id in (4889,4893,4904,4907,5537,4916,4918,4927,4943,4946,5044,5046,7067
)
                                      "))

##INFO DE TIENDAS ACTIVAS EN LOS PERIODOS REQUERIDOS
TIENDAS <- INDEX_PERIOD_SOURCE %>% left_join(SMS_MASTER_SCAN, by = "source_id")

### CONSTRUCCION DEL FICHERO
### el "gsub" es sustituir, "toupper" cambia a mayúsculaS

TIENDAS$CHANNEL_aux<-NA

for (i in 1:nrow(TIENDAS)) {

#GTSC GUATEMALA #2SALVADOR #3.-HONDURAS3 4.-NICARAGUA 5.-COSTA RICA 6.-PANAMA
if (TIENDAS$pais[i]==1) {TIENDAS$CHANNEL_aux[i] <-'GTSC'}
if (TIENDAS$pais[i]==2) {TIENDAS$CHANNEL_aux[i] <-'SVSC'}
if (TIENDAS$pais[i]==3) {TIENDAS$CHANNEL_aux[i]<-'HNSC'}
if (TIENDAS$pais[i]==4) {TIENDAS$CHANNEL_aux[i]<-'NISC'}
if (TIENDAS$pais[i]==5) {TIENDAS$CHANNEL_aux[i]<-'CRSC'}
if (TIENDAS$pais[i]==6) {TIENDAS$CHANNEL_aux[i]<-'PASC'}
}
  
# TIENDAS$address1[is.na(TIENDAS$address1)] <- "NN"
# TIENDAS$address1[TIENDAS$address1==0] <- "NN"
# TIENDAS$address1[TIENDAS$address1==""] <- "NN"
# TIENDAS$address1[TIENDAS$address1=="."] <- "NN"

VARIABLES_FICHERO <- TIENDAS %>% mutate(SHOP_EXTERNAL_CODE = source_id + 5700000000, 
                                      SHO_NAME = gsub("'","",gsub(',',"_",gsub('"',"",gsub("Â","A",gsub("Á","A",gsub("É","E",gsub("Í","I",gsub("Ó","O",gsub("Ú","U",gsub("Ü","U",gsub("Ñ","N",toupper(source_name)))))))))))),
                                      COUNTRY="XC",
                                      CHANNEL= CHANNEL_aux,
                                      TYPE="C",
                                      ADDRESS_1 = ifelse(address1 == "", "NN", ifelse (is.na(address1), "NN" , gsub("'","", gsub(',',"_", gsub('"',"", gsub("Â","A",gsub("Á","A",gsub("´","A", gsub("É","E",gsub("Í","I",gsub("Ó","O",gsub("Ú","U",gsub("Ü","U",gsub("Ñ","N",gsub("°","o",gsub("º","o",toupper(address1))))))))))))))))),
                                      ADDRESS_2 = ifelse(address2 == "", "NN", ifelse (is.na(address2), "NN" , gsub("'","", gsub(',',"_", gsub('"',"", gsub("Â","A",gsub("Á","A",gsub("´","A", gsub("É","E",gsub("Í","I",gsub("Ó","O",gsub("Ú","U",gsub("Ü","U",gsub("Ñ","N",gsub("°","o",gsub("º","o",toupper(address2))))))))))))))))),
                                      ADDRESS3 = " ",
                                      CITY=" ", 
                                      POSTAL=" ", 
                                      STAT=" ", 
                                      PROVINCE=" ", 
                                      COUNTY=" ", 
                                      ADDRESS4=" ")


#VARIABLES_FICHERO<-VARIABLES_FICHERO[-57]
VARIABLES_FICHERO <- sqldf("SELECT DISTINCT SHOP_EXTERNAL_CODE, SHO_NAME, COUNTRY, CHANNEL, TYPE, ADDRESS_1 AS ADDRESS1, ADDRESS_2 as ADDRESS2, ADDRESS3, ADDRESS4, CITY, POSTAL, STAT, PROVINCE, COUNTY
                           FROM VARIABLES_FICHERO")


fwrite(VARIABLES_FICHERO, "01_NEW_STORES_COLON_MUSMA.csv", na = '', sep = ";", row.names = F, quote = F, col.names = T)
shell.exec("01_NEW_STORES_COLON_MUSMA.csv")

