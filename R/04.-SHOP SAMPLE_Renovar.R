########SCRIPT PARA LOS FICHEROS DE MASSIVE UPLOAD EN MADRAS#########
rm(list=ls())
options(guiToolkit = "RGtk2")

Sys.setenv(JAVA_HOME='C:/Program Files/Java/jre1.8.0_191/')
#---------------------------------------------FICHERO_SHOP_SAMPLE--------------------------------------------------------------------
########PREAMBULO########
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

library(rJava)
library(RJDBC)
library(RODBC)
library(dplyr)
library(sqldf)
library(xlsx)
library(tidyr)
library(fitdistrplus)
library(data.table)

######EXPANSION DE CAPACIDAD DE R (SI ES DE 64 BITS, PUEDE SER HASTA 9000)
#memory.limit(9000)
memory.limit(20000)

#########INSUMOS#############
#####RUTA DE TRABAJO
ruta <- "C:/Users/jima9001/Desktop/NIELSEN/CHILE PROD/SALIDAS/ACT DIC" 
insumos <- "INSUMOS/"
indice <- 14
sample <- 20000744
industry <- 20000736
indice_escrito <- 'INPA'
periodo_inicio <- 2017015
#periodo_previo <- 2018024
periodo_fin <- 2019018

########CARPETAS DE SALIDA#########
setwd(ruta)
dir.create(salidas,showWarnings = TRUE,recursive = FALSE, mode = "077")

########CONEXIONES A SMS#############
servidor_sms<- "ACN057BOGSMS01"        #Servidor SMS
sms_retail<- "Smsretail"                   #Base de Datos Current del SMS RETAIL
sms_retail_hist<- "smsretailhistory"          #Base de Datos History del SMS RETAIL

sms_scan<- "smsscan"                   #BASE DE DATOS SMS SCAN CURRENT
sms_scan_history="SMSScanHistory"      #BASE DE DATOS SMS SCAN HISTORY

user<- "usr_read"             #Usuario
pass<- "LEeras31"            #Password

connection_sms<-odbcDriverConnect(paste0("driver={SQL Server};Server=",servidor_sms,";uid=",user,";pwd=",pass))

################################CONEXIONES_MADRAS##################################

drv <- JDBC(driverClass = "oracle.jdbc.OracleDriver"
            , classPath = "C:/JAVA/ojdbc8.jar"       
            , identifier.quote = "'")


#####MADRAS#####
conn_m <- dbConnect(drv=drv
                    , url = "jdbc:oracle:thin:@oravalprdscan.enterprisenet.org:1535/VALPRDMDR"
                    , user = "NRSP_GUEST"
                    , password = "NRSP_GUEST")


######MADRAS_POC######
conn_m_poc <- dbConnect(drv=drv
                        , url = "jdbc:oracle:thin:@daylvalq01:1521/VALPOCNA"
                        , user = "MADRAS_GUEST"
                        , password = "MADRAS_GUEST")

#####MADRAS_PRODUCCION COLIMBIA######
conn_m_prod <- dbConnect(drv=drv
                         , url = "jdbc:oracle:thin:@dayrhevalplc1:1521/VALPLC01"
                         , user = "MADRAS_GUEST"
                         , password = "MADRAS_GUEST")

####MASTER

SMS_MASTER_RETAIL <- SMS_MASTER_RETAIL <- sqlQuery(connection_sms,
                                                   paste0("SELECT *
                                                          FROM ",servidor_sms,".",sms_retail,".dbo.source_master "))

#############INDEX_PERIOD_SOURCE CON TIENDAS ACTIVAS------------------------------------------------------------

MASTER <- sqldf("SELECT DISTINCT source_id, scan_source_id, scan_audit
                FROM SMS_MASTER_RETAIL")

###INDEX_RETAIL

INDEX_PERIOD_SOURCE_0_H <- sqlQuery(connection_sms,
                                    paste0("SELECT * 
                                           FROM ",servidor_sms,".",sms_retail_hist,".dbo.index_period_source ",
                                          "WHERE period_id >= ",periodo_inicio,
                                          "AND period_id <= ",periodo_previo,
                                          "AND index_id = ",indice,"
                                           AND status_id > 5
                                           AND sot_source = 0"))

INDEX_PERIOD_SOURCE_0_H <- INDEX_PERIOD_SOURCE_0_H %>% left_join(MASTER, by = c("source_id"))

#current en caso de pinturas no
INDEX_PERIOD_SOURCE_0_C <- sqlQuery(connection_sms,
                                  paste0("SELECT * 
                                          FROM ",servidor_sms,".",sms_retail,".dbo.index_period_source ",
                                         "WHERE period_id > ",periodo_previo,
                                         "AND period_id <= ",periodo_fin,
                                         "AND index_id = ",indice,"
                                          AND status_id > 5
                                          AND sot_source = 0"))

INDEX_PERIOD_SOURCE_0_C <- INDEX_PERIOD_SOURCE_0_C %>% left_join(MASTER, by = c("source_id"))

#juntar current & history
INDEX_PERIOD_SOURCE_2 <- rbind(INDEX_PERIOD_SOURCE_0_H, INDEX_PERIOD_SOURCE_0_C)

PERIODS <- read.csv(paste0(insumos, "Periodos_Retail_Mejorados.csv"))

INDEX_PERIOD_SOURCE_2 <- INDEX_PERIOD_SOURCE_2 %>% left_join(PERIODS, by = c("period_id"="MONTH_RETAIL"))

INDEX_PERIOD_SOURCE_2 <- sqldf("SELECT DISTINCT period_id, NRSP_WEEK, source_id, source_acv, area_ventas
                               FROM INDEX_PERIOD_SOURCE_2")

fwrite(INDEX_PERIOD_SOURCE_2, paste0(salidas,"COMPARACION_SHOP_SAMPLE_",periodo_fin,".csv"))

#RT1 <- INDEX_PERIOD_SOURCE_2 %>% filter(source_id %in% c(96309,96351))
###--------------------------------------------------------------FICHERO_SHOP_SAMPLE-------------------------------------------------------------------------------------------------

SHOP_SAMPLE <- INDEX_PERIOD_SOURCE_2 %>% mutate(ACTION = "INSERT",
                                                SSW_SAM_ID = sample,
                                                SSW_CCH_ID = "COMONT",
                                                SHO_EXTERNAL_CODE = source_id + 9310000000,
                                                TPR_SHORT_DESCRIPTION = NRSP_WEEK)


FICHERO_SHOP_SAMPLE <- sqldf("SELECT ACTION, SSW_SAM_ID, SSW_CCH_ID, SHO_EXTERNAL_CODE, TPR_SHORT_DESCRIPTION
                             FROM SHOP_SAMPLE")

fwrite(FICHERO_SHOP_SAMPLE, paste0(salidas,"FICHERO_SHOP_SAMPLE_FULL_",periodo_fin,"_",indice,".csv"), na = '', sep = ";", row.names = F,  quote = F)

#DIRECTO DE SMS
FICHERO_SHOP_SAMPLE <- FICHERO_SHOP_SAMPLE %>% mutate(FLAG_NEW = 1)
summary(FICHERO_SHOP_SAMPLE)

#####################################SHOP_SAMPLE_CARGADA_EN_MADRAS#############################

QUERY_SSAMPLE <- paste0("SELECT A.*, B.SHO_EXTERNAL_CODE
                        FROM MADRAS_DATA.TMXP_SHOP_SAMPLE A
                        LEFT JOIN MADRAS_DATA.TRSH_SHOP B
                        ON SSW_SHO_ID = SHO_ID
                        WHERE SSW_CCH_ID = 'COMONT'
                        AND SSW_SAM_ID = ",sample,"")

SHOP_SAMPLE_CERVEZAS <- dbGetQuery(conn=conn_m_prod,statement=paste0(QUERY_SSAMPLE))

SHOP_SAMPLE_CERVEZAS <- SHOP_SAMPLE_CERVEZAS %>% left_join(PERIODS, by = c("SSW_TPR_ID"="TPR_ID"))
SHOP_SAMPLE_CERVEZAS <- SHOP_SAMPLE_CERVEZAS %>% mutate(ACTION = "INSERT")

SHOP_SAMPLE_CERVEZAS <- SHOP_SAMPLE_CERVEZAS[-10]
names(SHOP_SAMPLE_CERVEZAS) <- c("SSW_SAM_ID","SSW_SHO_ID","SSW_TPR_ID","SSW_CCH_ID","SHO_EXTERNAL_CODE","MONTH_RETAIL","NRSP_WEEK","A?O","SEMANA", "ACTION")

ACTUAL_SHOP_SAMPLE <- sqldf("SELECT ACTION, SSW_SAM_ID, SSW_CCH_ID, SHO_EXTERNAL_CODE, NRSP_WEEK AS TPR_SHORT_DESCRIPTION
                            FROM SHOP_SAMPLE_CERVEZAS")

ACTUAL_SHOP_SAMPLE <- ACTUAL_SHOP_SAMPLE %>% mutate(FLAG_ACTUAL = 1)

summary(ACTUAL_SHOP_SAMPLE)
ACTUAL_SHOP_SAMPLE$SHO_EXTERNAL_CODE <- as.numeric(ACTUAL_SHOP_SAMPLE$SHO_EXTERNAL_CODE)

COMPARACION <- ACTUAL_SHOP_SAMPLE %>% full_join(FICHERO_SHOP_SAMPLE, by = c("ACTION", "SSW_SAM_ID", "SSW_CCH_ID", "SHO_EXTERNAL_CODE", "TPR_SHORT_DESCRIPTION"))

################CAMBIOS#####################
########A?ADIR TIENDAS DE SHOP_SAMPLE#######
FICHERO_ADD_SHOP_SAMPLE <- COMPARACION %>% filter(is.na(FLAG_ACTUAL) & FLAG_NEW == 1) 
FICHERO_ADD_SHOP_SAMPLE <- FICHERO_ADD_SHOP_SAMPLE[c(-6,-7)]
FICHERO_ADD_SHOP_SAMPLE <- FICHERO_ADD_SHOP_SAMPLE %>% mutate(ACTION = "INSERT")

########QUITAR TIENDAS DE SHOP_SAMPLE#######
FICHERO_DELETE_SHOP_SAMPLE <- COMPARACION %>% filter(is.na(FLAG_NEW) & FLAG_ACTUAL == 1) 
FICHERO_DELETE_SHOP_SAMPLE <- FICHERO_DELETE_SHOP_SAMPLE[c(-6,-7)]
FICHERO_DELETE_SHOP_SAMPLE <- FICHERO_DELETE_SHOP_SAMPLE %>% mutate(ACTION = "DELETE")

########ESCRIBIR EL FICHERO ###########
FICHERO_SHOP_SAMPLE_CAMBIOS <- rbind(FICHERO_ADD_SHOP_SAMPLE, FICHERO_DELETE_SHOP_SAMPLE)
FICHERO_SHOP_SAMPLE_CAMBIOS <- FICHERO_SHOP_SAMPLE_CAMBIOS %>% filter(SHO_EXTERNAL_CODE < 9311000000)

fwrite(FICHERO_SHOP_SAMPLE_CAMBIOS, paste0(salidas,"FICHERO_SHOP_SAMPLE_CAMBIOS_",periodo_fin,"_",indice,".csv"), na = '', sep = ";", row.names = F,  quote = F)
  

################################################################################################################