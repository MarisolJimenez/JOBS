##Comparativo LEGACY vs CIP
##?Mercados con las mismas tiendas? 
##?Factores de proyeccion iguales?
library(dplyr)
library(DBI)
library(rJava)
library(RJDBC)
library(RODBC)
library(sqldf)
library(xlsx)
library(tidyr)
library(fitdistrplus)
library(data.table)
#####C H E C A R   I D I O M A    J E R A R Q U I A S    S E A     I G U A L
###########
##INSUMOS##
rm(list=ls())
setwd("C:/Users/jima9001/Desktop/NIELSEN/ARG POC/SUMAS")
#setwd("C:/Users/jima9001/Desktop/NIELSEN/CAM/SHEETS CITLA VALIDACIONES/SUMA MERCADOS/VALIDACIONES R &CHECKS")

calendario <- read.csv("C:\\Users\\jima9001\\Desktop\\NIELSEN\\CAM PROD\\VALIDACIONES\\CALENDARIO.csv") 
##TSR ##cadenas
##CAMBIA EL INSUMO PARA LAS NO TSR
#cadenas_tsr_periodos <- read.csv("INSUMOS/CADENAS_TOTAL.csv")
##TSR ##cadenas_tsr_periodos <- read.csv("INSUMOS/CADENAS_TSR_PERIODOS.csv")

##Equivalencia TPR_ID y period_id
periodos <- calendario
Mercados <- read.csv("mbd_mas_id.csv")
shell.exec("mbd_mas_id.csv")
#Mercados <- Mercados[1,]
# Mercados[1,1] <- 20041528  #cip
# Mercados[1,2] <- 2
 #mbd
a<-"SUMAS_SCAN" #NOMBRE DE ARCHIVOS SALIENTES
periodo_inicial <- 2017038
#periodo_final <- 2019067
#periodos_msr <- c(942:1071)#all
#PERIODOS CORTADOS ############### 
#periodo_final <- 2017073 #1 
#periodo_inicial <- 2019025
# periodo_final <- 2018035#2 # periodo_inicial <- 2018036 # 
#periodo_final <- 2018049#3
#periodo_inicial <- 2018050 # 
periodo_final <- 2019076 #1097

#4 # periodo_inicial <- 2018064
# periodo_final <- 2019025#5 # periodo_inicial <- 2019026 # periodo_final <- 2019037#6
#periodo_inicial <- 2019038
#periodos_msr<-c(1089:1089)#2 
periodos_msr<-c(955:1097)#2 
#periodos_msr <- c(1019:1071)
                    #990)#1 # 
#periodos_msr<-c(991:1004)#2 # periodos_msr<-c(1005:1018)#3
# 
#periodos_msr<-c(1046:1088)#4 # periodos_msr<-c(1033:1046)#5 # periodos_msr<-c(1047:1058)#6
#periodos_msr<-c(1059:1071)#7 
#######################
                 ##DESCARGA DE LA MSR## 
memory.limit(9000)
servidor_sms<- "acn054buesqlsms"        #Servidor SMS
sms_scan<- "SMSscanArg"                   #BASE DE DATOS SMS SCAN CURRENT
#sms_scan_history="smsscancam"      #BASE DE DATOS SMS SCAN HISTORY
sms_scan_history="SMSscanArgHistory"      #BASE DE DATOS SMS SCAN HISTORY
user<- "usr_read"             #Usuario
pass<- "LEeras31"             #Password
connection_sms <- odbcDriverConnect(paste0("driver={SQL Server};Server=",servidor_sms,";uid=",user,";pwd=",pass))

#UNE MI PC A BASES DE DATOS, necesario para las bases de MADRAS
drv <- JDBC(driverClass = "oracle.jdbc.OracleDriver"
            , classPath = "C:/64 bits/sqldeveloper-17.3.1.279.0537-x64/sqldeveloper/jdbc/lib/ojdbc8.jar"       
            , identifier.quote = "'")

##Conexión a las BD de MADRAS POC, ##credenciales para entrar a SQL
 conn_m <- dbConnect(drv=drv
                     , url = "jdbc:oracle:thin:@daylvalq01:1521/VALPOCNA"
                     , user = "MADRAS_GUEST"
                     , password = "MADRAS_GUEST")
##Conexión a las BD de MADRAS PRODUCTIVO
# conn_m <- dbConnect(drv=drv
#                     , url = "jdbc:oracle:thin:@dayrhevalplc1.enterprisenet.org:1521/VALPLC01"
#                     , user = "MADRAS_GUEST"
#                     , password = "MADRAS_GUEST")

# Obtener datos de la MSR. MARKET SEGMENT RESOLUTION
for (i in periodos_msr) {
  t0=Sys.time()
  msr <- dbGetQuery(conn=conn_m,statement=paste0("SELECT MSR_TPR_ID,
                                                 SHO_EXTERNAL_CODE,
                                                 MSR_CEL_ID,
                                                 MSR_X_FACTOR,
                                                 MSR_Z_FACTOR,
                                                 MSR_MAS_ID
                                                 FROM MADRAS_DATA.tmms_ms_resolution
                                                 JOIN MADRAS_DATA.TRSH_SHOP ON MSR_sho_id=sho_id
                                                 WHERE msr_cch_id IN ('ARSCA1') 
                                                 AND msr_cel_sam_id IN (991698)
                                                 AND MSR_MAS_ID IN (1097961,1098008,1098009,1098011,1098010,1098013,1098012,1097944,1097997,
                                                 1097987,1097974,1097975,1097972,1098021,1097980,1098002,1097979,1097969,1098006,1097984,
                                                 1097985,1097932,1097971,1097983,1097967,1097965,1097936,1097945,1097988,1098014,1097963,
                                                 1098038,1098004,1098015,1097968,1097960,1097938,1097970,1097981,1097966,1098007,1098016,
                                                 1097953,1097937,1097964,1097994,1097991,1097989,1097995,1097996,1097993,1097992,1097990,
                                                 1098003,1097973,1098017,1097976,1098018,1097930,1098005,1097934,1097933,1097949,1097947,
                                                 1097946,1097948,1097977,1097935,1098000,1098001,1097998,1097999,1097958,1097959,1097957,
                                                 1097956,1097955,1097978,1098020,1097931,1097952,1097943,1097940,1097939,1097941,1097942,
                                                 1098022,1098023,1098019,1097982,1097986,1097951,1097962,1097950,1097954)
                                                 AND msr_tpr_id =",i,""))
  assign(paste0("MSR_",i,""),msr)
  t1=Sys.time()
  et=t1-t0
  print(paste0("EL periodo ",i," se ha completado en:"))
    print(et)}

msr_fin <- data_frame()

for (j in periodos_msr) {msr_fin <- rbind(msr_fin, get(paste0("MSR_",j,"")))}

msr <- msr_fin 

rm(list = ls()[grepl("MSR_", ls())]) #BORRAR LAS MSR_i QUE SE OBTUVIERON

##MAP / INDEX_PERIOD_SOURCE## con tiendas activas
map <- sqlQuery(connection_sms,
                paste0("SELECT period_id, index_id, mbd_id, source_id, x_factor, z_factor, source_acv
                        FROM ",servidor_sms,".",sms_scan_history,".dbo.map ",
                       "WHERE period_id BETWEEN ", periodo_inicial, " AND ", periodo_final,
                       " AND index_id = 1 
                          AND status_id in (7,8)
                        AND mbd_id IN (684,3505,689,1475,1746,746,692,349,3084,1712,620,3510,676,1621,3257,
                       1767,3,3188,1805,3507,596,1733,605,1956,1552,3140,610,3078,1933,609,1981,608,3031,612,
                       1874,603,1776,604,1748,1955,3141,462,474,262,1957,500,538,3831,505,3181,3771,509,231,
                       301,1433,182,162,142,1754,1807,1756,1790,204,3535,3617,3851,3678,3679,3680,3681,3700,
                       3449,1308,1307,102,1796,693,3506,1770,691,1937,690,137,685,3065,633,3509,3190,635,3263,
                       382,1750,678,1472,1938,1982,1553,3069,611,3235,1728,3077,1783,1979,1980,3086,3317,3072,
                       1987,1988,1989,3168,3523,3060,3082,1916,1409,724,3172,1978,1873,3287,3121,3236,3058,
                       934,1772,1798,1743,3193,3064,3192,3508,3066,1922,1961,1665,3059,1983,3319,3183,3057,454,
                       1732,1848,1771,3453,634,765,1811,3063,3718,3142,3083,3169,3173,10,1903,1992,3504,1715,
                       607,3237,1683,1809,1808,1810,3621,3452,1306,4,458,475,3721,3126,670,3182,504,503,236,
                       3831,249,1787,1757,1780,1804,3628,3618,3755,3766,3764,3756,3767,3769,3451,1719,142,3143,
                       3083,3067,3455,3080,3585,1411,3521,3286,1607,3583,3773,1459,1598,1423,3031)")) 

map_v <- sqldf("SELECT period_id, index_id, mbd_id, source_id, x_factor, z_factor, source_acv
               FROM map")

msr_2 <- msr %>% left_join(Mercados, by = "MSR_MAS_ID")

msr_2 <- msr_2 %>% left_join(periodos, by = "MSR_TPR_ID")

msr_2 <- msr_2 %>% mutate(source_id = as.numeric(SHO_EXTERNAL_CODE) - 4700000000)
msr_2 <- msr_2 %>% mutate(llave = paste0(MSR_MAS_ID, "_", period_id, "_", source_id))
colnames(msr_2) <- c("MSR_TPR_ID_m", "SHO_EXTERNAL_CODE_m", "MSR_CEL_ID_m", "MSR_X_FACTOR_m", "MSR_Z_FACTOR_m", "MSR_MAS_ID_m",
                     "mbd_id_m", "period_id_m", "source_id_m", "llave")

map_2 <- map_v %>% left_join(Mercados, by = "mbd_id")
map_2 <- map_2 %>% left_join(periodos, by = "period_id")
map_2 <- map_2 %>% mutate(SHO_EXTERNAL_CODE = as.numeric(source_id) + 4700000000)
map_2 <- map_2 %>% mutate(llave = paste0(MSR_MAS_ID, "_", period_id, "_", source_id))
colnames(map_2) <- c("period_id_s", "index_id_s", "mbd_id_s", "source_id_s", "x_factor_s", "z_factor_s", "source_acv_s",        
                     "MSR_MAS_ID_s", "MSR_TPR_ID_s", "SHO_EXTERNAL_CODE_s", "llave" )

msr_vs_map <- msr_2 %>% left_join(map_2, by = "llave") #NA= EN MSR PERO NO MAP 

msr_vs_map_2 <- sqldf("SELECT MSR_TPR_ID_m AS MSR_TPR_ID, period_id_m AS period_id,
                              MSR_MAS_ID_m AS MSR_MAS_ID, mbd_id_m AS mbd_id,
                              SHO_EXTERNAL_CODE_m AS SHO_EXTERNAL_CODE, source_id_m AS source_id, 
                              llave, x_factor_s, MSR_X_FACTOR_m , z_factor_s, MSR_Z_FACTOR_m
                     FROM msr_vs_map")

map_vs_msr <- map_2 %>% left_join(msr_2, by = "llave") #NA= EN MAP PERO NO EN MSR

map_vs_msr_2 <- sqldf("SELECT MSR_TPR_ID_s AS MSR_TPR_ID, period_id_s AS period_id,
                              MSR_MAS_ID_s AS MSR_MAS_ID, mbd_id_s AS mbd_id_s,
                              SHO_EXTERNAL_CODE_s AS SHO_EXTERNAL_CODE, source_id_s AS source_id, 
                              llave, MSR_X_FACTOR_m, x_factor_s, MSR_Z_FACTOR_m, z_factor_s
                      FROM map_vs_msr")


sobrantes <- msr_vs_map_2 %>% filter(is.na(x_factor_s)) # EN MSR PERO NO EN MAP
faltantes <- map_vs_msr_2 %>% filter(is.na(MSR_X_FACTOR_m)) #EN MAP PERO NO EN MSR

indice <- 1
INDEX_SOURCE <- sqlQuery(connection_sms,
                         paste0("SELECT distinct cadena_secundaria,auxiliar,formato,source_id,period_id,ratio_353 
                                      FROM ",
                                servidor_sms,".",sms_scan_history,".dbo.index_period_source ",
                                " WHERE period_id >=", periodo_inicial, "AND period_id <=  ", periodo_final,
                                " AND index_id = ",indice,
                                " AND status_id > 5"))
MASTER<- sqlQuery(connection_sms,
                  paste0("SELECT source_id,cadena_de_tiendas,localidad,departamento,area,
                                 provincia,localidad_asbase
                                   FROM ",servidor_sms,".",sms_scan,".dbo.source_master "))



sobrantes <- sobrantes %>% left_join(INDEX_SOURCE,by=c("source_id","period_id"))
sobrantes <- sobrantes %>% left_join(MASTER,by="source_id") #debe salir justificado
faltantes <- faltantes %>% left_join(MASTER,by="source_id")
faltantes <- faltantes %>% left_join(INDEX_SOURCE,by=c("source_id","period_id"))
tsr <- c(57,4,6,13,33,87,202,224,225,226,75,42,170,333,27013)
faltantes_real <- faltantes %>% filter(cadena_de_tiendas %in% tsr ||cadena_secundaria %in% tsr) #956 es paralela colonia 317


fwrite(sobrantes,"sobrantes.csv")
fwrite(faltantes_real,"faltantes")
shell.exec("sobrantes.csv")
