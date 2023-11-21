
                                          ## INAH - Chile ##

#NOTA PARA UTILIZAR EL PROGRAMA DEBERAS ACTUALIZAR LOS INSUMOS DE LA FUNCION read.csv que seran los insumos
#NOTA 2 TAMBIEN DEBES ACTUALIZAR LAS CONSULTAS
#NOTA 3 Al buscar "BuscarCambiar" con Ctrl+f te mandara donde en la siguiente linea hay que hacer cambios

#################### PREAMBULO ###################
rm(list=ls())
options(guiToolkit = "RGtk2")
library(DBI)
library(rJava)
library(RJDBC)
library(RODBC)
library(dplyr)
library(sqldf)
library(xlsx)
library(tidyr)
library(fitdistrplus)
library(data.table)
library(stringr)
library(gWidgets)
library(gWidgets2)
library(gWidgets2RGtk2)
memory.limit()
rm(list = ls())
setwd("C:/Users/jima9001/Desktop/NIELSEN/CHILE PROD/INDE/SALIDAS ACT DIC/SALIDAS/UNIVERSOS")
#gmessage("Seleccione la ruta donde desea que se guarde el archivo",title = "Info",icon = "info")
dir<- ("C:/Users/jima9001/Desktop/NIELSEN/CHILE PROD/INDE/SALIDAS ACT DIC/SALIDAS/UNIVERSOS")
##Carpetas de salida (accesos)
servidor_sms <- "ACN056STGSQL22"        #Servidor SMS
sms_retail<- "SMS"                   #Base de Datos Current del SMS RETAIL
sms_retail_hist <- "SMSHistory"          #Base de Datos History del SMS RETAIL
sms_scan<- "SMScan"                   #BASE DE DATOS SMS SCAN CURRENT
sms_scan_history="SMScanHistory"      #BASE DE DATOS SMS SCAN HISTORY
user<- "usr_read"             #Usuario
pass<- "LEeras31"          #Password
#ind1<-ginput("Ingrese la sample Audit", title = "Indice", icon = "question")
ind1 <- 20000883 #sample audit INDE
sam1<-ind1
sam_pses<-ginput("Ingrese la sample P Sesgada (0 si no aplica)", title = "Indice", icon = "question")

connection_sms <- odbcDriverConnect(paste0("driver={SQL Server};Server=",servidor_sms,";uid=",user,";pwd=",pass))
master <- sqlQuery(connection_sms,
                   paste0("SELECT * FROM ",servidor_sms,".",sms_retail,".dbo.source_master "))
#BuscarCambiar - Relación period_id vs tpr_id del índice
MESES <- read.csv("MESES.csv") 
#BuscarCambiar - Relaci?n mbd_id vs MAS_ID de los mercados a validar
MERCADOS_TABLA <- read.xlsx("Relacion MAS_ID v MBD_ID Ratios - Copy.xlsx", sheetIndex = 1) 
Mercados_mbd_id<-unique(MERCADOS_TABLA$MBD_ID)
mercados_MAS_ID <- toString(unique(MERCADOS_TABLA$MAS_ID))

#BuscarCambiar ?ndice en n?mero
indice <- 6
TIENDAS_A2S <- c(2115,2890,2897,2904,2908,2965,2994,2998,3111,3272,3677,3974,6028,6048,6049,6969,7029,7030,7431,7432,7434,7435,7436,7439,7715,7716,7717,7718,7729,
                 8504,8505,8506,8507,8508,8509,8511,8512,10369,10370,10373,10528,10529,10530,10532,10969,11548,11732,11733,11770,11771,11837,11949,12131,12133,12135,12136,12137,12139,
                 12140,12141,12142,12143,12144,12145,12146,12147,12148,12149,12150,12151,12152,12153,12154,12155,12156,12157,12158,12159,12160,12161,12163,12165,12166,12167,12168,12170,
                 12171,12172,12173,12174,12175,12176,12177,12179,12348,12349,12350,12351,12352,12353,12354,12355,12357,12358,12359,12360,12361,12362,12363,12364,12365,12366,12368,12369,
                 12370,12371,12372,12373,12374,12405,12406,12407,12408,12409,12410,12411,12412,12553,12554,12639,12640,12900,12949,12950,13079,13080,13081,13082,13083,13164,13165,13167,
                 13170,13455,13612,14070,14071,14072,14520,14525,14526,14527,14936,14942,14943,15096,15290,15575,15576,15577,15729,15730,15731,15732,15733,15734,15735,15736,15737,15738,
                 15740,15741,15742,15743,15868,15869,15946,16100,16935,17234,17292,17293,17294,17342,17343,17344,17345,19971,21247,21248,21249,21250,21490,22014,22015,22016,22017,22369,
                 22370,22371,22574,22575,22576,22577,22642,22695,22696,22697,24482,24483,24484,24485,24486,24487,24488,24489,24490,24491,24492,24493,24494,24495,24496,24497,24498,24499)
Mercados_mbd_id<-unique(MERCADOS_TABLA$MBD_ID)
#################### PROGRAMA ####################

while (TRUE){
  #################### PRIMERA PARTE ##########  
  gmessage("EMPEZARA LA PRIMERA PARTE DEL PROGRAMA (Periodo a periodo ; Diferencia en mercados por sample)",title = "Info",icon = "info")
  
  progbar <- txtProgressBar(min=0,max=length(MESES[,1]),style=3,char="=") #Funcion para colocar una barra de avance
  for (d in 1:nrow(MESES)) {
    MS_RES<-data.frame()
    MS_RES_1<-data.frame()
    MS_RES_AUX<-data.frame()
    MS_RES_AUX_1<-data.frame()
    F1<-data.frame()
    F2<-data.frame()
    F3<-data.frame()
    F4<-data.frame()
    F5<-data.frame()
    F6<-data.frame()
    
    #acum<-acum+1
    
    Mes_Legacy<-MESES[d,1]
    Mes_CIP<-MESES[d,2]
    
    `%notin%` = function(x,y) !(x %in% y)
    var_aux<-5
    connection_sms <- odbcDriverConnect(paste0("driver={SQL Server};Server=",servidor_sms,";uid=",user,";pwd=",pass))

    
    
    periodo_inicio <- Mes_Legacy # se cambia para cigarrillos
    periodo_fin <- Mes_Legacy

    period_source_id_2 <- sqlQuery(connection_sms,
                                   paste0("SELECT * FROM ",
                                          servidor_sms,".",sms_retail_hist,".dbo.index_period_source ",
                                          " WHERE period_id >=", periodo_inicio, "AND period_id <=  ", periodo_fin,
                                          " AND index_id = ",indice,
                                          " AND status_id > ",var_aux))
    periodo_inicio <- Mes_Legacy # se cambia para cigarrillos
    periodo_fin <- Mes_Legacy

    period_source_id_2_54 <- sqlQuery(connection_sms,
                                      paste0("SELECT * FROM ",
                                             servidor_sms,".",sms_retail,".dbo.index_period_source ",
                                             " WHERE period_id >=", periodo_inicio, "AND period_id <=  ", periodo_fin,
                                             " AND index_id = ",indice,
                                             " AND status_id > ",var_aux))
    
    period_source_11 <- rbind(period_source_id_2,period_source_id_2_54)
    period_source<-period_source_11
    
    period_source_ztv <- period_source %>% left_join(master, by = c("source_id" = "source_id"))
    
    period_source_1<-sqldf("SELECT DISTINCT source_id,sot_source,scan_source_id FROM period_source_ztv")

    periodo_inicio <- Mes_Legacy # se cambia para cigarrillos
    periodo_fin <- Mes_Legacy

    MAP_3 <- sqlQuery(connection_sms,paste0("SELECT source_id,period_id,mbd_id,status_id FROM ",
                                            servidor_sms,".",sms_retail_hist,".dbo.map",
                                            " WHERE period_id >=", periodo_inicio, " AND period_id <=  ", periodo_fin,
                                            " AND index_id = ",indice,
                                            " AND status_id > 5"))
    
    MAP_4 <- sqlQuery(connection_sms,paste0("SELECT source_id,period_id,mbd_id,status_id FROM ",
                                            servidor_sms,".",sms_retail,".dbo.map",
                                            " WHERE period_id >=", periodo_inicio, " AND period_id <=  ", periodo_fin,
                                            " AND index_id = ",indice,
                                            " AND status_id > 5"))
    MAP <- rbind(MAP_3,MAP_4)
    
    MAP_1 <- MAP %>% left_join(period_source_1, by = c("source_id" = "source_id"))
    Mercados_Master<-as.numeric(Mercados_mbd_id)
    #Mercados_Master<-c(267,268,269,270,3053,3029,3028,3054,9245,3124,3020,9246,3021,3128,3055,3022,3023,3024,3056,3026,3025,3027,3074,3086,3087,3088,3089,3090,3091,3096,817,818,149,148,232,129,128,9218,176,177,175,815,820,816,2001,472,3040,422,421,417,3041,3042,9149,473,420,431,474,3014,3016,3015,3007,3008,3013,3017,3317,3325,3329,3333,3337,3318,3326,3330,3334,3338,3340,3333,3334,3331,3332,2009,2010,2012,2014,2016,9147,100,1001,1002,1003,1004,1005,1006,1011,8744,9089,605,604,223,615,609,219,349,610,207,351,9148,9195,607,227,233,229,9153,9151,9152,3328,3324,3336,9178,9177,9179,9180,9187,9188,9189,9190,9161,9159,9162,9160,9167,9185,9169,9168,3057,3010,1002,1012,1007,1008,1010,1009,723,3075,101,105,100,809,204,2008,805,9146,228,803,804,216,801,224,2013,811,2017,148,232,813,220,3034,139,138,814,3077,3078,216,3251,3079,3321,3322,3274,3099,9217,806,843,395,236,173,905,906,3110,3196,3104,3152,3087,3097,3092,3093,3095,3094,3030)
    Mercados_Master<-as.data.frame(Mercados_Master)
    Mercados_Master$FLAG<-1
    MAP_1<- MAP_1 %>% left_join(Mercados_Master,by = c("mbd_id"="Mercados_Master"))
    
    MAP_1<-MAP_1 %>% filter(FLAG==1)
    
    MAP_aux<-MAP_1 %>% filter(period_id==Mes_Legacy)
    
    #cambiar
    Sys.setenv(JAVA_HOME='C:/64 bits/sqldeveloper-17.3.1.279.0537-x64/sqldeveloper/jdbc/lib/ojdbc8.jar')
    library(rJava)
    library(RJDBC)
    library(RODBC)
    library(dplyr)
    drv <- JDBC(driverClass = "oracle.jdbc.OracleDriver"
                , classPath = "C:/64 bits/sqldeveloper-17.3.1.279.0537-x64/sqldeveloper/jdbc/lib/ojdbc8.jar"       
                , identifier.quote = "'")
    conn_m <- dbConnect(drv=drv
                        , url = "jdbc:oracle:thin:@dayrhevalplc1:1521/VALPLC01"
                        , user = "MADRAS_GUEST"
                        , password = "MADRAS_GUEST")

    MS_RES <- dbGetQuery(conn=conn_m,statement=paste0("SELECT DISTINCT A.MSR_TPR_ID, A.MSR_MAS_ID, A.MSR_X_FACTOR,
                                                        A.MSR_Z_FACTOR, B.SHO_EXTERNAL_CODE
                                                        FROM MADRAS_DATA.TMMS_MS_RESOLUTION A
                                                        LEFT JOIN MADRAS_DATA.TRSH_SHOP B
                                                        ON MSR_SHO_ID = SHO_ID
                                                        WHERE MSR_CCH_ID LIKE 'CL%'
                                                        AND MSR_CEL_SAM_ID = ",sam1,"
                                                        AND MSR_TPR_ID = ",Mes_CIP,"
                                                        AND MSR_MAS_ID IN (20050956,20050957,20050958,20050959,20050969,20050972,20050971,20050970,20050981,20050987,20050985,20050984,20050983,20050982,20050986,20050978,20050980,20051010,20051013,20051012,20051024,20051029,20051025,20051026,20051028,20051036,20051040,20051042,20051038,20051041,20051039,20051037,20051050,20051053,20051052,20051054,20051055,20051051,20051056,20051061,20051060,20051059,20051057,20051058,20051062,20051043,20051044,20051048,20051045,20051046,20051049,20051047,20051033,20051034,20051030,
                                                      20051031,20051021,20051023,20051020,20050997,20050994,20050996,20050995,20050991,20050993,20050992,20050990)"))
    #BuscarCambiar###########PREGUNTAR SCAN 1
    MS_RES_1 <- dbGetQuery(conn=conn_m,statement=paste0("SELECT DISTINCT A.MSR_TPR_ID, A.MSR_MAS_ID, A.MSR_X_FACTOR,
                                                          A.MSR_Z_FACTOR, B.SHO_EXTERNAL_CODE
                                                          FROM MADRAS_DATA.TMMS_MS_RESOLUTION A
                                                          LEFT JOIN MADRAS_DATA.TRSH_SHOP B
                                                          ON MSR_SHO_ID = SHO_ID
                                                          WHERE MSR_CCH_ID LIKE 'CL%'
                                                          AND MSR_CEL_SAM_ID = 20000686
                                                          AND MSR_TPR_ID = ",Mes_CIP,"
                                                          AND MSR_MAS_ID IN (20050956,20050957,20050958,20050959,20050969,20050972,20050971,20050970,20050981,20050987,20050985,20050984,20050983,20050982,20050986,20050978,20050980,20051010,20051013,20051012,20051024,20051029,20051025,20051026,20051028,20051036,20051040,20051042,20051038,20051041,20051039,20051037,20051050,20051053,20051052,20051054,20051055,20051051,20051056,20051061,20051060,20051059,20051057,20051058,20051062,20051043,20051044,20051048,20051045,20051046,20051049,20051047,20051033,20051034,20051030,
                                                      20051031,20051021,20051023,20051020,20050997,20050994,20050996,20050995,20050991,20050993,20050992,20050990)"))

    if (sam_pses > 0) {
      MS_RES_2 <- dbGetQuery(conn=conn_m,statement=paste0("SELECT DISTINCT A.MSR_TPR_ID, A.MSR_MAS_ID, A.MSR_X_FACTOR,
                                                            A.MSR_Z_FACTOR, B.SHO_EXTERNAL_CODE
                                                            FROM MADRAS_DATA.TMMS_MS_RESOLUTION A
                                                            LEFT JOIN MADRAS_DATA.TRSH_SHOP B
                                                            ON MSR_SHO_ID = SHO_ID
                                                            WHERE MSR_CCH_ID LIKE 'CL%'
                                                            AND MSR_CEL_SAM_ID = ",sam_pses,"
                                                            AND MSR_TPR_ID = ",Mes_CIP,"
                                                            AND MSR_MAS_ID IN (20050956,20050957,20050958,20050959,20050969,20050972,20050971,20050970,20050981,20050987,20050985,20050984,20050983,20050982,20050986,20050978,20050980,20051010,20051013,20051012,20051024,20051029,20051025,20051026,20051028,20051036,20051040,20051042,20051038,20051041,20051039,20051037,20051050,20051053,20051052,20051054,20051055,20051051,20051056,20051061,20051060,20051059,20051057,20051058,20051062,20051043,20051044,20051048,20051045,20051046,20051049,20051047,20051033,20051034,20051030,
                                                      20051031,20051021,20051023,20051020,20050997,20050994,20050996,20050995,20050991,20050993,20050992,20050990)"))
      
      MS_RES_AUX_2<-MS_RES_2 %>% left_join(MERCADOS_TABLA,by = c("MSR_MAS_ID"="MAS_ID"))
      MS_RES_AUX_2$SHOP_SCAN<-as.numeric(MS_RES_AUX_2$SHO_EXTERNAL_CODE)-5501000000
      MS_RES_AUX_2$LLAVE_3<- paste0(MS_RES_AUX_2$MBD_ID,"_",MS_RES_AUX_2$SHOP_SCAN)
      
      MAP_aux$LLAVE_3 <- paste0(MAP_aux$mbd_id, "_", MAP_aux$scan_source_id)
      
      ############## PARA EST SESG ########################
      #BuscarCambiar - Vector de tiendas de proyecci?n sesgada del ?ndice trabajado
      EST_SESG_SHOPS <- c(1135,2551,2332,2553,3420,2331)
      
      MAP_aux_ES <- MAP_aux %>% filter(scan_source_id %in% EST_SESG_SHOPS)
      MAP_SCAN_ES<- MAP_aux_ES %>% filter(SUMA==1)
      SCAN_FINAL_ES<-MAP_SCAN_ES %>% left_join(MS_RES_AUX_2,by = c("LLAVE_3"="LLAVE_3"))
      F5 <-SCAN_FINAL_ES %>% filter(is.na(SHO_EXTERNAL_CODE))
      SCAN_FINAL_1_ES<- MS_RES_AUX_2 %>% left_join(MAP_SCAN_ES,by = c("LLAVE_3"="LLAVE_3"))
      F6 <-SCAN_FINAL_1_ES %>% filter(is.na(scan_source_id))
    }
    
    #BuscarCambiar - C?digo de servicio
    MS_RES_AUX<-MS_RES %>% left_join(MERCADOS_TABLA,by = c("MSR_MAS_ID"="MAS_ID"))
    MS_RES_AUX$SHOP<-as.numeric(MS_RES_AUX$SHO_EXTERNAL_CODE)-5520000000
    if(nrow(MS_RES_AUX)!=0){
      MS_RES_AUX$LLAVE<- paste0(MS_RES_AUX$MBD_ID,"_",MS_RES_AUX$SHOP)
    }
    MS_RES_AUX_1<-MS_RES_1 %>% left_join(MERCADOS_TABLA,by = c("MSR_MAS_ID"="MAS_ID"))
    MS_RES_AUX_1$SHOP_SCAN<-as.numeric(MS_RES_AUX_1$SHO_EXTERNAL_CODE)-5500000000
    MS_RES_AUX_1$LLAVE_2<- paste0(MS_RES_AUX_1$MBD_ID,"_",MS_RES_AUX_1$SHOP_SCAN)
    
    MAP_aux[MAP_aux$source_id %in% TIENDAS_A2S,"sot_source"] <- 0
    MAP_aux$SUMA<-MAP_aux$sot_source
    MAP_aux$LLAVE<- paste0(MAP_aux$mbd_id,"_",MAP_aux$source_id)
    MAP_aux$LLAVE_2<- paste0(MAP_aux$mbd_id,"_",MAP_aux$scan_source_id)

    
    ############## PARA AUDIT ########################
    
    MAP_AUDIT<-MAP_aux %>% filter(sot_source==0 | SUMA==0)
    if(nrow(MS_RES_AUX)!=0){
      AUDIT_FINAL<-MAP_AUDIT %>% left_join(MS_RES_AUX,by = c("LLAVE"="LLAVE"))
      F1<-AUDIT_FINAL %>% filter(is.na(SHO_EXTERNAL_CODE))
      AUDIT_FINAL_1<-MS_RES_AUX %>% left_join(MAP_AUDIT,by = c("LLAVE"="LLAVE"))
      F2<-AUDIT_FINAL_1 %>% filter(is.na(source_id))
    }
    
    ############## PARA SCAN ########################
    
    MAP_SCAN<-MAP_aux %>% filter(sot_source==1)
    SCAN_FINAL<-MAP_SCAN %>% left_join(MS_RES_AUX_1,by = c("LLAVE_2"="LLAVE_2"))
    F3<-SCAN_FINAL %>% filter(is.na(SHO_EXTERNAL_CODE))
    SCAN_FINAL_1<- MS_RES_AUX_1 %>% left_join(MAP_SCAN,by = c("LLAVE_2"="LLAVE_2"))
    F4<-SCAN_FINAL_1 %>% filter(is.na(scan_source_id))
    
    if (nrow(F3)!=0) {
      F3$SEMANA<-Mes_CIP
    }
    
    file <- paste0(dir,"\\COMPARACION_UNIVERSOS_",Mes_Legacy,"_",Mes_CIP,".xlsx",sep="")
    wb <- createWorkbook()
    sheet1 <- createSheet(wb, sheetName="Map vs Madras AUDIT")
    sheet2 <- createSheet(wb, sheetName="Madras vs Map AUDIT")
    sheet3 <- createSheet(wb, sheetName="Map vs Madras SCAN")
    sheet4 <- createSheet(wb, sheetName="Madras vs Map SCAN")
    if(sam_pses > 0){
      sheet5 <- createSheet(wb, sheetName="Map vs Madras EST SESG")
      sheet6 <- createSheet(wb, sheetName="Madras vs Map EST SESG")
    }
    addDataFrame(F1, sheet1, row.names=F)
    addDataFrame(F2, sheet2, row.names=F)
    addDataFrame(F3, sheet3, row.names=F)
    addDataFrame(F4, sheet4, row.names=F)
    if (sam_pses > 0) {
      addDataFrame(F5, sheet5, row.names=F)
      addDataFrame(F6, sheet6, row.names=F)
    }
    
    saveWorkbook(wb, file)
    #print(d)
    Sys.sleep(0.5) #Pausa para generar la barra de avance
    setTxtProgressBar(progbar,value=d) #Actualiza el progreso de ejecucion de la barra de avance
  }
  close(progbar)
  gmessage("Lo deseable de estos archivos es que esten vacios",title = "Info",icon = "info")
  
  #################### SEGUNDA PARTE ####################
  continuar<-ginput("Desea continuar con la siguiente parte: \n \n 1:Si \n 2:No", title = "Variables", icon = "question")
  if (continuar==2) {
    break
  }
  gmessage("EMPEZARA LA SEGUNDA PARTE DEL PROGRAMA (Creacion de consolidados)",title = "Info",icon = "info")
  
  rm(list=ls())
  options(guiToolkit = "RGtk2")
  
  library(DBI)
  library(rJava)
  library(RJDBC)
  library(RODBC)
  library(dplyr)
  library(sqldf)
  library(xlsx)
  library(tidyr)
  library(data.table)
  library(stringr)
  library(gWidgets) 
  library(gWidgets2)
  library(gWidgets2RGtk2)
  
  gmessage("Seleccione todos los archivos a procesar(Todos los archivos de la parte 1)",title = "Info",icon = "info")
  documentos <- choose.files()
  
  gmessage("Seleccione la ruta donde desea que se guarde el archivo",title = "Info",icon = "info")
  dir<-choose.dir()
  
  
  lista1<-list()
  lista2<-list()
  Consolidado_MAP_MADRAS<-data.frame()
  Consolidado_MADRAS_MAP<-data.frame()
  progbar <- txtProgressBar(min=0,max=length(documentos),style=3,char="=") #Funcion para colocar una barra de avance
  for (x in 1:length(documentos)) {
    aux1<-data.frame()
    aux2<-data.frame()
    lista1[[x]]<-read.xlsx(documentos[x],sheetName = "Map vs Madras SCAN")
    lista2[[x]]<-read.xlsx(documentos[x],sheetName = "Madras vs Map SCAN")
    if (nrow(lista1[[x]])!=0) {
      aux1<- lista1[[x]][,c(18,6)]
    }
    if (nrow(lista2[[x]])!=0) {
      aux2<- lista2[[x]][,c(1,7)]
    }
    
    Consolidado_MAP_MADRAS<-rbind(Consolidado_MAP_MADRAS,aux1)
    Consolidado_MADRAS_MAP<-rbind(Consolidado_MADRAS_MAP,aux2)
    Sys.sleep(0.5) #Pausa para generar la barra de avance
    setTxtProgressBar(progbar,value=x) #Actualiza el progreso de ejecucion de la barra de avance
  }
  close(progbar)
  
  Consolidado_MAP_MADRAS <- Consolidado_MAP_MADRAS[!duplicated(Consolidado_MAP_MADRAS), ]
  Consolidado_MADRAS_MAP <- Consolidado_MADRAS_MAP[!duplicated(Consolidado_MADRAS_MAP), ]
  #BuscarCambiar - Relaci?n TPR_ID vs period_id Scantrack
  equivalencia<-read.csv("EQUIVALENCIA.csv")
  
  Consolidado_MAP_MADRAS_<-Consolidado_MAP_MADRAS %>% left_join(equivalencia,by = c("SEMANA"="TPR.ID"))
  colnames(Consolidado_MAP_MADRAS_)[colnames(Consolidado_MAP_MADRAS_)=="LEGACY.SCANTRACK"] <- "MES"
  Consolidado_MAP_MADRAS_1<-sqldf("SELECT MES,scan_source_id FROM Consolidado_MAP_MADRAS_")
  #BuscarCambiar - Calendario Audit
  equivalencia1<-read.csv("StorageWeek_Par_Impar.csv")
  
  Consolidado_MADRAS_MAP_<-Consolidado_MADRAS_MAP %>% left_join(equivalencia1,by = c("MSR_TPR_ID"="TPR_ID_1"))
  colnames(Consolidado_MADRAS_MAP_)[colnames(Consolidado_MADRAS_MAP_)=="PAR"] <- "MES_PAR"
  Consolidado_MADRAS_MAP_1<-sqldf("SELECT MES_PAR,SHOP_SCAN FROM Consolidado_MADRAS_MAP_")
  
  fwrite(Consolidado_MAP_MADRAS_1,paste0(dir,"\\Consolidado_MAP_MADRAS.csv",sep=""))
  fwrite(Consolidado_MADRAS_MAP_1,paste0(dir,"\\Consolidado_MADRAS_MAP.csv",sep=""))
  
  #################### TERCERA PARTE ####################
  continuar<-ginput("Desea continuar con la siguiente parte: \n \n 1:Si \n 2:No", title = "Variables", icon = "question")
  if (continuar==2) {
    break
  }
  gmessage("EMPEZARA LA TERCERA PARTE DEL PROGRAMA (Validacion de MAP vs MADRAS)",title = "Info",icon = "info")
  
  rm(list=ls())
  options(guiToolkit = "RGtk2")
  
  library(DBI)
  library(rJava)
  library(RJDBC)
  library(RODBC)
  library(dplyr)
  library(sqldf)
  library(xlsx)
  library(tidyr)
  library(data.table)
  library(stringr)
  library(gWidgets) 
  library(gWidgets2)
  library(gWidgets2RGtk2)
  
  gmessage("Seleccione la ruta donde desea que se guarde el archivo",title = "Info",icon = "info")
  dir<-choose.dir()
  
  ##Carpetas de salida (accesos)
  servidor_sms <- "ACN056STGSQL22"        #Servidor SMS
  sms_retail<- "SMS"                   #Base de Datos Current del SMS RETAIL
  sms_retail_hist <- "SMSHistory"          #Base de Datos History del SMS RETAIL
  sms_scan<- "SMScan"                   #BASE DE DATOS SMS SCAN CURRENT
  sms_scan_history="SMScanHistory"      #BASE DE DATOS SMS SCAN HISTORY
  user<- "usr_read"             #Usuario
  pass<- "LEeras31"  
  periodo_inicio_scan<-2017041
  periodo_fin_scan<-2019067


    
  
  connection_sms <- odbcDriverConnect(paste0("driver={SQL Server};Server=",servidor_sms,";uid=",user,";pwd=",pass))
  #Cambiar indice scan de acuerdo al pais
  #BuscarCambiar
  period_source <- sqlQuery(connection_sms,
                            paste0("SELECT period_id,source_id FROM ",
                                   servidor_sms,".",sms_scan_history,".dbo.index_period_source ",
                                   " WHERE period_id >=", periodo_inicio_scan, "AND period_id <=  ", periodo_fin_scan,
                                   " AND index_id IN (1) AND status_id > 5 "))
  
  gmessage("Seleccione el archivo CONSOLIDADO MAP vs MADRAS ",title = "Info",icon = "info")
  
  tiendas_malas<-read.csv(file.choose())
  tiendas_malas$LLAVE<-paste0(tiendas_malas$scan_source_id,"_",tiendas_malas$MES_PAR)
  period_source$LLAVE<-paste0(period_source$source_id,"_",period_source$period_id)
  Validacion<-tiendas_malas %>% left_join(period_source,by = c("LLAVE"="LLAVE"))
  Validacion$Comentario<-ifelse(is.na(Validacion$source_id),"NR - Tienda usable, pero inactiva en storage week","R - Tienda usable de mas en semana")
  
  
  fwrite(Validacion,paste0(dir,"\\Validacion_MAP_MADRAS.csv",sep=""))
  
  #################### CUARTA PARTE ####################
  continuar<-ginput("Desea continuar con la siguiente parte: \n \n 1:Si \n 2:No", title = "Variables", icon = "question")
  if (continuar==2) {
    break
  }
  
  gmessage("EMPEZARA LA CUARTA PARTE DEL PROGRAMA (Validacion de MADRAS vs MAP)",title = "Info",icon = "info")
  
  rm(list=ls())
  options(guiToolkit = "RGtk2")
  
  library(DBI)
  library(rJava)
  library(RJDBC)
  library(RODBC)
  library(dplyr)
  library(sqldf)
  library(xlsx)
  library(tidyr)
  library(data.table)
  library(stringr)
  library(gWidgets) 
  library(gWidgets2)
  library(gWidgets2RGtk2)
  
  gmessage("Seleccione la ruta donde desea que se guarde el archivo",title = "Info",icon = "info")
  dir<-choose.dir()
  
  ##Carpetas de salida (accesos)
  
  servidor_sms <- "ACN056STGSQL22"        #Servidor SMS
  sms_retail<- "SMS"                   #Base de Datos Current del SMS RETAIL
  sms_retail_hist <- "SMSHistory"          #Base de Datos History del SMS RETAIL
  sms_scan<- "SMScan"                   #BASE DE DATOS SMS SCAN CURRENT
  sms_scan_history="SMScanHistory"      #BASE DE DATOS SMS SCAN HISTORY
  user<- "usr_read"             #Usuario
  pass<- "LEeras31"           #Password
  
  periodo_inicio_scan <-2017041
  periodo_fin_scan <-2019067
  
  
  connection_sms <- odbcDriverConnect(paste0("driver={SQL Server};Server=",servidor_sms,";uid=",user,";pwd=",pass))
  
  period_source <- sqlQuery(connection_sms,
                            paste0("SELECT period_id,source_id FROM ",
                                   servidor_sms,".",sms_scan_history,".dbo.index_period_source ",
                                   " WHERE period_id >=", periodo_inicio_scan, "AND period_id <=  ", periodo_fin_scan,
                                   " AND index_id IN (1) AND status_id > 5 "))
  
  gmessage("Seleccione el archivo CONSOLIDADO MADRAS vs MAP ",title = "Info",icon = "info")
  
  tiendas_malas<-read.csv(file.choose())
  tiendas_malas$LLAVE<-paste0(tiendas_malas$SHOP_SCAN,"_",tiendas_malas$MES)
  #BuscarCambiar
  equivalenciaa<-read.xlsx("CALENDARIO.xlsx", sheetIndex = 1)
  Cruce<-period_source %>% left_join(equivalenciaa,by = c("period_id"="LEGACY_SCANTRACK"))
  Cruce$LLAVE<-paste0(Cruce$source_id,"_",Cruce$PAR)
  
  Validacion<-Cruce %>%                       
    group_by(LLAVE) %>%       
    tally()
  
  
  Validacion_pro<-tiendas_malas %>% left_join(Validacion,by = c("LLAVE"="LLAVE"))
  
  Validacion_pro$n[is.na(Validacion_pro$n)]<-0
  Validacion_pro$Comentario<-ifelse(Validacion_pro$n<3,"NR - Tienda no usable, pero activa en storage week","R - Tienda usable, Revisar")
  fwrite(Validacion_pro,paste0(dir,"\\Validacion_MADRAS_MAP.csv",sep=""))
  
  break
}




