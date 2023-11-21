##VALIDACION MERCADOS ADICIONALES DE SUMAS
 library(rJava)
 library(RJDBC)
 library(RODBC)
 library(dplyr)
 library(DBI)
 library(sqldf)
 library(tidyr)
 library(fitdistrplus)
 library(data.table)
 library(rJava)
 library(xlsx)
rm(list=ls())
SUMA<- "20046274" #tsr 
TOTAL<-(20041478) #no tsr
PERIODO_INCIAL <- 990
PERIODO_FINAL <- 1097
#drv <- JDBC("oracle.jdbc.OracleDriver", classPath="G://My Drive//R//BO-PY//ojdbc6.jar", " ")
#MADRAS <- dbConnect(drv,"jdbc:oracle:thin:@daylvalq01:1521/VALPOCNA","MADRAS_GUEST", "MADRAS_GUEST")
drv <- JDBC(driverClass = "oracle.jdbc.OracleDriver"
            , classPath = "C:/64 bits/sqldeveloper-17.3.1.279.0537-x64/sqldeveloper/jdbc/lib/ojdbc8.jar"       
            , identifier.quote = "'")

#drv <- JDBC("oracle.jdbc.OracleDriver", classPath="G://My Drive//R//BO-PY//ojdbc6.jar", " ")
#MADRAS <- dbConnect(drv,"jdbc:oracle:thin:@dayrhevalplc1.enterprisenet.org:1521/VALPLC01","NRSP_GUEST", "NRSP_GUEST")
MADRAS <- dbConnect(drv=drv
                    , url = "jdbc:oracle:thin:@dayrhevalplc1.enterprisenet.org:1521/VALPLC01"
                    , user = "MADRAS_GUEST"
                    , password = "MADRAS_GUEST")
#CONSULTA DE RESOLUCION

SUM<- paste ("select
             A.msr_tpr_id,
             C.SHO_EXTERNAL_CODE,
             A.msr_mas_id
             FROM madras_data.tmms_ms_resolution A 
             JOIN madras_data.tmms_market_segment B ON A.msr_mas_id=B.mas_id 
             JOIN madras_data.TRSH_SHOP C ON A.MSR_SHO_ID=C.SHO_ID
             where msr_cch_id in ('XCGTSC','XCSVSC','XCHNSC','XCNISC','XCCRSC','XCPASC') 
             and msr_cel_sam_id in (20000798, 20000800, 20000801,20000802,
                                                                        20000803,20000804) 
            AND MSR_TPR_ID BETWEEN 1095 AND 1099 AND MSR_MAS_ID IN (",SUMA,") and msr_sho_id=SHO_ID and msr_mas_id=mas_id")


SUMA1<-dbGetQuery(MADRAS,SUM) #tiendas en los mercados a,b


TOT<-paste ("select
            A.msr_tpr_id,
            C.SHO_EXTERNAL_CODE,
            A.msr_mas_id
            FROM madras_data.tmms_ms_resolution A 
            JOIN madras_data.tmms_market_segment B ON A.msr_mas_id=B.mas_id 
            JOIN madras_data.TRSH_SHOP C ON A.MSR_SHO_ID=C.SHO_ID
            where msr_cch_id in ('XCGTSC','XCSVSC','XCHNSC','XCNISC','XCCRSC','XCPASC') and msr_cel_sam_id in (20000798, 20000800, 20000801,20000802,
                                                                      20000803,20000804) 
            AND MSR_TPR_ID BETWEEN 1095 AND 1099 AND MSR_MAS_ID IN (",TOTAL,") and msr_sho_id=SHO_ID and msr_mas_id=mas_id")

TOTAL1<-dbGetQuery(MADRAS,TOT) #tiendas en el mercado A con A=a,b

dbDisconnect(MADRAS)


##CREACION DE LLAVE
SUMA1$LLAVE<- paste0(SUMA1$MSR_TPR_ID,";",SUMA1$SHO_EXTERNAL_CODE)
TOTAL1$LLAVE<- paste0(TOTAL1$MSR_TPR_ID,";",TOTAL1$SHO_EXTERNAL_CODE)



#pegar source_id
SUMA1 <- SUMA1 %>% mutate(source_id = as.numeric(SHO_EXTERNAL_CODE) - 5700000000)
TOTAL1 <- TOTAL1 %>% mutate(source_id = as.numeric(SHO_EXTERNAL_CODE) - 5700000000)
#IDENTIFICACION DE TIENDAS FALTANTES
faltantes<-sqldf('SELECT "LLAVE" FROM SUMA1 EXCEPT SELECT "LLAVE" FROM TOTAL1')#suma - total
#entran a la suma pero no al total ###entra a TSR pero no al mercado original  #suma=TSR
sobrantes<-sqldf("SELECT LLAVE,source_id FROM TOTAL1 EXCEPT SELECT LLAVE,source_id FROM SUMA1")#total - suma
#original -tsr  
##FALTANTES DEBE SER 0 Y SOBRANTES MUCHAS, SOBRANTES REAL DEBE SER 0 

##################IF TSR STUFF
##original pero no al TSR lo cual está bien deben salir cadenas no TSR
servidor_sms<- "acn052cpsms01"        #Servidor SMS
sms_scan<- "smsscancam"                   #BASE DE DATOS SMS SCAN CURRENT
sms_scan_history="smsscancamHistory"      #BASE DE DATOS SMS SCAN HISTORY
user<- "usr_read"             #Usuario
pass<- "LEeras31"             #Password
connection_sms <- odbcDriverConnect(paste0("driver={SQL Server};Server=",servidor_sms,";uid=",user,";pwd=",pass))


MASTER<- sqlQuery(connection_sms,
                  paste0("SELECT distinct source_id,cadena_de_tiendas
                         FROM ",servidor_sms,".",sms_scan,".dbo.source_master"))

sobrantes <- sobrantes %>% left_join(MASTER,by="source_id")
tsr <- c(2,8,14,29,101,109,201,205,301,317,401,513,602,317,636,956)
sobrantesreal <- sobrantes %>% filter(cadena_de_tiendas %in% tsr) #956 es paralela colonia 317

#cadenas <- sqldf("SELECT DISTINCT cadena_de_tiendas
 #              FROM sobrantes")
#View(cadenas)

#fwrite(sobrantesreal,"sobrantes+.csv")
#shell.exec("sobrantes+.csv")
# dbDisconnect(connection_sms)

#getwd()# #write.xlsx(DIFERENCIAS,file = paste0("G:/My Drive/CIP TRANSITION TEAM/PERU/PRODUCTIVO/VALIDACION M-ADICIONALES/DIFERENCIAS_",TOTAL,".xlsx"), sheetName = "Faltantes", append = T)
# 
# file <- paste0("G:/My Drive/CIP TRANSITION TEAM/PERU/PRODUCTIVO/SCAN/VALIDACION M-ADICIONALES/DIFERENCIAS_",TOTAL,".xlsx",sep="")
# wb <- createWorkbook()
# sheet1 <- createSheet(wb, sheetName="Suma vs Total")
# sheet2 <- createSheet(wb, sheetName="Total vs Suma")
# 
# addDataFrame(DIFERENCIAS, sheet1, row.names=F)
# addDataFrame(DIFERENCIAS1, sheet2, row.names=F)
# 
# saveWorkbook(wb, file)
# 
print(paste0("TSR ES: ", SUMA))
length(faltantes[,1])
length(sobrantesreal[,1])
