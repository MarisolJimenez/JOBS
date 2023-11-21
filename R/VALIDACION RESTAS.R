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

#SUMA <- ginput("PROD \n \n 1")
SUMA<- ""  #PROD
TOTAL<-(1094433)  #POC
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

MADRAS_POC <- dbConnect(drv=drv
                    , url = "jdbc:oracle:thin:@daylvalq01:1521/VALPOCNA"
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
            AND MSR_TPR_ID BETWEEN 990 AND 1111 AND MSR_MAS_ID IN (",SUMA,") and msr_sho_id=SHO_ID and msr_mas_id=mas_id")


SUMA1<-dbGetQuery(MADRAS,SUM) #tiendas en los mercados PRODUCTIVO


TOT<-paste ("select
            A.msr_tpr_id,
            C.SHO_EXTERNAL_CODE,
            A.msr_mas_id
            FROM madras_data.tmms_ms_resolution A 
            JOIN madras_data.tmms_market_segment B ON A.msr_mas_id=B.mas_id 
            JOIN madras_data.TRSH_SHOP C ON A.MSR_SHO_ID=C.SHO_ID
            where msr_cch_id in ('XCGTSC','XCSVSC','XCHNSC','XCNISC','XCCRSC','XCPASC') and msr_cel_sam_id in (288,289,290,291,292,293) 
            AND MSR_TPR_ID BETWEEN 990 AND 1111 AND MSR_MAS_ID IN (",TOTAL,") and msr_sho_id=SHO_ID and msr_mas_id=mas_id")

TOTAL1<-dbGetQuery(MADRAS_POC,TOT) #tiendas en el mercado POC

dbDisconnect(MADRAS)

##CREACION DE LLAVE
SUMA1$LLAVE<- paste0(SUMA1$MSR_TPR_ID,"_",SUMA1$SHO_EXTERNAL_CODE)
TOTAL1$LLAVE<- paste0(TOTAL1$MSR_TPR_ID,"_",TOTAL1$SHO_EXTERNAL_CODE)

#IDENTIFICACION DE TIENDAS FALTANTES
sobrantes<-sqldf('SELECT "LLAVE" FROM SUMA1 EXCEPT SELECT "LLAVE" FROM TOTAL1')#MADRAS - POC
#entraN A PRODUCTIVO Y NO A POC
faltantes<-sqldf('SELECT "LLAVE" FROM TOTAL1 EXCEPT SELECT "LLAVE" FROM SUMA1
                 order by llave')#POC - MADRAS
#View(sobrantes)
fwrite(faltantes,"faltantes+.csv")
fwrite(sobrantes,"sobrantes+.csv")

shell.exec("sobrantes+.csv")
shell.exec("faltantes+.csv")

#write.xlsx(DIFERENCIAS,file = paste0("G:/My Drive/CIP TRANSITION TEAM/PERU/PRODUCTIVO/VALIDACION M-ADICIONALES/DIFERENCIAS_",TOTAL,".xlsx"), sheetName = "Faltantes", append = T)
# 
# file <- paste0("G:/My Drive/CIP TRANSITION TEAM/PERU/PRODUCTIVO/SCAN/VALIDACION M-ADICIONALES/DIFERENCIAS_",TOTAL,".xlsx",sep="")
# wb <- createWorkbook()
# sheet1 <- createSheet(wb, sheetName="Suma vs Total")
# sheet2 <- createSheet(wb, sheetName="Total vs Suma")
# 
# 
# addDataFrame(DIFERENCIAS, sheet1, row.names=F)
# addDataFrame(DIFERENCIAS1, sheet2, row.names=F)
# 
# saveWorkbook(wb, file)
# 
