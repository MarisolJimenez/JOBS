############################Hierarchy Universe Lacing Check#####################################
################################################################################################
#######PREAMBULO########
rm(list=ls())
Sys.setenv(JAVA_HOME='C:/64 bits/sqldeveloper-17.3.1.279.0537-x64/sqldeveloper/jdbc/lib')

if (!require(data.table)) {install.packages("data.table")}
if (!require(fitdistrplus)) {install.packages("fitdistrplus")}
if (!require(tidyr)) {install.packages("tidyr")}
if (!require(xlsx)) {install.packages("xlsx")}
if (!require(sqldf)) {install.packages("sqldf")}
if (!require(dplyr)) {install.packages("dplyr")}
if (!require(RODBC)) {install.packages("RODBC",dependencies = TRUE)}
if (!require(RJDBC)) {install.packages("RJDBC")}
if (!require(rJava)) {install.packages("rJava")}
if (!require(DBI)) {install.packages("DBI")}
if (!require(gWidgets)) {install.packages("gWidgets")}
#if (!require(tidyverse)) {install.packages("tidyverse")}

#rm(list=ls())
#options(guiToolkit = "RGtk2")

library(gWidgets) #Instalar RGtk2 y gWidgetsRGtk2
library(DBI)
library(rJava)
library(RJDBC)
library(RODBC)
library(dplyr)
library(sqldf)
library(xlsx)
#library(tidyverse)
library(tidyr)
library(fitdistrplus)
library(data.table)
memory.limit(20000)

#########INSUMOS#############
#####RUTA DE TRABAJO
#ruta <- "C:/Users/teda7002/Desktop/LACING/INPA_MONT/"
#ruta <- "C:/Users/teda7002/Desktop/LACING/INPA_SCAN/"
rm(list=ls())
ruta <- "C:/Users/jima9001/Desktop/NIELSEN/ARG PROD/SCAN/TSR" #cambiar
#LOS PERIODOS SON TPR_ID
periodo_inicio <- 955 #cambiar
periodo_fin <- 1118 #cambiar
jerarquias <- c(20016858,20016840,20016853,20016841,20016859,20016838,20016846,20016843,
                20016852,20016850,20016855,20016834,20016862,20016863,20016849,20016851,
                20016856,20016836,20016854,20016847,20016848,20016845,20016839,20016842,
                20016860,20016861,20016837,20016832,20016844,20016833,20016835,20016857) #cambiar, sólo poner las jerarquías que se quieren analizar
#jerarquias <- c(20017563,20017564,20017562)
sample <- 	20001506 #cambiar. sample a validar
country <- 'AR' #cambiar
channel <- 'SCA2' #cambiar
cch_id <- paste0(country, channel)
`%notin%` = function(x,y) !(x %in% y)

####################################################################################
################################CONEXIONES_MADRAS##################################

drv <- JDBC(driverClass = "oracle.jdbc.OracleDriver"
            , classPath = "C:/64 bits/sqldeveloper-17.3.1.279.0537-x64/sqldeveloper/jdbc/lib/ojdbc8.jar"       
            , identifier.quote = "'")


#####MADRAS#####
# conn_m <- dbConnect(drv=drv
#                     , url = "jdbc:oracle:thin:@dayrhevalplc1.enterprisenet.org:1521/VALPLC01"
#                     , user = "MADRAS_GUEST"
#                     , password = "MADRAS_GUEST")
# 
#POC
 # conn_m <- dbConnect(drv=drv
 #                     , url = "jdbc:oracle:thin:@daylvalq01:1521/VALPOCNA"
 #                     , user = "MADRAS_GUEST"
 #                     , password = "MADRAS_GUEST")

#####MADRAS_PRODUCCION COLIMBIA######
conn_m_prod <- dbConnect(drv=drv
                         , url = "jdbc:oracle:thin:@oravalplc01scan.enterprisenet.org:1535/VALPLC01"
                         , user = "MADRAS_GUEST"
                         , password = "MADRAS_GUEST")
##Hostname: oravalplc01scan.enterprisenet.org
##Port: 1535 CAMBIO DE SERVIDOR

#periodos <- c(1027,1028)

ISSUES_FINAL <- data.frame()
DIFF_FINAL <- data.frame()

#Para casos especificos deber?n usar el vector de periodos
for (i in periodo_inicio:periodo_fin) {

  ISSUES <- data.frame()
  DIFF <- data.frame()

QUERY <- paste0("select * from (with mr as
(select * from nrsp_v.tmms_ms_resolution
WHERE msr_cch_id IN ('",cch_id,"')
AND MSR_CEL_SAM_ID IN ('",sample,"')
AND msr_tpr_id = ",i," 
),
msr as
(select * from nrsp_v.tmms_ms_resolution
WHERE msr_cch_id IN ('",cch_id,"')
AND MSR_CEL_SAM_ID IN ('",sample,"')
AND msr_tpr_id = ",i," 
)
,CALC AS(SELECT hi.hii_id AS inst,
msu.hiu_mas_id AS seg,
msu.hiu_level AS hiu_lvl,
msu.hiu_hiu_mas_id AS nextup,
hi.hii_description AS instdesc,
decode(sum (mr.MSR_ACV_TURNOVER * mr.MSR_X_FACTOR),NULL,0,sum (mr.MSR_ACV_TURNOVER * mr.MSR_X_FACTOR)) AS XUNI,
decode(sum (MR.MSR_Z_FACTOR),NULL,0,sum (MR.MSR_Z_FACTOR)) zuni,
decode(SUM(SUM(mr.MSR_ACV_TURNOVER * mr.MSR_X_FACTOR))
OVER (PARTITION BY MSU.HIU_HIU_MAS_ID, MSU.HIU_HII_ID),NULL,0,
SUM(SUM(mr.MSR_ACV_TURNOVER * mr.MSR_X_FACTOR))
OVER (PARTITION BY MSU.HIU_HIU_MAS_ID, MSU.HIU_HII_ID))
AS lowx,
decode(sum((SUM(MR.MSR_Z_FACTOR)))
OVER (PARTITION BY MSU.HIU_HIU_MAS_ID, MSU.HIU_HII_ID),NULL,0,
sum((SUM(MR.MSR_Z_FACTOR)))
OVER (PARTITION BY MSU.HIU_HIU_MAS_ID, MSU.HIU_HII_ID))
AS LowZ,                
ms.mas_description AS segdesc         
FROM nrsp_v.tmms_ms_hierarchy_instance hi
JOIN nrsp_v.tmms_ms_hie_usage msu ON (hi.hii_id = msu.hiu_hii_id)
JOIN nrsp_v.tmms_market_segment ms ON (ms.mas_id = MSU.hiu_mas_id)
left outer JOIN MR
ON ( MSU.hiu_mas_id = MR.msr_mas_id
)
WHERE hi.hii_cou_code = '",country,"'
AND msu.hiu_level <> 1
AND msu.HIU_HIU_MAS_ID IS NOT NULL
GROUP BY hi.hii_id,
msu.hiu_mas_id,
msu.hiu_level,
hiu_hii_id,
hiu_hiu_mas_id,
hi.hii_description,
mas_description
ORDER BY hi.hii_id ASC),
UPCAL AS(SELECT hi.hii_id AS inst,
msu.hiu_mas_id AS seg,
msu.hiu_level AS hiu_lvl,
msu.hiu_hiu_mas_id AS nextup,
hi.hii_description AS instdesc,
decode(SUM(mSr.MSR_ACV_TURNOVER * mSr.MSR_X_FACTOR),NULL,0,SUM(mSr.MSR_ACV_TURNOVER * mSr.MSR_X_FACTOR)) UPX,
decode(SUM(MSR.MSR_Z_FACTOR),NULL,0,SUM(MSR.MSR_Z_FACTOR))
AS UPZ       
FROM nrsp_v.tmms_ms_hierarchy_instance hi
JOIN nrsp_v.tmms_ms_hie_usage msu ON (hi.hii_id = msu.hiu_hii_id)
left outer JOIN MSR
ON ( MSU.HIU_HIU_MAS_ID = MSR.msr_mas_id
)
WHERE hi.hii_cou_code = '",country,"'
AND msu.hiu_level <> 1
AND msu.HIU_HIU_MAS_ID IS NOT NULL
GROUP BY hi.hii_id,
msu.hiu_mas_id,
msu.hiu_level,
hiu_hii_id,
hiu_hiu_mas_id,
hi.hii_description
ORDER BY hi.hii_id ASC)
select CALC.INST,CALC.seg,CALC.hiu_lvl,CALC.nextup,CALC.instdesc,CALC.XUNI,CALC.ZUNI,
CALC.LOWX,
CALC.LOWZ,
UPCAL.UPX,UPCAL.UPZ,CALC.SEGDESC,round((UPCAL.upx-LOWX),2) as diffx, round((UPCAL.upz-LOWZ),2) as diffz from CALC, UPCAL
where CALC.INST = UPCAL.INST
and CALC.seg = UPCAL.seg
order by inst) where (diffx <> 0 or diffz <> 0)")

ISSUES <- dbGetQuery(conn=conn_m_prod,statement=QUERY)
ISSUES <- subset(ISSUES,ISSUES$INST %in% jerarquias)

QUERY_2 <- paste0("/* NRSP_V-ALL_MAS */
/* All_MAS */
select * from (with mr as
(select * from nrsp_v.tmms_ms_resolution
WHERE msr_cch_id IN ('",cch_id,"')
AND MSR_CEL_SAM_ID IN ('",sample,"')
AND msr_tpr_id = ",i,"
),
msr as
(select * from nrsp_v.tmms_ms_resolution
WHERE msr_cch_id IN ('",cch_id,"')
AND MSR_CEL_SAM_ID IN ('",sample,"')
AND msr_tpr_id = ",i,"
)
,CALC AS(SELECT hi.hii_id AS inst,
msu.hiu_mas_id AS seg,
msu.hiu_level AS hiu_lvl,
msu.hiu_hiu_mas_id AS nextup,
hi.hii_description AS instdesc,
decode(sum (mr.MSR_ACV_TURNOVER * mr.MSR_X_FACTOR),NULL,0,sum (mr.MSR_ACV_TURNOVER * mr.MSR_X_FACTOR)) AS XUNI,
decode(sum (MR.MSR_Z_FACTOR),NULL,0,sum (MR.MSR_Z_FACTOR)) zuni,
decode(SUM(SUM(mr.MSR_ACV_TURNOVER * mr.MSR_X_FACTOR))
OVER (PARTITION BY MSU.HIU_HIU_MAS_ID, MSU.HIU_HII_ID),NULL,0,
SUM(SUM(mr.MSR_ACV_TURNOVER * mr.MSR_X_FACTOR))
OVER (PARTITION BY MSU.HIU_HIU_MAS_ID, MSU.HIU_HII_ID))
AS lowx,
decode(sum((SUM(MR.MSR_Z_FACTOR)))
OVER (PARTITION BY MSU.HIU_HIU_MAS_ID, MSU.HIU_HII_ID),NULL,0,
sum((SUM(MR.MSR_Z_FACTOR)))
OVER (PARTITION BY MSU.HIU_HIU_MAS_ID, MSU.HIU_HII_ID))
AS LowZ,                
ms.mas_description AS segdesc         
FROM nrsp_v.tmms_ms_hierarchy_instance hi
JOIN nrsp_v.tmms_ms_hie_usage msu ON (hi.hii_id = msu.hiu_hii_id)
JOIN nrsp_v.tmms_market_segment ms ON (ms.mas_id = MSU.hiu_mas_id)
left outer JOIN MR
ON (    MSU.hiu_mas_id = MR.msr_mas_id
)
WHERE hi.hii_cou_code = '",country,"'
AND msu.hiu_level <> 1
AND msu.HIU_HIU_MAS_ID IS NOT NULL
GROUP BY hi.hii_id,
msu.hiu_mas_id,
msu.hiu_level,
hiu_hii_id,
hiu_hiu_mas_id,
hi.hii_description,
mas_description
ORDER BY hi.hii_id ASC),
UPCAL AS(SELECT hi.hii_id AS inst,
msu.hiu_mas_id AS seg,
msu.hiu_level AS hiu_lvl,
msu.hiu_hiu_mas_id AS nextup,
hi.hii_description AS instdesc,
decode(SUM(mSr.MSR_ACV_TURNOVER * mSr.MSR_X_FACTOR),NULL,0,SUM(mSr.MSR_ACV_TURNOVER * mSr.MSR_X_FACTOR)) UPX,
decode(SUM(MSR.MSR_Z_FACTOR),NULL,0,SUM(MSR.MSR_Z_FACTOR))
AS UPZ       
FROM nrsp_v.tmms_ms_hierarchy_instance hi
JOIN nrsp_v.tmms_ms_hie_usage msu ON (hi.hii_id = msu.hiu_hii_id)
left outer JOIN MSR
ON ( MSU.HIU_HIU_MAS_ID = MSR.msr_mas_id
)
WHERE hi.hii_cou_code = '",country,"'
AND msu.hiu_level <> 1
AND msu.HIU_HIU_MAS_ID IS NOT NULL
GROUP BY hi.hii_id,
msu.hiu_mas_id,
msu.hiu_level,
hiu_hii_id,
hiu_hiu_mas_id,
hi.hii_description
ORDER BY hi.hii_id ASC)
select CALC.INST,CALC.seg,CALC.hiu_lvl,CALC.nextup,CALC.instdesc,CALC.XUNI,CALC.ZUNI,
CALC.LOWX,
CALC.LOWZ,
UPCAL.UPX,UPCAL.UPZ,CALC.SEGDESC,round((UPCAL.upx-LOWX),2) as diffx, round((UPCAL.upz-LOWZ),2) as diffz from CALC, UPCAL
where CALC.INST = UPCAL.INST
and CALC.seg = UPCAL.seg
order by inst)")

DIFF <- dbGetQuery(conn=conn_m_prod,statement=QUERY_2)

DIFF$TPR_ID <- i
DIFF_FINAL <- rbind(DIFF_FINAL,DIFF)

if(length(ISSUES[,1]) > 0){
  ISSUES$TPR_ID <- i
  ISSUES_FINAL <- rbind(ISSUES_FINAL,ISSUES)
  }


print(i)

}

#DIFRENECIAS Serán todas las diferencias, incluidas las jerarquías sin lacing, es decir
#con diferencias=0, por lo que hay que enfocarse en el archivo ISSUES.
fwrite(DIFF_FINAL,file=paste(ruta,"\\DIFF_",sample,".csv",sep="") , na = '', sep = ",", row.names = F, quote = F, col.names = T)
fwrite(ISSUES_FINAL,file=paste(ruta,"/ISSUES_",sample,".csv",sep="") , na = '', sep = ",", row.names = F, quote = F, col.names = T)
shell.exec(paste0(ruta,"/ISSUES_",sample,".csv"))



