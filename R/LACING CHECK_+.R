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
if (!require(RODBC)) {install.packages("RODBC")}
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
rm(list=ls())
ruta <- "C:/Users/jima9001/Desktop/NIELSEN/ARG POC/SUMAS"
#LOS PERIODOS SON TPR_ID
periodo_inicio <- 955
periodo_fin <- 1097
##hii to be analyzed
jerarquias <- c(1073649,1073699,1073696,1073697,1073700,1073698,1073701,1073632,1073685,
                1073675,1073662,1073663,1073660,1073709,1073668,1073690,1073667,1073657,
                1073694,1073672,1073673,1073620,1073659,1073671,1073655,1073653,1073624,
                1073633,1073676,1073702,1073651,1073712,1073692,1073703,1073656,1073648,
                1073626,1073658,1073669,1073654,1073695,1073704,1073641,1073625,1073652,
                1073677,1073682,1073683,1073678,1073679,1073680,1073684,1073681,1073691,
                1073661,1073705,1073664,1073706,1073618,1073693,1073622,1073621,1073634,
                1073635,1073637,1073636,1073665,1073623,1073687,1073689,1073686,1073688,
                1073645,1073647,1073643,1073644,1073646,1073666,1073708,1073619,1073640,
                1073627,1073628,1073631,1073630,1073629,1073710,1073711,1073707,1073670,
                1073674,1073639,1073650,1073638,1073642)
sample <- 	991698
country <- 'AR'
channel <- 'SCA1'
cch_id <- paste0(country, channel)
`%notin%` = function(x,y) !(x %in% y)

####################################################################################
################################CONEXIONES_MADRAS##################################

drv <- JDBC(driverClass = "oracle.jdbc.OracleDriver"
            , classPath = "C:/64 bits/sqldeveloper-17.3.1.279.0537-x64/sqldeveloper/jdbc/lib/ojdbc8.jar"       
            , identifier.quote = "'")


#####MADRAS#####
conn_m <- dbConnect(drv=drv
                    , url = "jdbc:oracle:thin:@dayrhevalplc1.enterprisenet.org:1521/VALPLC01"
                    , user = "MADRAS_GUEST"
                    , password = "MADRAS_GUEST")


#####MADRAS_PRODUCCION COLIMBIA######
# conn_m_prod <- dbConnect(drv=drv
#                          , url = "jdbc:oracle:thin:@dayrhevalplc1:1521/VALPLC01"
#                          , user = "MADRAS_GUEST"
#                          , password = "MADRAS_GUEST")

#POC
conn_m_prod <- dbConnect(drv=drv
                         , url = "jdbc:oracle:thin:@daylvalq01:1521/VALPOCNA"
                         , user = "MADRAS_GUEST"
                         , password = "MADRAS_GUEST")

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
###WE ONLY CARE ABOUT ISSUES FINAL
cons <- sqldf("SELECT DISTINC TPR_ID")
fwrite(DIFF_FINAL,file=paste(ruta,"\\DIFF_",sample,".csv",sep="") , na = '', sep = ",", row.names = F, quote = F, col.names = T)
fwrite(ISSUES_FINAL,file=paste(ruta,"/ISSUES_",sample,".csv",sep="") , na = '', sep = ",", row.names = F, quote = F, col.names = T)




