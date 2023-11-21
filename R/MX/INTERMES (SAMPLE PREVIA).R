rm(list = ls(all=T))

library(RJDBC)
library(dplyr)
library(sqldf)
library(xlsx)
library(tidyr)
library(data.table)


driver <- JDBC(driverClass = "oracle.jdbc.OracleDriver"
               , classPath = "C:/64 bits/sqldeveloper-17.3.1.279.0537-x64/sqldeveloper/jdbc/lib/ojdbc8.jar"       
               , identifier.quote = "'")  

conn_m <- dbConnect(drv = driver
                    , url = "jdbc:oracle:thin:@oravalplc01scan.enterprisenet.org:1535/VALPLC01"
                    , user = "NRSP_GUEST"
                    , password = "NRSP_GUEST")

conn_s <- dbConnect(drv = driver
                    , url = "jdbc:oracle:thin:@orasirplc01scan.enterprisenet.org:1521/SIRPLC01"
                    , user = "SIRVALREAD_MX"
                    , password = "Zl7KIAWz")


CATEGORIA <- "51361246,51329969,53412130,52135816,72550365,51936884,53138344,51101034,53289076,53141596,72550364,51536990"
##DIAPERS "51100952"
##Dairy  "51536920,63315988,51532936,51148455,51933317,63184419,52687667,72550406"
##H&B "51530695,51537003,53414021,51100977,52403082,51936996,51934722,53288711,51810289,52402058,52403600,52140298,51810732,53139051,53288342,52689276,52400846,52688617,54005627"
##BEVERAGE 51161927,51161927,72550452,52690043,52690043,53139171,70294858,70294861
MERCADOS <-"1216147,1216148,1216146,1216163,1216150,1216168,1216155,1216160,1216159,1216158,1216151,1216178,1216179,1216153,1216162,1216149,1216177,1216166,1216154,1216176,1216161,1216174,1216171,1216173,1216172,1216165,1216167"
## DIAPERS "1404678,1404679,1404680,1404681,1404682,1404683,1404637,1404638,1404639,1404641,1404642,1404643,1404646,1404647,1404648,1404649,1404651,1404652,1404653,1404654,1404656,1404657,1404658,1404659,1404662,1404663,1404664,1404665,1404666,1404667,1404669,1404670,1404671"
## DAIRY "1220265,1220264,1220263,1220268,1220261,1220287,1220256,1220270,1220272,1220271,1220266,1220278,1220276,1220257,1220273,1220260,1220279,1220289,1220258,1220277,1220274,1220283,1220285,1220284,1220282,1220288,1220290"
##H&B "1178534,1178536,1178535,1178560,1178531,1178564,1178546,1178555,1178556,1178557,1178537,1178551,1178550,1178547,1178558,1178532,1178552,1178562,1178545,1178549,1178559,1178541,1178542,1178543,1178540,1178565,1178563,1178689,1178687,1178684,1178686,1178685,1178688"
ULT_PER <- 1127
SAMPLE<- 1000302
INDUSTRY_<- 1000242
INDICE <- "HOME"


SHO_EXTERNAL <- dbGetQuery(conn_m, "SELECT SHO_ID, SHO_EXTERNAL_CODE AS TIENDA
                             FROM MADRAS_DATA.TRSH_SHOP
                             WHERE SHO_CCH_COU_CODE = 'MX' 
                             AND (SHO_EXTERNAL_CODE LIKE '230%'
                             OR SHO_EXTERNAL_CODE LIKE '231%'
                             OR SHO_EXTERNAL_CODE LIKE '233%'
                             OR SHO_EXTERNAL_CODE LIKE '239%')") 

var <- 1

Tiempo_tot<-Sys.time()
##PARA SACAR LAS VENTAS Y SUMA DE FACTORES, CORRER 14 PERIODOS##
##997,1001,1005,1010,1014,1018,1023,1027,1032,1036,1040,1045,1049,1053
##1053,1058,1062,1066,1071,1075,1079,1084,1088,1092,1097,1101,1105,1123
for(period in c(1071,1075,1079,1084,1088,1092,1097,1101,1105,1110,1114,1119,1123,1127))
{

TMMS_1 <- dbGetQuery(conn_m, paste0("SELECT DISTINCT MSR_TPR_ID, MSR_MAS_ID, B.MAS_DESCRIPTION, MSR_CEL_ID, CEL_DESCRIPTION, MSR_SHO_ID, MSR_ACV_TURNOVER, MSR_X_FACTOR, MSR_Z_FACTOR
                            FROM MADRAS_DATA.TMMS_MS_RESOLUTION A
                            LEFT JOIN MADRAS_DATA.TMMS_MARKET_SEGMENT B ON A.MSR_MAS_ID = B.MAS_ID
                            LEFT JOIN MADRAS_DATA.TMXP_CELL C ON A.MSR_CEL_ID = C.CEL_ID
                            WHERE MSR_TPR_ID  = ",period," 
                            AND MSR_MAS_ID IN (", MERCADOS,")"))

TMMS_1 <- left_join(TMMS_1, SHO_EXTERNAL, by=c("MSR_SHO_ID"="SHO_ID"))

SIRVAL_1 <- dbGetQuery(conn_s, paste0("SELECT nc_periodid-989 as PERIODO,AC_NSHOPID AS TIENDA,a.NC_CHARVALUEID as CATEGORIA, a.AC_CHARVALUETAG AS CATEG_DESC, SUM(NC_SLOT4) AS VENTAS_UNIDAD,  SUM(NC_SLOT4*NC_SLOT3) AS VENTAS_VALOR 
                              from VLDRAWDATA_MX.RAWDATA_MM C
                              JOIN VLDIMDB_MX.NANCHARVALS B ON B.F_NCV_NAN_KEY=C.F_NAN_KEY
                              JOIN VLDIMDB_MX.CHARVALS A On (A.NC_CHARID=B.NC_CHARID and a.NC_CHARVALUEID=b.NC_CHARVALUEID)
                              WHERE AC_DTGROUP LIKE 'AUDIT_DTYPE' and nc_periodid = ",period,"+989 and A.NC_CHARID = 57153 AND F_NAN_KEY IN (SELECT
                                                                                                                                   B.F_NCV_NAN_KEY
                                                                                                                                   FROM VLDIMDB_MX.CHARVALS A
                                                                                                                                   JOIN VLDIMDB_MX.NANCHARVALS B on (A.NC_CHARID=B.NC_CHARID and a.NC_CHARVALUEID=b.NC_CHARVALUEID)
                                                                                                                                   WHERE A.NC_CHARID =57153 
                                                                                                                                   AND A.NC_CHARVALUEID IN (",CATEGORIA,"))
                              GROUP BY NC_PERIODID, AC_NSHOPID,a.NC_CHARVALUEID, a.AC_CHARVALUETAG")) 

if(var==1) {
                 TMMS <- TMMS_1
                 SIRVAL <- SIRVAL_1
                 var <- var+1  
                 }else{
                 TMMS <- rbind(TMMS, TMMS_1)    
                 SIRVAL <- rbind(SIRVAL, SIRVAL_1)
            }                       
print(period)
}

CONSOL <- left_join(SIRVAL, TMMS, by=c("PERIODO"="MSR_TPR_ID", "TIENDA"))%>%
          mutate(VENTAS_VALOR_PROYECTADAS=VENTAS_VALOR*MSR_X_FACTOR)%>%
          select(PERIODO,TIENDA,VENTAS_UNIDAD,VENTAS_VALOR,CATEGORIA,MSR_CEL_ID,MSR_MAS_ID,MAS_DESCRIPTION,CEL_DESCRIPTION,MSR_SHO_ID,MSR_ACV_TURNOVER,MSR_X_FACTOR,MSR_Z_FACTOR,VENTAS_VALOR_PROYECTADAS)%>%
          arrange(MSR_CEL_ID)

RESUMEN <- group_by(CONSOL,PERIODO, MSR_MAS_ID, MAS_DESCRIPTION, CATEGORIA)%>%
           summarise(VENTAS_VALOR=sum(VENTAS_VALOR), VENTAS_VALOR_PROYECTADAS=sum(VENTAS_VALOR_PROYECTADAS))

SUMA_FACTORES<- group_by(TMMS, MSR_TPR_ID, MSR_MAS_ID, MAS_DESCRIPTION, MSR_CEL_ID, CEL_DESCRIPTION)%>%
                summarise(TIENDAS=n(), ACV_MUESTRA=sum(MSR_ACV_TURNOVER), SUMA_X=sum(MSR_X_FACTOR), SUMA_Z=sum(MSR_Z_FACTOR))
           
TIEMPO_TOTAL<-Sys.time()-Tiempo_tot

print(TIEMPO_TOTAL)


##PARA ACTUALIZAR LAS PESTAñAS DE RESUMEN Y ESTATUS_PREV##

INDUSTRY <- dbGetQuery(conn_m,paste0("SELECT DISTINCT CIN_CEL_ID AS CELDA, CIN_ACV_TURNOVER AS ACV_UNIV, CIN_NUMBER_OF_SHOPS AS N_UNIV,
                       CIN_X_FACTOR AS XF, CIN_Z_FACTOR AS ZF
                       FROM MADRAS_DATA.TMXP_CELL_INDUSTRY
                       WHERE CIN_CEL_SAM_ID = ",SAMPLE,"
                       AND CIN_TPR_ID = ",ULT_PER,"
                       AND CIN_TYPE = 'CE'"))

CELDA_DESCRIPCION <- dbGetQuery(conn_m, paste0("SELECT CEL_ID AS CELDA, CEL_DESCRIPTION AS NOMBRE_CELDA
                                FROM MADRAS_DATA.TMXP_CELL
                                WHERE CEL_SAM_ID = ",SAMPLE))

MERCADO_DESCRIPCION <- dbGetQuery(conn_m, paste0("SELECT A.MSC_CEL_ID AS CELDA, B.MAS_DESCRIPTION AS MERCADO
                                  FROM MADRAS_DATA.TMMS_MARKET_SEGMENT_CELL A 
                                  LEFT JOIN MADRAS_DATA.TMMS_MARKET_SEGMENT B ON A.MSC_MAS_ID = B.MAS_ID
                                  WHERE MSC_MAS_ID IN (",MERCADOS,")
                                  AND MSC_TPR_ID =", ULT_PER))


INSUMO_CELDA <- left_join(INDUSTRY, CELDA_DESCRIPCION, by=c("CELDA"))%>%
                left_join(MERCADO_DESCRIPCION, by=c("CELDA"))%>%
                select(CELDA, MERCADO, NOMBRE_CELDA, ACV_UNIV, N_UNIV, XF, ZF)
                


CONTENT <- dbGetQuery(conn_m,paste0("SELECT B.SHO_EXTERNAL_CODE AS SOURCE_ID, A.CCO_CEL_ID AS CELL_ID, C.SHI_ACV_TURNOVER
                             FROM MADRAS_DATA.TMXP_CELL_CONTENT A 
                             LEFT JOIN MADRAS_DATA.TRSH_SHOP B ON A.CCO_SHO_ID = B.SHO_ID
                             LEFT JOIN MADRAS_DATA.TMXP_SHOP_INDUSTRY C ON A.CCO_SHO_ID = C.SHI_SHO_ID AND A.CCO_TPR_ID = C.SHI_WEEK_EFFECTIVE_FROM
                             WHERE CCO_TPR_ID = ",ULT_PER,"
                             AND CCO_CEL_SAM_ID = ",SAMPLE, "
                             AND SHI_IND_ID = ",INDUSTRY_,"
                             AND CCO_TYPE = 'CE'"))

CONTENT<- mutate(CONTENT, USABILIDAD_PREV=1)
          
fwrite(CONSOL, paste0("G:/Shared drives/RETAIL REGULAR/PRODUCCION/SAMPLE INSPECTION/45 - HOME/2020020 - AGO20/FICHEROS PREVIA/R/CONSOL.csv"))
fwrite(RESUMEN, paste0("G:/Shared drives/RETAIL REGULAR/PRODUCCION/SAMPLE INSPECTION/45 - HOME/2020020 - AGO20/FICHEROS PREVIA/R/RESUMEN_VENTAS.csv"))
fwrite(SUMA_FACTORES, paste0("G:/Shared drives/RETAIL REGULAR/PRODUCCION/SAMPLE INSPECTION/45 - HOME/2020020 - AGO20/FICHEROS PREVIA/R/SUMA_FACTORES.csv"))
fwrite(INSUMO_CELDA, paste0("G:/Shared drives/RETAIL REGULAR/PRODUCCION/SAMPLE INSPECTION/45 - HOME/2020020 - AGO20/FICHEROS PREVIA/R/RESUMEN.csv"))
fwrite(CONTENT, paste0("G:/Shared drives/RETAIL REGULAR/PRODUCCION/SAMPLE INSPECTION/45 - HOME/2020020 - AGO20/FICHEROS PREVIA/R/ESTATUS_PREV.csv"))
