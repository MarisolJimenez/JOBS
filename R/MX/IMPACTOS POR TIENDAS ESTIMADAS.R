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

##CAMBIAR PARAMETROS DEPENDIENDO DEL INDICE PERIODO
indice <- 45
TIENDAS_ESTIMADAS <- read.csv("C:/Users/jima9001/Desktop/MX/HOME/TIENDAS ESTIMADAS AGO20 HOME/TIENDAS_ESTIMADAS+3.csv")%>%
                     filter(INDICE=="45")%>%
                     select(SHO_EXTERNAL)%>%
                     mutate(SHO_EXTERNAL=as.character(SHO_EXTERNAL), SAMPLE=0)
#shell.exec("C:/Users/jima9001/Desktop/MX/HOME/TIENDAS ESTIMADAS MAY20 HOME.csv")
RUTA <-"C:/Users/jima9001/Desktop/MX/HOME/TIENDAS ESTIMADAS AGO20 HOME/"
NOM <- "IMPACTOS_ESTIMADAS_HOME_AGO_+3"
CANAL <- "MXMONT"
var <- 1
var_fds <- 1
PERIODOS <- c(1132)


CATEGORIA <- "51361246,51329969,53412130,52135816,72550365,51936884,53138344,51101034,53289076,53141596,72550364,51536990"
##H&B 51530695,51537003,53414021,51100977,52403082,51936996,51934722,53288711,51810289,52402058,52403600,52140298,51810732,53139051,53288342,52689276,52400846,52688617,54005627
##PHARMA 53412062,99116971,54148630,72550410,51935644,51535314,61589667,51932586,53694829,53501171,72550387,51935499
##HOME 51361246,51329969,53412130,52135816,72550365,51936884,53138344,51101034,53289076,53141596,72550364,51536990
##BEVERAGES 51161927,51161927,72550452,52690043,53139171,70294858,70294861
##CONFECTIONERY 50685167,51807788,53138320,52136313,53287331,54147044,53141874
##FOOD 51807338,51933966,51148549,51329974,52401419,53414827,53287497,53900573,51309150,51329940,51809797,53502671,53287484,52400875,51936167,52140867,53412012,51810262,53141610,52690130,53139827,53141132,53139113,51536927,52401988,52134784,61917141,51807339,51329952
##DAIRY 51536920,63315988,51532936,51148455,51933317,63184419,52687667,72550406
##DIAPERS 51100952
MERCADO <-"1216147,1216148,1216146,1216163,1216150,1216168,1216155,1216160,1216159,1216158,1216151,1216178,1216179,1216153,1216162,1216149,1216177,1216166,1216154,1216176,1216161,1216174,1216171,1216173,1216172,1216165,1216167"
##H&B 1178534,1178536,1178535,1178560,1178531,1178564,1178546,1178555,1178556,1178557,1178537,1178551,1178550,1178547,1178558,1178532,1178552,1178562,1178545,1178549,1178559,1178541,1178542,1178543,1178540,1178565,1178563,1178689,1178687,1178684,1178686,1178685,1178688
##PHARMA 1318715,1318713,1318714,1318691,1318709,1318696,1318700,1318689,1318688,1318690,1318710,1318706,1318704,1318702,1318693,1318716,1318707,1318695,1318701,1318705,1318692,1318685,1318684,1318682,1318683,1318698,1318697,1318668,1318664,1318665,1318669,1318667,1318666
##HOME 1216147,1216148,1216146,1216163,1216150,1216168,1216155,1216160,1216159,1216158,1216151,1216178,1216179,1216153,1216162,1216149,1216177,1216166,1216154,1216176,1216161,1216174,1216171,1216173,1216172,1216165,1216167
##BEVERAGES 1220849,1220848,1220850,1220824,1220837,1220827,1220830,1220842,1220843,1220841,1220838,1220836,1220834,1220832,1220822,1220839,1220833,1220829,1220831,1220835,1220821,1220847,1220846,1220844,1220845,1220826,1220828,1220852,1220856,1220854,1220855,1220853,1220857
##CONFECTIONERY 1178645,1178647,1178646,1178641,1178649,1178669,1178652,1178640,1178639,1178638,1178643,1178662,1178664,1178651,1178636,1178648,1178661,1178667,1178653,1178663,1178635,1178656,1178659,1178657,1178658,1178666,1178668,1178678,1178675,1178676,1178679,1178674,1178677
##FOOD 1178966,1178964,1178965,1178973,1178960,1178957,1178951,1178970,1178971,1178969,1178962,1178979,1178978,1178953,1178974,1178961,1178977,1178955,1178952,1178976,1178972,1178947,1178948,1178946,1178949,1178958,1178956
##DAIRY 1220265,1220264,1220263,1220268,1220261,1220287,1220256,1220270,1220272,1220271,1220266,1220278,1220276,1220257,1220273,1220260,1220279,1220289,1220258,1220277,1220274,1220283,1220285,1220284,1220282,1220288,1220290
##DIAPERS 1404641,1404642,1404643,1404665,1404639,1404652,1404671,1404663,1404664,1404662,1404638,1404658,1404657,1404669,1404666,1404637,1404656,1404653,1404670,1404659,1404667,1404647,1404649,1404646,1404648,1404651,1404654,1404679,1404683,1404678,1404681,1404680,1404682

FDS_TOT <- c(1123396,1123398,1123397,1123399,1123400,1121286,1121313,1121261,1121262,
             1123404,1123406,1123402,1121308,1121271,1121284,1121285,1123238,1123230,
             1121290,1121291,1123265)
##H&B 1121288,1121322,1121235,1121323,1126309,1121304,1121333,1123325,1121325,1123247,1121295,1121326,1123216,1125305,1121334,1121306,1121250,1121327,1121328,1121299,1121263,1125386,1123244,1123240,1121321
##PHARMA 1121330,1121331,1121332,1121329,1121310,1121311,1123224,1121238,1123255,1121272,1121274,1125811,1126316,1126627,1126443,1123258,1121319,1121256
##HOME 1123396,1123398,1123397,1123399,1123400,1121286,1121313,1121261,1121262,1123404,1123406,1123402,1121308,1121271,1121284,1121285,1123238,1123230,1121290,1121291,1123265
##BEVERAGES 1121292,1121293,1121315,1123212,1123213,1123214,1121245,1121268
##CONFECTIONERY 1121279,1121280,1121278,1125316,1126300,1121282,1123384,1123251,1123250,1126291,1125841,1123392,1123220
##FOOD 1121257,1123222,1121243,1121305,1121259,1121260,1121298,1126244,1121297,1123223,1121283,1123226,1126331,1121289,1121273,1121301,1121300,1121302,1121275,1123246,1123231,1123232,1121303,1121265,1121264,1123237,1123233,1121269,1121267,1121266,1123235,1123234,1123245,1121270,1123218,1121276,1123219
##DAIRY 1121237,1126614,1121287,1123229,1121318,1126492,1123241,1123380
##DIAPERS 1126693,1159983 


SHO_EXTERNAL <- dbGetQuery(conn_m, "SELECT SHO_ID, SHO_EXTERNAL_CODE AS SHO_EXTERNAL
                           FROM MADRAS_DATA.TRSH_SHOP
                           WHERE SHO_CCH_COU_CODE = 'MX' 
                           AND (SHO_EXTERNAL_CODE LIKE '230%'
                           OR SHO_EXTERNAL_CODE LIKE '231%'
                           OR SHO_EXTERNAL_CODE LIKE '233%'
                           OR SHO_EXTERNAL_CODE LIKE '239%')") 


FABRICANTE <- paste0("SELECT B.F_NCV_NAN_KEY, A.AC_CHARVALUETAG AS FABRICANTE
                     FROM VLDIMDB_MX.CHARVALS A
                     JOIN VLDIMDB_MX.NANCHARVALS B on (A.NC_CHARID=B.NC_CHARID and a.NC_CHARVALUEID=b.NC_CHARVALUEID)
                     WHERE  a.NC_CHARID = 56982
                     AND B.F_NCV_NAN_KEY in (SELECT DISTINCT B.F_NCV_NAN_KEY
                     FROM VLDIMDB_MX.CHARVALS A
                     JOIN VLDIMDB_MX.NANCHARVALS B on (A.NC_CHARID=B.NC_CHARID and a.NC_CHARVALUEID=b.NC_CHARVALUEID)
                     WHERE A.NC_CHARID =57153)") 
##AND A.NC_CHARVALUEID IN (",CATEGORIA,"))")

FABRICANTE <- dbGetQuery(conn_s, FABRICANTE)

MARCA <- paste0("SELECT B.F_NCV_NAN_KEY, A.AC_CHARVALUETAG AS MARCA
                FROM VLDIMDB_MX.CHARVALS A
                JOIN VLDIMDB_MX.NANCHARVALS B on (A.NC_CHARID=B.NC_CHARID and a.NC_CHARVALUEID=b.NC_CHARVALUEID)
                WHERE  a.NC_CHARID = 56985
                AND B.F_NCV_NAN_KEY in (SELECT DISTINCT B.F_NCV_NAN_KEY
                FROM VLDIMDB_MX.CHARVALS A
                JOIN VLDIMDB_MX.NANCHARVALS B on (A.NC_CHARID=B.NC_CHARID and a.NC_CHARVALUEID=b.NC_CHARVALUEID)
                WHERE A.NC_CHARID =57153 
                AND A.NC_CHARVALUEID IN (",CATEGORIA,"))")

MARCA <- dbGetQuery(conn_s, MARCA)


SUBMARCA <- paste0("SELECT B.F_NCV_NAN_KEY, A.AC_CHARVALUETAG AS SUBMARCA
                   FROM VLDIMDB_MX.CHARVALS A
                   JOIN VLDIMDB_MX.NANCHARVALS B on (A.NC_CHARID=B.NC_CHARID and a.NC_CHARVALUEID=b.NC_CHARVALUEID)
                   WHERE  a.NC_CHARID = 56988
                   AND B.F_NCV_NAN_KEY in (SELECT DISTINCT B.F_NCV_NAN_KEY
                   FROM VLDIMDB_MX.CHARVALS A
                   JOIN VLDIMDB_MX.NANCHARVALS B on (A.NC_CHARID=B.NC_CHARID and a.NC_CHARVALUEID=b.NC_CHARVALUEID)
                   WHERE A.NC_CHARID =57153 
                   AND A.NC_CHARVALUEID IN (",CATEGORIA,"))")

SUBMARCA <- dbGetQuery(conn_s, paste0("SELECT B.F_NCV_NAN_KEY, A.AC_CHARVALUETAG AS SUBMARCA
                                              FROM VLDIMDB_MX.CHARVALS A
                                              JOIN VLDIMDB_MX.NANCHARVALS B on 
                                              (A.NC_CHARID=B.NC_CHARID and a.NC_CHARVALUEID=b.NC_CHARVALUEID)
                                       WHERE  a.NC_CHARID = 56988
                                       AND B.F_NCV_NAN_KEY in (SELECT DISTINCT B.F_NCV_NAN_KEY
                                       FROM VLDIMDB_MX.CHARVALS A
                                      JOIN VLDIMDB_MX.NANCHARVALS B on (A.NC_CHARID=B.NC_CHARID and a.NC_CHARVALUEID=b.NC_CHARVALUEID)
                                      WHERE A.NC_CHARID =57153 
                                      AND A.NC_CHARVALUEID IN (",CATEGORIA,"))"))


DESCRIPCION <- paste0("SELECT B.F_NCV_NAN_KEY, A.AC_CHARVALUETAG AS DESCRIPCION
                      FROM VLDIMDB_MX.CHARVALS A
                      JOIN VLDIMDB_MX.NANCHARVALS B on (A.NC_CHARID=B.NC_CHARID and a.NC_CHARVALUEID=b.NC_CHARVALUEID)
                      WHERE  a.NC_CHARID = 65890
                      AND B.F_NCV_NAN_KEY in (SELECT DISTINCT B.F_NCV_NAN_KEY
                      FROM VLDIMDB_MX.CHARVALS A
                      JOIN VLDIMDB_MX.NANCHARVALS B on (A.NC_CHARID=B.NC_CHARID and a.NC_CHARVALUEID=b.NC_CHARVALUEID)
                      WHERE A.NC_CHARID =57153 
                      AND A.NC_CHARVALUEID IN (",CATEGORIA,"))")

DESCRIPCION <- dbGetQuery(conn_s, DESCRIPCION)


for (PERIODO in c(get("PERIODOS")))
{
  TMMS_1 <- dbGetQuery(conn_m, paste0("SELECT DISTINCT MSR_TPR_ID, MSR_MAS_ID, B.MAS_DESCRIPTION, 
                                       MSR_CEL_ID, D.CEL_DESCRIPTION, C.CIN_ACV_TURNOVER, MSR_SHO_ID, 
                                       MSR_ACV_TURNOVER, MSR_X_FACTOR, MSR_Z_FACTOR
                                       FROM MADRAS_DATA.TMMS_MS_RESOLUTION A
                                      LEFT JOIN MADRAS_DATA.TMMS_MARKET_SEGMENT B ON A.MSR_MAS_ID = B.MAS_ID
                                      LEFT JOIN MADRAS_DATA.TMXP_CELL_INDUSTRY C ON A.MSR_CEL_ID = C.CIN_CEL_ID AND A.MSR_TPR_ID = C.CIN_TPR_ID
                                      LEFT JOIN MADRAS_DATA.TMXP_CELL D ON A.MSR_CEL_ID = D.CEL_ID
                                      WHERE MSR_TPR_ID  = ",PERIODO," 
                                      AND MSR_MAS_ID IN (", MERCADO,")"))
  
  TMMS_1 <- left_join(TMMS_1, SHO_EXTERNAL, by=c("MSR_SHO_ID"="SHO_ID"))
  
  for (FDS in c(get("FDS_TOT")))
  {
    DWH <- paste0("SELECT DWH_TPR_ID,DWH_SHO_ID,DWH_ITM_ID, DWH_ORIGINAL_SALES_VOLUME AS VENTAS_UNIDAD_ORIG, DWH_ORIGINAL_SALES_VALUE AS VENTAS_VALOR_ORIG,DWH_ORIGINAL_PRICE AS PRECIO_ORIG,DWH_CORRECTED_SALES_VOLUME AS VENTAS_UNIDAD, DWH_CORRECTED_SALES_VALUE AS VENTAS_VALOR,DWH_CORRECTED_PRICE AS PRECIO, DWH_ADD_FACT_04 AS COMPRAS, DWH_ADD_FACT_07 AS INVENTARIO
                  FROM NRSP_V.TPDW_DATAWAREHOUSE WHERE 
                  DWH_CCH_ID IN ('",CANAL,"')
                  AND DWH_TPR_ID IN (",PERIODO,")
                  AND DWH_ITM_ID IN (
                  Select distinct nan_key from 
                  nrsp_v.VRAG_FDS_FLAT_HIE_PRD 
                  where fds_id IN (",FDS,") and lev=2
                  union 
                  SELECT bap_itm_id_holds FROM nrsp_IMDB02.trpr_banded_pack
                  WHERE BAP_ITM_ID IN (SELECT fhp_itm_id
                  FROM nrsp_res.tecl_flat_hie_prd a
                  WHERE a.fhp_run_id in (", FDS, ")))")
    
    DWH<- dbGetQuery(conn_m,DWH)%>%
      mutate(FDS=get("FDS"))
    
    
    DWH <- left_join(DWH, FABRICANTE,by=c("DWH_ITM_ID"="F_NCV_NAN_KEY"))%>%
      left_join(MARCA,by=c("DWH_ITM_ID"="F_NCV_NAN_KEY"))%>%
      left_join(SUBMARCA,by=c("DWH_ITM_ID"="F_NCV_NAN_KEY"))%>%
      left_join(DESCRIPCION,by=c("DWH_ITM_ID"="F_NCV_NAN_KEY"))
    
    
    if(var_fds==1) {
      DWH_FINAL <- DWH
      var_fds <- var_fds+1  
    }else{
      DWH_FINAL <- rbind(DWH_FINAL, DWH)    
    }
    print(FDS)
  }  
  
  if(var==1) {
    TMMS <- TMMS_1
    var <- var+1  
  }else{
    TMMS <- rbind(TMMS, TMMS_1)    
  }                       
  print(PERIODO)
  
}


##RECALCULO DE FACTORES

APOYO_REC <- left_join(TMMS, TIENDAS_ESTIMADAS, by=c("SHO_EXTERNAL"))%>%
             mutate(SAMPLE=ifelse(is.na(SAMPLE),1,0),
                    ACV_NEW=MSR_ACV_TURNOVER*SAMPLE)

APOYO_RESUMEN <- group_by(APOYO_REC, MSR_CEL_ID)%>%
                 summarise(X_OLD=max(MSR_X_FACTOR),ACV_U=max(CIN_ACV_TURNOVER), 
                           ACV_OLD=sum(MSR_ACV_TURNOVER), ACV_NEW=sum(ACV_NEW))%>%
                 mutate(X_VAL=ACV_U/ACV_OLD, X_NEW=ACV_U/ACV_NEW, VAL=X_OLD-X_VAL)

CASOS_ERROR <- filter(APOYO_RESUMEN, VAL != 0)
print(CASOS_ERROR)

APOYO_MUTATE <- select(APOYO_RESUMEN, MSR_CEL_ID, X_NEW)
                

CONSOL <- left_join(DWH_FINAL, TMMS, by=c("DWH_TPR_ID"="MSR_TPR_ID", "DWH_SHO_ID"="MSR_SHO_ID"))%>%
          left_join(APOYO_MUTATE, by=c("MSR_CEL_ID"))%>%
          filter(!is.na(MSR_CEL_ID))%>%
          left_join(TIENDAS_ESTIMADAS, by=c("SHO_EXTERNAL"))%>%
          mutate(SAMPLE=ifelse(is.na(SAMPLE),1,SAMPLE),
          VENTAS_VALOR_PROYECTADAS=MSR_X_FACTOR*VENTAS_VALOR,
          VENTAS_UNIDAD_PROYECTADAS=MSR_X_FACTOR*VENTAS_UNIDAD,
          VENTAS_VALOR_PROYECTADAS_NEW=X_NEW*VENTAS_VALOR*SAMPLE,
          VENTAS_UNIDAD_PROYECTADAS_NEW=X_NEW*VENTAS_UNIDAD*SAMPLE)

RESUMEN_EXPORTAR <- group_by(CONSOL,MSR_MAS_ID, MAS_DESCRIPTION, MSR_CEL_ID, CEL_DESCRIPTION,
                             FDS, FABRICANTE)%>%
                    summarise(VENTAS_VALOR_PROYECTADAS=sum(VENTAS_VALOR_PROYECTADAS),
                              VENTAS_UNIDAD_PROYECTADAS=sum(VENTAS_UNIDAD_PROYECTADAS),
                              VENTAS_VALOR_PROYECTADAS_NEW=sum(VENTAS_VALOR_PROYECTADAS_NEW),
                              VENTAS_UNIDAD_PROYECTADAS_NEW=sum(VENTAS_UNIDAD_PROYECTADAS_NEW),
                              IMPACTO=min(SAMPLE))

RESUMEN_EXPORTAR <- RESUMEN_EXPORTAR %>% mutate(INDICE = indice)
fds_cat <- read.csv("C:/Users/jima9001/Desktop/MX/HOME/TIENDAS ESTIMADAS JUL20 HOME/FDS-CAT.csv")
RESUMEN_EXPORTAR <- RESUMEN_EXPORTAR %>% left_join(fds_cat, by =c("FDS"))
RESUMEN_EXPORTAR <- RESUMEN_EXPORTAR[,c(1,12,2,3,4,5,13,6,7,8,9,10,11)]

fwrite(RESUMEN_EXPORTAR,paste0(RUTA,NOM,".csv"), col.names = TRUE)
shell.exec(paste0(RUTA,NOM,".csv"))
