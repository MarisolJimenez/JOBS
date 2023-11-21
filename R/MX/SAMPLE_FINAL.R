  rm(list = ls(all=T))
  
  library(RJDBC)
  library(dplyr)
  library(sqldf)
  library(xlsx)
  library(tidyr)
  library(data.table)
  library(gWidgets)
  library(gWidgets2)
  library(gWidgets2RGtk2)
  
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
  
  
  
  CATEGORIA <- "51361246,72550365,53289076,53141596" 
  MERCADOS <- "1216147,1216148,1216146,1216163,1216150,1216168,1216155,1216160,1216159,1216158,1216151,1216178,1216179,
              1216153,1216162,1216149,1216177,1216166,1216154,1216176,1216161,1216174,1216171,1216173,1216172,1216165,
                1216167" 
  PRE_PER <- as.numeric(ginput("Ingrese el periodo previo")) #CAMBIAR
  ULT_PER <- PRE_PER+5  #CAMBIAR, REVISAR SI ESTA OK PERIODO, PUEDE SER Q SEA MÁS 5
  a <- c(PRE_PER,ULT_PER)
  SAMPLE<- 1000302  #CAMBIAR
  INDUSTRY_<- 1000242  #CAMBIAR
  INDICE <- "HOME"  #CAMBIAR
  
  SHO_EXTERNAL <- dbGetQuery(conn_m, "SELECT SHO_ID, SHO_EXTERNAL_CODE AS TIENDA
                               FROM MADRAS_DATA.TRSH_SHOP
                               WHERE SHO_CCH_COU_CODE = 'MX' 
                               AND (SHO_EXTERNAL_CODE LIKE '230%'
                               OR SHO_EXTERNAL_CODE LIKE '231%'
                               OR SHO_EXTERNAL_CODE LIKE '233%'
                               OR SHO_EXTERNAL_CODE LIKE '239%')") 
  
  var <- 1
  
  Tiempo_tot<-Sys.time()

  
  for(period in a) #CAMBIAR
  {
  
  TMMS_1 <- dbGetQuery(conn_m, paste0("SELECT DISTINCT MSR_TPR_ID, MSR_MAS_ID, B.MAS_DESCRIPTION, MSR_CEL_ID, CEL_DESCRIPTION, MSR_SHO_ID, MSR_ACV_TURNOVER, MSR_X_FACTOR, MSR_Z_FACTOR
                              FROM MADRAS_DATA.TMMS_MS_RESOLUTION A
                              LEFT JOIN MADRAS_DATA.TMMS_MARKET_SEGMENT B ON A.MSR_MAS_ID = B.MAS_ID
                              LEFT JOIN MADRAS_DATA.TMXP_CELL C ON A.MSR_CEL_ID = C.CEL_ID
                              WHERE MSR_TPR_ID  = ",period," 
                              AND MSR_MAS_ID IN (", MERCADOS,")"))
  
  TMMS_1 <- left_join(TMMS_1, SHO_EXTERNAL, by=c("MSR_SHO_ID"="SHO_ID"))
  
  SIRVAL_1 <- dbGetQuery(conn_s, paste0("SELECT nc_periodid-989 as PERIODO,AC_NSHOPID AS TIENDA,
                                SUM(NC_SLOT4*NC_SLOT3) AS VENTAS_VALOR 
                                from VLDRAWDATA_MX.RAWDATA_MM C
                           JOIN VLDIMDB_MX.NANCHARVALS B ON B.F_NCV_NAN_KEY=C.F_NAN_KEY
                          JOIN VLDIMDB_MX.CHARVALS A On (A.NC_CHARID=B.NC_CHARID and a.NC_CHARVALUEID=b.NC_CHARVALUEID)
                                WHERE AC_DTGROUP LIKE 'AUDIT_DTYPE' and nc_periodid = ",period,"+989 and A.NC_CHARID = 57153 AND F_NAN_KEY IN (SELECT
                                                                                                                                     B.F_NCV_NAN_KEY
                                                                                                                                     FROM VLDIMDB_MX.CHARVALS A
                                                                                                                                     JOIN VLDIMDB_MX.NANCHARVALS B on (A.NC_CHARID=B.NC_CHARID and a.NC_CHARVALUEID=b.NC_CHARVALUEID)
                                                                                                                                     WHERE A.NC_CHARID =57153 
                                                                                                                                     AND A.NC_CHARVALUEID IN (",CATEGORIA,"))
                                GROUP BY NC_PERIODID, AC_NSHOPID")) 
  
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
  
             
  TIEMPO_TOTAL<-Sys.time()-Tiempo_tot
  
  print(TIEMPO_TOTAL)
  
  
  ##PARA ACTUALIZAR LAS PESTAñAS DE RESUMEN Y ESTATUS_PREV##
  
  INDUSTRY <- dbGetQuery(conn_m,paste0("SELECT DISTINCT CIN_CEL_ID AS CELDA, CIN_ACV_TURNOVER AS ACV_UNIV, CIN_NUMBER_OF_SHOPS AS N_UNIV,
                         CIN_X_FACTOR AS XF, CIN_Z_FACTOR AS ZF
                         FROM MADRAS_DATA.TMXP_CELL_INDUSTRY
                         WHERE CIN_CEL_SAM_ID = ",SAMPLE,"
                         AND CIN_TPR_ID = ",PRE_PER,"
                         AND CIN_TYPE = 'CE'"))
  
  CELDA_DESCRIPCION <- dbGetQuery(conn_m, paste0("SELECT CEL_ID AS CELDA, CEL_DESCRIPTION AS NOMBRE_CELDA
                                  FROM MADRAS_DATA.TMXP_CELL
                                  WHERE CEL_SAM_ID = ",SAMPLE))
  
  MERCADO_DESCRIPCION <- dbGetQuery(conn_m, paste0("SELECT A.MSC_CEL_ID AS CELDA, B.MAS_DESCRIPTION AS MERCADO
                                    FROM MADRAS_DATA.TMMS_MARKET_SEGMENT_CELL A 
                                    LEFT JOIN MADRAS_DATA.TMMS_MARKET_SEGMENT B ON A.MSC_MAS_ID = B.MAS_ID
                                    WHERE MSC_MAS_ID IN (",MERCADOS,")
                                    AND MSC_TPR_ID =", PRE_PER))
  
  
  INSUMO_CELDA <- left_join(INDUSTRY, CELDA_DESCRIPCION, by=c("CELDA"))%>%
                  left_join(MERCADO_DESCRIPCION, by=c("CELDA"))%>%
                  select(CELDA, MERCADO, NOMBRE_CELDA, ACV_UNIV, N_UNIV, XF, ZF)%>%
                  arrange(MERCADO)
                  
  
  
  CONTENT_PREV <- dbGetQuery(conn_m,paste0("SELECT B.SHO_EXTERNAL_CODE AS SOURCE_ID, A.CCO_CEL_ID AS CELL_ID, C.SHI_ACV_TURNOVER
                               FROM MADRAS_DATA.TMXP_CELL_CONTENT A 
                               LEFT JOIN MADRAS_DATA.TRSH_SHOP B ON A.CCO_SHO_ID = B.SHO_ID
                               LEFT JOIN MADRAS_DATA.TMXP_SHOP_INDUSTRY C ON A.CCO_SHO_ID = C.SHI_SHO_ID AND A.CCO_TPR_ID = C.SHI_WEEK_EFFECTIVE_FROM
                               WHERE CCO_TPR_ID = ",PRE_PER,"
                               AND CCO_CEL_SAM_ID = ",SAMPLE, "
                               AND SHI_IND_ID = ",INDUSTRY_,"
                               AND CCO_TYPE = 'CE'"))
  
  CONTENT_CURR <- dbGetQuery(conn_m,paste0("SELECT B.SHO_EXTERNAL_CODE AS SOURCE_ID, A.CCO_CEL_ID AS CELL_ID,C.SHI_ACV_TURNOVER AS ACV_FINAL
                               FROM MADRAS_DATA.TMXP_CELL_CONTENT A 
                               LEFT JOIN MADRAS_DATA.TRSH_SHOP B ON A.CCO_SHO_ID = B.SHO_ID
                               LEFT JOIN MADRAS_DATA.TMXP_SHOP_INDUSTRY C ON A.CCO_SHO_ID = C.SHI_SHO_ID AND A.CCO_TPR_ID = C.SHI_WEEK_EFFECTIVE_FROM
                               WHERE CCO_TPR_ID = ",ULT_PER,"
                               AND CCO_CEL_SAM_ID = ",SAMPLE, "
                               AND SHI_IND_ID = ",INDUSTRY_,"
                               AND CCO_TYPE = 'CE'"))%>%
                  mutate(USABILIDAD_FIN=1)
  
  SIRVAL_V <- mutate(SIRVAL, PERIODO=paste0("P_",PERIODO))%>%
              spread(PERIODO, VENTAS_VALOR)
  
  ESTIMADAS <- dbGetQuery(conn_s,paste0("SELECT DISTINCT AC_NSHOPID AS SOURCE_ID FROM VLDSYS_MX.STOREMONITOR_MM
                                 WHERE NC_PERIODID = ",ULT_PER,"+989  
                                 AND AC_MONITORTAG = '999999'
                                 AND AC_MONITORSTATUS IN ('14','34')
                                 AND AC_DTGROUP = 'AUDIT_DTYPE'"))%>%
               mutate(ASTI=1)
  
  CONTENT<- mutate(CONTENT_PREV, USABILIDAD_PREV=1)%>%
            full_join(CONTENT_CURR, by=c("SOURCE_ID","CELL_ID"))%>%
            mutate(USABILIDAD_PREV=ifelse(is.na(USABILIDAD_PREV),0,USABILIDAD_PREV),
                   USABILIDAD_FIN=ifelse(is.na(USABILIDAD_FIN),0,USABILIDAD_FIN),
                   TIPO=substr(SOURCE_ID,0,4),
                   ESTATUS_PREVIO="",
                   PER_ESTIMADA="",
                   ACCION=ifelse(USABILIDAD_PREV==0 & USABILIDAD_FIN==1,"E",""),
                   ACCION=ifelse(USABILIDAD_PREV==1 & USABILIDAD_FIN==0,"S",ACCION),
                   SAMPLE=0)%>%
           left_join(SIRVAL_V, by=c("SOURCE_ID"="TIENDA"))%>%
           left_join(ESTIMADAS, by=c("SOURCE_ID"))%>%
           mutate(ASTI=ifelse(is.na(ASTI),0,ASTI))%>%
           select(SOURCE_ID:USABILIDAD_PREV, TIPO, ESTATUS_PREVIO, PER_ESTIMADA, ACCION, USABILIDAD_FIN,ACV_FINAL,VENTAS_PREV=paste0("P_",get("PRE_PER")),VENTAS_CURR=paste0("P_",get("ULT_PER")), ASTI, SAMPLE)%>%
           arrange(CELL_ID,desc(VENTAS_CURR))
            
  
  INDUSTRY_A <- select(INDUSTRY, CELDA, XF)
  
  VENTAS <- select(CONTENT,CELL_ID, SOURCE_ID, VENTAS_PREV, VENTAS_CURR,USABILIDAD_PREV)%>%
            left_join(INDUSTRY_A, by=c("CELL_ID"="CELDA"))%>%
            mutate(VVP_PREV= VENTAS_PREV*XF,
                   RELATIVE_PREV= VENTAS_PREV*XF,)
  
fwrite(INSUMO_CELDA,"G:/Shared drives/RETAIL REGULAR/PRODUCCION/SAMPLE INSPECTION/45 - HOME/2020020 - AGO20/FINAL/R/RESUMEN.csv")
fwrite(CONTENT, "G:/Shared drives/RETAIL REGULAR/PRODUCCION/SAMPLE INSPECTION/45 - HOME/2020020 - AGO20/FINAL/R/ESTATUS_PREV.csv")
fwrite(VENTAS, "G:/Shared drives/RETAIL REGULAR/PRODUCCION/SAMPLE INSPECTION/45 - HOME/2020020 - AGO20/FINAL/R/VENTAS.csv")
shell.exec("G:/Shared drives/RETAIL REGULAR/PRODUCCION/SAMPLE INSPECTION/45 - HOME/2020020 - AGO20/FINAL/R")    

  