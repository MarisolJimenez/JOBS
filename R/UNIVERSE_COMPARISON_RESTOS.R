##################### PROGRAMA #############
rm(list=ls())

Sys.setenv(JAVA_HOME='C:/Program Files/Java/jre1.8.0_171')

library(RJDBC)
library(RODBC)
library(dplyr)
library(sqldf)
library(xlsx)
library(tidyr)
library(fitdistrplus)
library(data.table)


relacion <- read.csv("C:/Users/golu8002/Documents/Transicion/2019/Chile/08_MERCADOS_NUEVOS/VALIDACION/RELACION_RESTA.csv")


periodos_msr <- c(980, 981, 982, 983, 984, 985, 986, 987, 988, 989, 990, 991, 992, 993, 994, 995, 996, 997, 998, 999,
                  1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 1014, 1015, 1016,
                  1017, 1018, 1019, 1020, 1021, 1022, 1023, 1024, 1025, 1026, 1027, 1028, 1029, 1030, 1031, 1032, 1033,
                  1034, 1035, 1036, 1037, 1038, 1039, 1040, 1041, 1042, 1043, 1044, 1045, 1046, 1047, 1048, 1049, 1050,
                  1051, 1052, 1053, 1054, 1055, 1056, 1057, 1058, 1059, 1060, 1061, 1062, 1063, 1064, 1065, 1066, 1067, 
                  1068, 1069, 1070, 1071
)

periodoss <- "980, 981, 982, 983, 984, 985, 986, 987, 988, 989, 990, 991, 992, 993, 994, 995, 996, 997, 998, 999, 1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 1014, 1015, 1016, 1017, 1018, 1019, 1020, 1021, 1022, 1023, 1024, 1025, 1026, 1027, 1028, 1029, 1030, 1031, 1032, 1033, 1034, 1035, 1036, 1037, 1038, 1039, 1040, 1041, 1042, 1043, 1044, 1045, 1046, 1047, 1048, 1049, 1050, 1051, 1052, 1053, 1054, 1055, 1056, 1057, 1058, 1059, 1060, 1061, 1062, 1063, 1064, 1065, 1066, 1067, 1068, 1069, 1070, 1071"
periodo_inicio <- 980
periodo_fin <- 1071


drv <- JDBC(driverClass = "oracle.jdbc.OracleDriver"
            , classPath = "C:/JAVA/ojdbc8.jar"       
            , identifier.quote = "'")



##Conexión a las BD de MADRAS
#conn_m <- dbConnect(drv=drv
#                   , url = "jdbc:oracle:thin:@oravalprdscan.enterprisenet.org:1535/VALPRDMDR"
#                  , user = "NRSP_GUEST"
#                 , password = "NRSP_GUEST")

##POC
conn_m <- dbConnect(drv=drv
                    , url = "jdbc:oracle:thin:@daylvalq01:1521/VALPOCNA"
                    , user = "MADRAS_GUEST"
                    , password = "MADRAS_GUEST")



Lista_Mercados <- list()
Relacion_Diferencias <- data.frame(Mercado=NA,Estaus=NA,Numero_de_errores=NA)

for (i in 1:nrow(relacion)) {
  
  msr_final <- dbGetQuery(conn=conn_m,statement=paste0("SELECT A.MSR_TPR_ID, B.SHO_EXTERNAL_CODE, A.MSR_MAS_ID
                                                 FROM MADRAS_DATA.tmms_ms_resolution A
                                                       JOIN MADRAS_DATA.TRSH_SHOP B ON A.MSR_sho_id = B.sho_id
                                                       WHERE A.msr_cch_id='CLSCAN' AND A.msr_cel_sam_id = 990852 
                                                       AND A.MSR_TPR_ID IN (",periodoss,")
                                                       AND A.MSR_MAS_ID IN (",relacion[i,1] ,")"))
  
  
  msr_original <- dbGetQuery(conn=conn_m,statement=paste0("SELECT A.MSR_TPR_ID, B.SHO_EXTERNAL_CODE, A.MSR_MAS_ID
                                                          FROM MADRAS_DATA.tmms_ms_resolution A
                                                          JOIN MADRAS_DATA.TRSH_SHOP B ON A.MSR_sho_id = B.sho_id
                                                          WHERE A.msr_cch_id='CLSCAN' AND A.msr_cel_sam_id = 990852 
                                                          AND A.MSR_TPR_ID IN (",periodoss,")
                                                          AND A.MSR_MAS_ID IN (",relacion[i,2] ,")"))
  
  
 
  msr_aresta <- dbGetQuery(conn=conn_m,statement=paste0("SELECT A.MSR_TPR_ID, B.SHO_EXTERNAL_CODE, A.MSR_MAS_ID
                                                        FROM MADRAS_DATA.tmms_ms_resolution A
                                                        JOIN MADRAS_DATA.TRSH_SHOP B ON A.MSR_sho_id = B.sho_id
                                                        WHERE A.msr_cch_id='CLSCAN' AND A.msr_cel_sam_id = 990852 
                                                        AND A.MSR_TPR_ID IN (",periodoss,")
                                                        AND A.MSR_MAS_ID IN (",relacion[i,3] ,",",relacion[i,4] ,")"))
  
  
  msr_final$LLAVE<-paste0(msr_final$MSR_TPR_ID,"_",msr_final$SHO_EXTERNAL_CODE)
  msr_original$LLAVE<-paste0(msr_original$MSR_TPR_ID,"_",msr_original$SHO_EXTERNAL_CODE)
  msr_aresta$LLAVE<-paste0(msr_aresta$MSR_TPR_ID,"_",msr_aresta$SHO_EXTERNAL_CODE)
  
  orig_resta<-msr_original %>% left_join(msr_aresta, by = c("LLAVE" = "LLAVE")) 
  orig_resta<-orig_resta %>% filter(is.na(MSR_MAS_ID.y))
  diferencia<-msr_final %>% left_join(orig_resta,by = c("LLAVE" = "LLAVE"))
  diferencia_final<-diferencia %>% filter(is.na(MSR_MAS_ID.x))
  Relacion_Diferencias[i,1]<-relacion[i,1]
  Relacion_Diferencias[i,2]<-"BIEN"
  Relacion_Diferencias[i,3]<-"Hay 0 diferencias"
  if (nrow(diferencia_final)!=0) {
    Relacion_Diferencias[i,2]<-"MAL"
    Relacion_Diferencias[i,3]<-paste0("Hay ",nrow(diferencia_final)," diferencias")
  }
  Lista_Mercados[[i]]<-diferencia_final
  
}
################## FWRITE ###############

fwrite(Relacion_Diferencias,"C:/Users/golu8002/Documents/Transicion/2019/Chile/08_MERCADOS_NUEVOS/VALIDACION/RESTOS/Validacion_Restas_980-1071.csv")

##View(Lista_Mercados[[75]])






