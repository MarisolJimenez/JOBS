rm(list = ls(all=T))

library(RJDBC)
library(dplyr)
library(sqldf)
library(xlsx)
library(tidyr)
library(data.table)

driver <- JDBC(driverClass = "oracle.jdbc.OracleDriver"
               , classPath = "C:/Users/alad9002/Documents//ojdbc7.jar"        
               , identifier.quote = "'")

##driver <- JDBC(driverClass = "oracle.jdbc.OracleDriver"
##               , classPath = "C:/JAVA/ojdbc7.jar"       
##             , identifier.quote = "'")   

conn_m <- dbConnect(drv = driver
                    , url = "jdbc:oracle:thin:@oravalplc01scan.enterprisenet.org:1535/VALPLC01"
                    , user = "NRSP_GUEST"
                    , password = "NRSP_GUEST")

conn_s <- dbConnect(drv = driver
                    , url = "jdbc:oracle:thin:@orasirplc01scan.enterprisenet.org:1521/SIRPLC01"
                    , user = "SIRVALREAD_MX"
                    , password = "Zl7KIAWz")

PER_INI <- 919


SAMPLE <- dbGetQuery(conn_m, paste0("SELECT DISTINCT *
                                     FROM MADRAS_DATA.TMXP_SHOP_SAMPLE 
                                     WHERE SSW_SAM_ID IN (1000304)
                                     AND SSW_TPR_ID >=", PER_INI))



INDUSTRY <- dbGetQuery(conn_m, paste0("SELECT DISTINCT *
                                     FROM MADRAS_DATA.TTAG_SHOP_INDUSTRY
                                     WHERE SIN_IND_ID IN (1000244)
                                     AND SIN_TPR_ID >=", PER_INI))





VALIDACION <- left_join(SAMPLE, INDUSTRY, by=c("SSW_TPR_ID"="SIN_TPR_ID", "SSW_SHO_ID"="SIN_SHO_ID"))


tiendas_error <- filter(VALIDACION,is.na(SIN_ACV_TURNOVER))







