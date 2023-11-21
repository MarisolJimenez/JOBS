########SCRIPT PARA LOS FICHEROS DE MASSIVE UPLOAD EN MADRAS#########
#############FICHERO DE SHOP INDUSTRY_POC###################
########PREAMBULO########
###SÍ TU JAVA NO JALA, PRUEBA EL SIG CODIGO: 
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
library(gWidgets)
library(gWidgets2)
library(gWidgets2RGtk2)
######EXPANSION DE CAPACIDAD DE R (SI ES DE 64 BITS, PUEDE SER HASTA 9000)
#memory.limit(9000)
memory.limit(NA)

#gmessage("Seleccione la ruta donde desea que se guarde el archivo",title = "Info",icon = "info")
ruta <- choose.dir() 
periodos <- read.csv("C:/Users/jima9001/Desktop/NIELSEN/CAM/Shop_sample_periodos.csv")

#industry <- 1
periodo_inicio <- 2017025
periodo_fin <- 2019067
setwd(ruta)


##SMS
servidor_sms<- "acn052cpsms01"        #Servidor SMS
sms_retail<- "smsscancam"                   #Base de Datos Current del SMS RETAIL
sms_retail_hist<- "smsscancamHistory"          #Base de Datos History del SMS RETAIL

sms_scan<- "smsscancam"                   #BASE DE DATOS SMS SCAN CURRENT
sms_scan_history="smsscancamHistory"      #BASE DE DATOS SMS SCAN HISTORY

user<- "usr_read"             #Usuario
pass<- "LEeras31"            #Password

connection_sms<-odbcDriverConnect(paste0("driver={SQL Server};Server=",servidor_sms,";uid=",user,";pwd=",pass))



##TIENDA, ACV, PERIODO
INDEX_PERIOD_SOURCE <- sqlQuery(connection_sms,
                                paste0("SELECT DISTINCT source_id, period_id, source_acv 
                                       FROM ",servidor_sms,".",sms_scan_history,".dbo.index_period_source ",
                                       "WHERE period_id >= ",periodo_inicio,
                                       "AND period_id <= ",periodo_fin,
                                       "AND index_id = 1
                                       AND status_id >= 6
                                       and source_id IN (1937,1938,1940,1941,1942,1943,1944,1945,1946,1947,1948,1949,1950,1951,1952,1953,1954,1955,1956,1957,1958,1959,1960,1962,1963,1964,1965,1966,1967,1968,1969,1970,1971,1972,1973,1974,1975,1976,1977,1979,1980,1981,1982,1983,1984,1985,1986,1987,1988,1989,1990,1991,2016,2018,2019,2020,2022,2023,2024,2025,2026,2027,2099,2101,2106,2107,2139,2218,2220,2223,2242,2243,2244,2285,2289,2291,2292,2293,2294,2295,2296,2297,2298,2299,2300,2301,2302,2303,2304,2305,2306,2307,2308,2310,2311,2313,2314,2315,2316,2317,2318,2319,2320,2321,2323,2324,2329,2359,2361,2363,2364,2372,2385,2393,2400,2408,2417,2435,4305,4306,4311,4323,4371,4391,4399,4544,4595,4607,4802,4832,4848,4852,4858,4864,4865,4866,5049,5052,5053,5054,5057,5140,5160,5162,5170,5207,5224,5236,5238,5259,5264,5270,5281,5283,5288,5298,5452,5453,5454,5469,5502,5509,5708,5716,6152,6218,6286,6287,6288,6289,6290,6291,6292,6293,6294,6295,6296,6297,6298,6299,6300,6301,6302,6303,6304,6305,6306,6307,6308,6309,6310,6311,6314,6338,6342,6360,6378,6379,6380,6381,6382,6383,6384,6385,6386,6387,6388,6389,6390,6391,6392,6393,6394,6395,6396,6397,6398,6399,6587,6600,6634,6637,6711,6732,6733,6735,6736,6737,6738,6739,6740,6741,6742,6743,6744,6745,6746,6747,6801,6885,6886,6907,6911,6912,6961,7111,7114,7115,7126,7127,7128,7129,7130,7131,7132,7133,7134,7135,7136,7137,7138,7139,7140,7141,7142,7165,7191,7192,7208,7209,7213,7219,7264,7265,7266,7267,7274,7374,7378,7379,7380,7383)"))
SMS_MASTER_SCAN <- sqlQuery(connection_sms,
                            paste0("SELECT *
                                   FROM ",servidor_sms,".",sms_scan,".dbo.source_master "))
INDEX_PERIOD_SOURCE$source_acv[INDEX_PERIOD_SOURCE$source_acv == 0] <- 1
TIENDAS <- INDEX_PERIOD_SOURCE %>% left_join(SMS_MASTER_SCAN, by = "source_id")
index_id<-c(291,292,290,293,288,289) #also sample id in this particular case
##ACV = 0 => ACV = 1
for (i in 1:6) {
  
  
  if (i==1) {ind_i<-index_id[1]}
  if (i==2) {ind_i<-index_id[2]}
  if (i==3) {ind_i<-index_id[3]}
  if (i==4) {ind_i<-index_id[4]}
  if (i==5) {ind_i<-index_id[5]}
  if (i==6) {ind_i<-index_id[6]}
  
INDEX_PERIOD_SOURCE_5484<- TIENDAS %>% filter(pais==i)

INDEX_PERIOD_SOURCE_1 <- INDEX_PERIOD_SOURCE_5484 %>% left_join(periodos, by = c("period_id" = "LEGACY_SCANTRACK"))
FICHERO_SHOP_INDUSTRY <- INDEX_PERIOD_SOURCE_1 %>% mutate(SHO_EXTERNAL_CODE = 5700000000 + source_id,
                                                          INDUSTRY_ID = ind_i,
                                                          ACV = source_acv,
                                                          SQUARE_METERS = 1,
                                                          WEEK_FROM = NRSP_DESC,
                                                          WEEK_TO = NRSP_DESC)
FICHERO_SHOP_INDSUTRY_1 <- sqldf("SELECT SHO_EXTERNAL_CODE, INDUSTRY_ID, ACV, SQUARE_METERS,
                                         WEEK_FROM, WEEK_TO
                                  FROM FICHERO_SHOP_INDUSTRY")
fwrite(FICHERO_SHOP_INDSUTRY_1, paste0("05_SCANTRACK_SHOP_INDUSTRY_CAM_",index_id[i],".csv"), na = '', 
       row.names = F, quote = F, col.names = F)
}

