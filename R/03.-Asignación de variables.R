rm(list=ls())
options(guiToolkit = "RGtk2")

library(DBI)
library(rJava)
library(RJDBC)
library(RODBC)
library(dplyr)
library(sqldf)
library(xlsx)
library(tidyr)
library(fitdistrplus)
library(data.table)
library(stringr)
library(gWidgets)
library(gWidgets2)
library(gWidgets2RGtk2)

memory.limit(9000)

#gmessage("Seleccione la ruta donde desea que se guarde el archivo",title = "Info",icon = "info")
#dir<-choose.dir()
rm(list=ls())
dir<-"C:/Users/jima9001/Desktop/NIELSEN/ARG PROD"
##Carpetas de salida (accesos)
servidor_sms<- "acn054buesqlsms"        #Servidor SMS
sms_retail<- "SMSArg"                   #Base de Datos Current del SMS RETAIL
sms_retail_hist<- "SMSArgHistory"          #Base de Datos History del SMS RETAIL
user_sms<- "ugsa7001"             #Usuario SMS
pass_sms<- "ugsa7001"             #Password SMS
user<- "usr_read"             #Usuario 
pass<- "LEeras31"            #Password 
sms_scan<- "SMSscanArg"                   #BASE DE DATOS SMS SCAN CURRENT
connection_sms <- odbcDriverConnect(paste0("driver={SQL Server};Server=",servidor_sms,";uid=",user,";pwd=",pass))
#ind1<-ginput("Introduce el ID de indice", title = "Industry", icon = "question")
ind1 <- 86
###############CONSULTAS Y CRUCES####################
#dato<-ginput("Ingrese el indice, periodo inicial y final, de la siguiente manera: \n \n Indice,Periodo_inicial,Periodo_final", title = "Variables", icon = "question")

#dato <- strsplit(dato,",")[[1]]
#indice<-dato[1]
#periodo_inicio<-dato[2]
#periodo_fin<-dato[3]
# sca1:# 2,87,33,6,59,98,158,77,52,281,9,13,4,16,23,202,433,288,18,15,74,209,10,150,151,152,47,25,204,8,78,
# 27,36,55,440,26,49,115
# sca2: 225,116,46,71,214,102,283,88,227,287,201,188,32,218,215,62,226,184,182,208,145,153,224,181,203,251,
# 159,106

 
var_aux<-5

variable_aux<-ginput("De la siguiente lista ingrese el numero de la variable que desea trabajar: \n \n 1.- tipo_de_tienda \n 2.- tamaño_de_tienda \n 3.- cadena_de_tiendas \n 4.- formato")     
if(variable_aux==1) { variable <- 'tamaño_de_tienda'}else{
  if(variable_aux==2) {  variable <- 'cod_provincia'}else{
    if(variable_aux==3) {  variable <- 'tipo_de_negocio'}else{
      if(variable_aux==4) {  variable <- 'asiatico'}else{
        if(variable_aux==6) {  variable <- 'CLASIF'}else{
          if(variable_aux==7) {  variable <- 'area_nielsen'}else{
            if(variable_aux==5) {  variable <- 'tipo_de_tienda'}else{
              if(variable_aux==8) {  variable <- 'source_acv' 
              var_aux<-5} else{gmessage("No es un valor correcto",title = "Error",icon = "error")}
            }
          }  
        }
      }
    }
  }
}
while (TRUE){
  if (variable_aux>8) {
    break
  }
  
  
  master <- sqlQuery(connection_sms,
                     paste0("SELECT * FROM ",servidor_sms,".",sms_retail,".dbo.source_master "))
  ##############################CASO_CESAR_SOLIS#####################################################
  
  
  
  periodo_inicio <- 2017006 #CAMBIAR
  periodo_fin <- 2019012 #CAMBIAR
  indice <-86
  period_source_id_2 <- sqlQuery(connection_sms,
                                 paste0("SELECT * FROM ",
                                        servidor_sms,".",sms_retail_hist,".dbo.index_period_source ",
                                        " WHERE period_id >=", periodo_inicio, "AND period_id <=  ", periodo_fin,
                                        " AND index_id = ",indice,
                                        " AND status_id > ",var_aux,
                                        "AND (source_id NOT IN (42630,42675,42910,43137,43202,42524,42525,42526,43447,42527,42528,
                                         42529,42530,42531,42532,42533,42534,42535,42536,42537,42538,42539,42540,42541,42542,
                                         42543,42544,42545,42546,43237,42547,42548,42549,42550,42551,42552,42553,43448,42554,
                                         42555,42556,42558,42559,42560,42561,42562,42563,42564,42565,42566,42567,43449,42568,
                                         42569,43450,43451,42570,42571,42572,42573,42574,42575,42576,42578,42579,42580,42581,
                                         42582,42583,42584,42585,42586,42587,42588,42589,42591,42592,42593,42594,42595,42596,
                                         42597,42598,42599,42600,42601,42602,42603,42604,42605,42606,42607,42608,42609,42610,
                                         42611,42613,42615,42616,42617,42618,42619,42620,42621,42622,42623,42624,42625,42626,
                                         42628,42629,42631,42632,42634,42636,42637,42638,42639,42640,42642,42643,42644,42645,
                                         42646,42647,42648,42649,42650,42652,42654,42655,42656,42657,42658,42659,43235,42660,
                                         42662,42663,42664,42665,42666,42667,42668,42669,42670,42671,42672,42673,42674,42676,
                                         42677,42678,42679,42680,42681,42682,43452,42683,42684,42685,42686,42687,42688,42690,
                                         42691,42692,42693,42694,42695,42696,42698,42699,42700,42701,42703,42705,42706,42707,
                                         42708,42709,42710,42711,42712,42713,42714,42715,42717,42718,42719,42720,42721,42722,
                                         43454,42723,42724,42725,42726,42727,42729,42730,42732,42733,42734,42735,42737,42738,
                                         42739,42740,42741,42742,42744,42745,42746,42747,42748,42749,42750,42751,42752,42753,
                                         42754,42755,42756,42757,42758,42759,42760,42761,42762,42763,42764,42765,42766,42767,
                                         42768,42769,42770,42771,42773,42774,42775,42776,42777,42778,42779,42780,42781,42782,
                                         42783,42785,42786,42787,42788,42790,42791,42792,42793,43455,43456,42795,42796,42798,
                                         42799,42800,42801,42802,42803,42805,42807,42808,42809,42810,42811,42812,42813,42814,
                                         42815,42816,42817,42818,42819,42821,42822,42824,42825,42826,42828,42829,42830,42831,
                                         42832,42833,42834,42835,42836,42837,42838,42839,42840,42841,42842,42843,42844,42845,
                                         42846,42847,42848,42849,42850,42851,42852,42854,42855,42856,42857,42858,42859,42860,
                                         42861,43457,42862,42863,42864,42865,42866,42867,42869,42870,42871,42872,42874,42875,
                                         42876,42877,42878,42879,42881,42882,42883,42884,42885,42886,42887,42888,42889,42890,
                                         42892,42894,42895,42896,42897,42898,42899,42900,42901,43458,42903,42904,42906,42907,
                                         42908,42911,42912,42913,42914,42915,42916,42917,42919,42920,42921,42922,42923,43459,
                                         42924,42925,42926,42927,42929,42930,42931,42932,42933,42934,42935,42936,43460,42937,
                                         42938,42939,42940,42941,42942,42944,42945,42946,42947,42948,42949,42950,42951,42952,
                                         42953,42954,42955,42957,42958,42959,42961,42962,42963,42964,42965,42966,42968,42969,
                                         42971,42972,42973,42974,42975,42976,42977,42978,42979,42981,42983,42984,42985,42986,
                                         42987,42988,42989,42990,42991,42992,42993,42994,42995,42996,42999,43000,43001,43002,
                                         43003,43004,43005,43006,43007,43008,43010,43011,43012,43013,43014,43015,43016,43017,
                                         43461,43018,43019,43021,43022,43023,43024,43025,43026,43027,43028,43029,43030,43031,
                                         43032,43033,43034,43035,43036,43037,43039,43040,43041,43042,43043,43044,43045,43047,
                                         43048,43049,43051,43052,43053,43462,43054,43055,43056,43058,43059,43060,43061,43062,
                                         43063,43064,43065,43066,43067,43068,43069,43071,43072,43073,43074,43075,43077,43078,
                                         43080,43081,43083,43084,43085,43086,43087,43088,43089,43090,43091,43092,43093,43094,
                                         43095,43097,43098,43099,43100,43101,43102,43103,43104,43105,43106,43107,43108,43109,
                                         43110,43111,43112,43113,43114,43115,43116,43117,43118,43119,43120,43121,43122,43123,
                                         43124,43125,43126,43127,43128,43129,43130,43132,43133,43135,43136,43236,43138,43139,
                                         43140,43141,43142,43143,43144,43145,43147,43149,43150,43152,43153,43154,43155,43156,
                                         43157,43159,43160,43161,43162,43163,43164,43165,43166,43167,43168,43169,43171,43172,
                                         43173,43174,43175,43177,43178,43179,43180,43182,43183,43184,43185,43186,43187,43188,
                                         43189,43190,43191,43192,43193,43194,43195,43196,43197,43198,43199,43200,43201,43464,
                                         43203,43204,43205,43206,43207,43208,43209,43210,43211,43212,43213,43214,43215,43216,
                                         43465,43218,43219,43220,43221,43222,43242,43223,43224,43225,43226,43227,43228,43229,
                                         43231,43232,43233,43234,43245,43243,43241,43239,43246,43244,42557,42590,42614,42627,
                                         42641,42653,42689,42702,42716,42728,42743,42784,42794,42806,42820,42868,42880,42893,
                                         42905,42918,42956,42970,42982,42997,43046,43057,43070,43082,43134,43146,43158,43181,
                                         43217,43230,42577,42612,42633,42651,42661,42697,43453,42736,42772,42789,42804,42823,
                                         42853,42873,42891,42909,42928,42943,42960,42980,43009,43020,43038,43238,43076,43096,
                                         43463,43131,43151,43170,43466,42635,42704,42731,42797,42827,42902,42967,42998,43050,
                                         43079,43148,43176,43240,41668,27246,20494,4030,4420,6810,7472,11808,11809,11810,11811,
                                         11814,11815,11817,14196,17880,19977,20001,20273,21410,21413,27436) AND sot_source=1) "))
  
  #CAMBIAR Verificar tiendas a considerar
  # periodo_fin <- 2019037
  # indice <-1
  # period_source_id_25484 <- sqlQuery(connection_sms,
  #                                    paste0("SELECT * FROM ",
  #                                           servidor_sms,".",sms_retail,".dbo.index_period_source ",
  #                                           " WHERE period_id >=", periodo_inicio, "AND period_id <=  ", periodo_fin,
  #                                           " AND index_id = ",indice,
  #                                           " AND status_id > ",var_aux))
  
  
  
  period_source_11 <- period_source_id_2
  #rbind(period_source_id_2,period_source_id_25484)
  
  period_source_11 <- period_source_11 %>% left_join(master,by = c("source_id"))
  
  
  #period_source_11$scan_audit[is.na(period_source_11$scan_audit)]<-0
  #period_source_11$SUMA<-period_source_11$sot_source - period_source_11$scan_audit
  #period_aux<-period_source_11 %>% filter(SUMA==1)
  #period_aux<-period_source_11 %>% filter(sot_source==0|SUMA==0)
  
  period_source_11_ztv <- sqldf(paste0("SELECT period_id,scan_source_id as source_id,",variable,",index_id 
                                       FROM period_source_11
                                       WHERE cadena_de_tiendas IN (450,411,101,402,280,406,410,190,434,29,400,403,405,134,407,409,121,131,401,404,408,50)")) 
  ##todas las index y master variables cambiar if scan, proy sesga
  
  period_source<-period_source_11_ztv
  
  
  data_index <- sqldf(paste0("SELECT * FROM period_source ORDER BY source_id,period_id"))
  data_index[,3][is.na(data_index[,3])]<-0
  data_index <- data_index %>% filter(!is.na(data_index[,3]))
  data_index$dif_source<-c(0,diff(data_index$source_id))
  data_index$dif_tamano<-c(0,diff(data_index[,3]))
  data_index$dif<-data_index$dif_source+data_index$dif_tamano
  data_index$count<-ifelse(data_index$dif_source==0 & data_index$dif==0,0,1)
  data_index[1,8]<-1
  data_index$period_id_aux<-NA
  data_index$period_id_aux[1]<-data_index$period_id[1]
  for (x in 2:nrow(data_index)) {
    data_index$period_id_aux[x]<-ifelse(data_index$count[x]==1,data_index$period_id[x],data_index$period_id_aux[x-1])
  }
  
  data_index$Week_From_1<-NA
  data_index$Week_To_1<-NA
  for (x in 1:(nrow(data_index)-1)) {
    data_index$Week_From_1[x]<-ifelse(data_index$dif[x+1]==0 & data_index$source_id[x]==data_index$source_id[x+1],0,data_index$period_id_aux[x])
  }
  data_index$Week_From_1[nrow(data_index)]<-ifelse(data_index$dif_source[nrow(data_index)]==0,data_index$period_id_aux[nrow(data_index)],data_index$Week_From_1[nrow(data_index)-1])
  
  for (x in 1:(nrow(data_index)-1)) {
    data_index$Week_To_1[x]<-ifelse(data_index$Week_From_1[x]==0,NA,ifelse(data_index$source_id[x]==data_index$source_id[x+1],data_index$period_id[x],NA))
  }
  data_index$Week_To_1[nrow(data_index)]<-NA
  
  data_index<-data_index %>% filter(Week_From_1!=0)
  #gmessage("Seleccione el input de equivalencia de periodos",title = "Info",icon = "info")
  #Periodos<-read.csv("C:/Users/soce8001/Documents/Periodos_Retail_Mejorados.csv")
  Periodos <- read.csv("C:/Users/jima9001/Desktop/NIELSEN/R STUDIO/PROGRAMAS/CAM_R/Shop_sample_periodo_par.csv")
  
  Input_1 <- data_index %>% left_join(Periodos, by = c("Week_From_1"="DRUG"))#cambiar
  Input_2 <- Input_1 %>% left_join(Periodos, by = c("Week_To_1"="DRUG"))#cambiar
  
  Input_2 %>% mutate_if(is.factor, as.character) -> Input_2
  
  Input_2$NRSP_DESC.y[is.na(Input_2$NRSP_DESC.y)]<-""
  
  
  if(variable_aux!=8){
    FICHERO_FINAL <- Input_2 %>% mutate(CHR_CODE = paste0("AR_",toupper(variable),"_DRUG"), #CAMBIAR	
                                        VALUE = Input_2[,3],	
                                        WEEK_FROM = NRSP_DESC.x,
                                        WEEK_TO = NRSP_DESC.y,	
                                        SHOP_CODE = source_id + 4750000000) #CAMBIAR POR SERVICIO
    FICHERO_FINAL_AUX<- sqldf("SELECT CHR_CODE,VALUE,WEEK_FROM,WEEK_TO,SHOP_CODE FROM FICHERO_FINAL")
    FICHERO_FINAL_AUX %>% write.table(paste0(dir,"\\ASIGNACION_",indice,"_SCA3_",toupper(variable),".csv"), sep = ",", eol = "\n", dec = ".", row.names = FALSE, quote = FALSE, col.names = T)
    shell.exec(paste0(dir,"\\ASIGNACION_",indice,"_SCA3_",toupper(variable),".csv"))
  }else{
    industry_var<-ginput("Introduce el ID de la Industry", title = "Industry", icon = "question")
    FICHERO_FINAL <- Input_2 %>% mutate(SHOP_CODE = source_id + 4730000000, #CAMBIAR POR SERVICIO
                                        INDUSTRY = industry_var,
                                        ACV = Input_2[,3],
                                        SM = 1,
                                        WEEK_FROM = NRSP_DESC.x,	
                                        WEEK_TO = NRSP_DESC.y
    )
    FICHERO_FINAL_AUX<- sqldf("SELECT SHOP_CODE,INDUSTRY,ACV,SM,WEEK_FROM,WEEK_TO FROM FICHERO_FINAL")
    FICHERO_FINAL_AUX %>% write.table(paste0(dir,"\\SHOP_IND_",indice,".csv"), sep = ";", eol = "\n", dec = ".", row.names = FALSE, quote = FALSE, col.names = T)

        shell.exec(paste0(dir,"\\SHOP_IND_",indice,".csv"))
  }
    break}

#fwrite(FICHERO_FINAL_AUX,"ACV.csv")
