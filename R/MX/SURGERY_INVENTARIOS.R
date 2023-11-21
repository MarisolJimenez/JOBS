rm(list = ls(all=T))

library(RJDBC)
library(dplyr)
library(sqldf)
library(xlsx)
library("tidyr")
library("data.table")


ruta <-"C:/Users/jima9001/Desktop/MX/LIBERACION/EXTRACCIONES/"

#file:///G:/Shared drives/RETAIL REGULAR/PRODUCCION/LIBERACION/MAR20/1_AFI_HOME.csv

insumo <- read.csv(paste0(ruta,"SURGERY_INV_BOTANAS_AGO",".csv"))



per_act <- 1132
per_prev<- 1127
per_sir_act <- per_act + 989
per_sir_prev<- per_prev + 989


#CONEXIONES

driver <- JDBC(driverClass = "oracle.jdbc.OracleDriver"
               , classPath = "C:/64 bits/sqldeveloper-17.3.1.279.0537-x64/sqldeveloper/jdbc/lib/ojdbc8.jar"       
               , identifier.quote = "'")

conn_s <- dbConnect(drv = driver
                    , url = "jdbc:oracle:thin:@orasirplc01scan.enterprisenet.org:1521/SIRPLC01"
                    , user = "SIRVALREAD_MX"
                    , password = "Zl7KIAWz")

conn_m <- dbConnect(drv = driver
                    , url = "jdbc:oracle:thin:@oravalplc01scan.enterprisenet.org:1535/VALPLC01"
                    , user = "NRSP_GUEST"
                    , password = "NRSP_GUEST")


TIENDAS <- select(insumo, TIENDA)%>%
  unique
TIENDAS_DWH<-TIENDAS
TIENDAS <- paste0(TIENDAS$TIENDA, collapse=",")
TIENDAS_DWH<-paste0(TIENDAS_DWH$TIENDA, collapse="','")

NANKEY <- select(insumo, NANKEY)%>%
  unique
NANKEY <- paste0(NANKEY$NANKEY, collapse=",")


#CONSULTAS LA INFO DE LA RAW 
raw_data <- dbGetQuery(conn_s, paste0("SELECT ac_dtgroup, nc_periodid as PERIODO,AC_NSHOPID*1 AS TIENDA,AC_CREF, NC_HASH_SIGNATURE, AC_CREFSUFFIX, f_nan_key,NC_SLOT4 AS VTAS_UNIDAD,NC_SLOT2 AS COMPRAS, NC_SLOT1 AS INVENTARIO,  NC_SLOT3 AS PRECIO
                                  from VLDRAWDATA_MX.RAWDATA_MM 
                                  WHERE AC_DTGROUP LIKE 'AUDIT_DTYPE' and nc_periodid in (",per_sir_act,")
                                  AND AC_NSHOPID IN (",TIENDAS,")
                                  AND F_NAN_KEY IN (",NANKEY,")"))

ayuda <- left_join(raw_data, insumo, by=c("TIENDA", "F_NAN_KEY"="NANKEY"))%>%
         filter(!is.na(INVENTARIO_NEW))

reg_ayuda <- count(ayuda)
reg_insumo <- count(insumo)

duplicados <- select(ayuda, TIENDA, F_NAN_KEY)%>%
              group_by(TIENDA,F_NAN_KEY)%>%
              summarise(REGISTROS=n())

#CONSULTAS LA INFO DE DWH

SURGERY_ARMADO <- mutate(ayuda,
                         ac_operation="UC",
                         ac_ticketlabel=TLABEL,
                         country_ID="52",
                         local_period="",
                         Sirval_periodid=get("per_sir_act"),
                         local_shop="",
                         Sirval_shop=TIENDA,
                         EAN_PLU=AC_CREF,
                         cref_suffix=AC_CREFSUFFIX,
                         Volumetric=AC_DTGROUP,
                         nc_nan=F_NAN_KEY,
                         conv="",
                         nc_slot1=INVENTARIO_NEW,
                         nc_slot2="",
                         nc_slot3="",
                         nc_slot4="",
                         nc_slot5="",
                         nc_slot6="",
                         nc_slot7="",
                         nc_slot8="",
                         nc_slot9="",
                         nc_slot10="",
                         ac_cslot1="",
                         ac_cslot2="",
                         Comment=TLABEL,
                         Hash_Signature=NC_HASH_SIGNATURE) 

SURGERY_ARMADO <- select(SURGERY_ARMADO,ac_operation:Hash_Signature)  

tlab<-sqldf("select distinct tlabel from insumo")
print(paste0("RAW DATA ",reg_ayuda, " de ",reg_insumo))

#fwrite(SURGERY_ARMADO, paste0(ruta,"SURGERY_",tlab[,1],".TXT"), col.names = FALSE, sep=",", na = "")
#fwrite(REGISTROS_UNICOS_CH, paste0(ruta,"SURGERYCH_",tlab[,1],".TXT"), col.names = FALSE, sep=",", na = "")

write.table(SURGERY_ARMADO,file=paste0(ruta,"SURGERY_",tlab[,1],".TXT"),
            quote = FALSE,sep = ",",col.names = FALSE, row.names = FALSE,na = "")