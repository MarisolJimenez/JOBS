install.packages("combinat")
library(combinat)
combn(8,8,)

#####
#distance au sens de Ward

#datos grupo A
a<-c(-0.70,1.16, -1.15, -0.22 )
b<-c(-1.44,0.62, 0.15, 0.16)
c<-c(1.28,-1.06,0.08,1.06)
e1<-data.frame(rbind(a,b,c))
  #A <- scale(e1, center = TRUE, scale = TRUE)

#datos grupo B
d<-c(1.49, 0.37, -0.98, 0.94)
f<-c(-0.92, 1.03,0.54, 0.71)
e2<-data.frame(rbind(d,f))
  #B <- scale(e2, center = TRUE, scale = TRUE)
#centros de gravedad
ga<-colMeans(e1)
gb<-colMeans(e2)
d1<-rbind(ga,gb)
distanciasg<-dist(d1, method = "euclidean")
distancesware <- (nrow(e1)*nrow(e2)*dist(d1)^2)/(nrow(e1)+nrow(e2))


#####
#distance au sens de max et lien simple

#datos grupo A
a<-c(-0.58,1.58, -0.29, -0.88 )
b<-c(0.94,-0.84, 1.88, -1.27)
c<-c(-0.15,-0.48, 1.78, 0.29 )
d<-c(0.03, 1.23, 0.27, -0.41)
e1<-rbind(a,b,c,d)
e1<-data.frame(e1)
#A <- scale(e1, center = TRUE, scale = TRUE)
#datos grupo B
d<-c(5.05, 2.50, 1.85, 3.29)
f<-c(4.89, 4.50, 3.07, 1.58)
e2<-rbind(d,f)
e2<-data.frame(e2)
#B <- scale(e2, center = TRUE, scale = TRUE)
e<-data.frame(rbind(e1,e2))
#centros de gravedad
distanciasg<-dist(e, method = "maximum")
distanciasg #ver el min entre los dos ultimos renglones que son el grupo B

##### 8
#inertia intraclasse de P, dadas intertias de cada conjunto, sus cardinalidades y el centro de gravedad total
a<-c(-0.13,-0.25,-2.28)
b<-c(0.38,-0.16,1.28)
c<-c(1.98,-0.15,1.98)
d<-c(0.59,0.16,0.48)
e<-c(0.7,-0.1,0.37) # centro de gravedad total
ensemble<-data.frame(rbind(a,b,c,d,e))
d2_2g_gsi<-dist(ensemble)^2 #distancia al cuadrado, del centro de cada S al centro de g total, solo ver ultimo row

#####
#distancia maxima entre 2 vectores
a<-c(0.83, 0.13, 0.34, -1.76) 
b<-c(1.59, -0.05, 0.78, -0.07)
max(abs(a-b))

#####
#nume de subconjuntos de tamaño tal de un ensemble con 21 items 
itemstotal<-21
cardinal_ensemble_1<-11
cardinal_ensemble_2<-12
num_conjuntos<-factorial(itemstotal)/(factorial(itemstotal-cardinal_ensemble_1)*factorial(cardinal_ensemble_1))
num_conjuntos
num_conjuntos2<-factorial(itemstotal)/(factorial(itemstotal-cardinal_ensemble_2)*factorial(cardinal_ensemble_2))
respuesta<-num_conjuntos2+num_conjuntos
respuesta


##centrar reducir un dato dada su media y su varianza, ver si se da esta o ecart type!
moyenne<-c(80.26,-2.54)
ecartype<-sqrt(c(0.12,6.31))
dato<-c(79.92,-2.76)
(dato-moyenne)/ecartype


#obtener entropia de Gini
clase1<-108 #cardinalidad de clase i
clase2<-54
clase3<-196
totalclas<-sum(clase1+clase2+clase3) #suma de cardinalidades de esa clase
gini<-2*(clase1/totalclas)*(clase2/totalclas)*(clase3/totalclas)



