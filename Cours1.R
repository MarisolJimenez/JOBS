#rm(list=ls())

Mx<-rbind(x1,x2,x3,x4)
My<-rbind(y1,y2,y3,y4)

inertie <- function (x){
 #centre de gravité des k vecteurs
g<-colMeans(x)  
k<-nrow(x)
 #calcul de l'inertie
#g
#return(g) #pas necéssaire en R, c'est la dernière expression évaluée qui
#est renvoyéee
}

inertie2<-function(x){
  k<-nrow(x)
  p<-ncol(x)
  g<-colMeans(x)
  mean(rowSums((x-matrix(g,k,p,byrow = TRUE))^2))
}

inertie3<-function(x){
  #k<-nrow(x)
  #p<-ncol(x)
  g<-colMeans(x)
  sum((t(x)-g)^2)/nrow(x)
}


#Test de la fonction
k<-1e4
p<-1e3
set.seed(123)
x<-matrix(rnorm(k*p),k,p)
#inertie(x)
inertie2(x)
inertie3(x)
 #méasure la qualité du program
#system.time(inertie(x))
system.time(inertie2(x))
system.time(inertie3(x))
######################################      EX 3
x1<-c(2,3)
x2<-c(2.5,3.5)
x3<-c(2,3.5)
x4<-c(2.5,3)
x<-matrix(cbind(x1,x2,x3,x4),4,2,byrow = TRUE)
y1<-c(2,4)
y2<-c(3,5)
y3<-c(2,5)
y4<-c(3,4)
y<-matrix(cbind(y1,y2,y3,y4),4,2,byrow = TRUE)
  inertie2(x)
  inertie2(y)
xlimi <- range(c(x[,1],y[,1]))
ylimi <- range(c(x[,2],y[,2]))
plot(x[,1],x[,2],col = "blue",xlim=xlimi, ylim=ylimi)
#plot(y[,1],y[,2],col = "red")
points(y[,1],y[,2], col="red")
#Calculer les inerties de S=(x1,   x4) et S'=(y1,...y4)



#Exo pp15
v1<-c(1,10,2,11,12)
v2<-c(2,11,3,12,13)
d<-data.frame(v1,v2)
d
distances <- dist(d,method = "euclidean")
dist<-(d[])
help("dist")









