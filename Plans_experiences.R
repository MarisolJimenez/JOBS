####   PLANS D'EXPÉRIENCES ET QUALITÉ

########################################            #TD1
#rm(list=ls()) remove objects
#les deux stratégies sont proposées
s1<-c(22,28,29,30)
s2<-c(20,24,26,30)

#1 premièrement il faut codée les deux stratégies
#on prends 20 comme minimum et 30 comme maximum, centre: 25 et [dist (min,mx)]/2 est 5
coded_s1<-(s1-25)/5
coded_s2<-(s2-25)/5

#creation du vector x 
x_1 <- matrix(c(1, 1, 1, 1,
             coded_s1), ncol = 2, byrow = FALSE) #by default il est arrange en colonnes

colnames(x_1)<-c("B_0","B_1")

x_2 <- matrix(c(1, 1, 1, 1,
                coded_s2), ncol = 2)
colnames(x_2)<-c("B_0","B_1")

#multiplication du vector x et sa transpose 
XtX_s1 <- crossprod(x_1,x_1)
XtX_s2 <- crossprod(x_2,x_2)
#inverse du produit x et sa transpose
varianzaB_1<-solve(XtX_s1)
varianzaB_2<-solve(XtX_s2)
a<-ifelse(abs(varianzaB_1) > abs(varianzaB_2), yes = "S2 menor varianza", no = "S1")
g<-c(1,x_1)


########################################            #TD2
#SSLOF, SSPE identificar si el modelo propuesto está bien ajustado, y sus errores se deben al error humano y no al modelo propuesto,
#se separan estos errores para identificar específicaamente a que se debe el error total
#rm(list = ls())

       #1 #explicar el rendimiento en base a la temper
temp<-c(10,10,15,20,25,30)
rend<-c(10,20,35,40,33,10)
NOM<-lm(rend ~ temp) #modelo lineal donde temp explica el rendimiento
summary(NOM)
#R2 es 0.004672 qué es demasiado pequeña, prácticamente temp explica muy poco el rend
   
#2 ajustar un modelo cuadrático
temp2<-temp^2 #crear la variable al cuadrado
NOM_1<-lm(rend ~ temp+temp2) #temp y temp al cudrado explicarán el rendimiento
summary(NOM_1)
#R2 es bastante buena 0.9412, procedamos a analizar el error

#SST=SSR+SSE
#sommes des carrés due a la régression
#SSR= [suma (yi estimada - y barra)] ^ 2
SSR<-sum((NOM_1$fitted.values - mean(NOM_1$model$rend))^2)
#sommes des carrés due a l'erreur
#SSE = [suma (yi - yi  estimada)] ^ 2
#SSE_1<-sum((NOM_1$model$rend - NOM_1$fitted.values)^2)
#somme totale des carrés (centrés)
#SST= [suma (yi - y barra)] ^ 2
SST<-sum((NOM_1$model$rend - mean(NOM_1$model$rend))^2) #~option+ñ#
SSE=SST-SSR

   #3
#cuántas temperáturas son distintas? 5, eso quiere decir
#que la 2da observa es un duplicado de la 1era
a <- data.frame(edad = seq(19.6,1,length.out = 1))
predict(NOM_1, a)
#

 
########################################            #TD3
   #1
pre<-c(40,80,40,80,40,80,40,80)
dur<-c(6,6,8,8,6,6,8,8)
qua<-c(10,10,10,10,15,15,15,15)
#reponse<-c(56,98,63,102,54,98,65,104)
#mean(reponse)-
#collage<-lm(reponse ~ pre+dur+qua)
#summary(collage)
#on ramange les valeurs a -1 et 1
pre<-c(-1,1,-1,1,-1,1,-1,1)
dur<-c(-1,-1,1,1,-1,-1,1,1)
qua<-c(-1,-1,-1,-1,1,1,1,1)
reponse<-c(56,98,63,102,54,98,65,104)
mean(reponse)
collage<-lm(reponse ~ pre+dur+qua)
summary(collage)

#SST=SSR+SSE
#SSR= [suma (yi estimada - y barra)] ^ 2
SSR<-sum((collage$fitted.values - mean(collage$model$reponse))^2)
#SSE_1<-sum((NOM_1$model$rend - NOM_1$fitted.values)^2)
SST<-sum((collage$model$reponse - mean(collage$model$reponse))^2) #~option+ñ#
SSE=SST-SSR


#########TD CH5 
#Example 1
library(FrF2)
#4 observaciones, 4 tamaño de la muestra, randomize orden de las observaciones
#false para que mantenga el orden de Yates
PlanC<-FrF2(nruns=4,nfactors=2, randomize=FALSE,factor.names=list(Tem=c(-1,1),
                                                                         Conc=c(-1,1)))

#rajouter des expériences au centre
#on utilise l'argument center
PlanCompletC<-FrF2(nruns=4,nfactors=2, randomize=FALSE,ncenter=3,factor.names=list(Tem=c(-1,1),
                                                                                    Vit=c(-1,1),
                                                                                    Lev=c(-1,1)))
#EX2
PlanCompletC<-FrF2(nruns=4,nfactors=3, randomize=FALSE,resolution=3,factor.names=list(Tem=c(-1,1),
                                                                                      Vit=c(-1,1),
                                                                                      Lev=c(-1,1)))

PlanCompletNC<-FrF2(nruns=4,nfactors=3, randomize=FALSE,resolution=3,factor.names=list(Tem=c(200,250),
                                                                                    Vit=c(40,50),
                                                                                    Lev=c(10,15)))

#la commande resolution peut etre omisi ici vu que le nombre d'experiences a ete precise (dans ce cas le logiciel
#affecte automatiquement la resoulution a III)

Y<- c(6,7,4,4,5,5,3,2,9,4,7,8,8,8,6,6,6,5,6,3)
PlanCompletCY<-add.reponse(PlanCompletC,Y)
PlanCompletNCY<-add.reponse(PlanCompletNC,Y)

PlanC<-FrF2(nfactors=6,
            nruns=8,
            randomize=FALSE,
            generators=c("ABC","BC","AC"),
            factor.names=list(Rot=c(-1,1),The=c(-1,1),Mas=c(-1,1),
                              Dis=c(-1,1),Mat=c(-1,1),Tur=c(-1,1)))

PlanNC<-FrF2(nfactors=6,
            nruns=8,
            randomize=FALSE,
            generators=c("ABC","BC","AC"),
            factor.names=list(Rot=c(2,4),The=c(1,2),Mas=c(150,300),
                              Dis=c(0,5),Mat=c('ver','pla'),Tur=c('mar','arr')))


#Convention pour les facteurs 5 et 6 : -1 code les modalités verre et marche
#Commande generatir optionnelle utilisée ici afin d'imposer les trois générateurs particuliers données (voir la suite)

#Construction automatique du plan d'expérience factoriel sous forme codée (exercice 1)
#plan factoriel seul
PlanC<-FrF2(nruns=4,nfactors=2, randomize=FALSE,factor.names=list(Tem=c(-1,1),
                                                                  Conc=c(-1,1)))
#Rajouter des expériences centrales
PlanCompletC<-FrF2(nruns=4,nfactors=2, randomize=FALSE,ncenter=3,factor.names=list(Tem=c(-1,1),
                                                                         Conc=c(-1,1)))
Y=c(48,68,77,76,64,66,65)
PlanCompletCY<-add.response(PlanCompletC,Y)
#LinearModel<-Y COMO tEM+Con+Tem*Con
predict(LinearModel)
residuals(LinearModel)


##14 octobre vendredi
PlanC<-FrF2(nfactors=4,
             nruns=16,
             randomize=FALSE,
             factor.names=list(Dur=c(-1,1),Tem=c(-1,1),Pre=c(-1,1),
                               Con=c(-1,1)))
PlanNC<-FrF2(nfactors=4,
             nruns=16,
             randomize=FALSE,
             factor.names=list(Dur=c(30,60),Tem=c(80,120),Pre=c(4,6),
                               Con=c(10,30)))

PlanCompletC<-FrF2(nfactors=4,
            nruns=16,
            randomize=FALSE,
            ncenter=3,
            factor.names=list(Dur=c(-1,1),Tem=c(-1,1),Pre=c(-1,1),
                              Con=c(-1,1)))
PlanNC<-FrF2(nfactors=4,
             nruns=16,
             randomize=FALSE,
             ncenter=3,
             factor.names=list(Dur=c(30,60),Tem=c(80,120),Pre=c(4,6),
                               Con=c(10,30)))


Y<-c(12.4,7.2,16.5,11.2,14.1,28.9,17.1,28.8,23.8,18.9,16.4,12.1,24,39.4,18.5,30.2,24.8,21.2,19.4)
PlanCompletCY<-add.response(PlanCompletC,Y)

LinearModel<-Y ~ ((Dur+Tem+Pre+Con)^2, data =PlanCompletCY)

anova(LinearModel)

RsmModel<- rsm(Y ~ FO(Dur,Tem,Pre,Con)+TWI(Dur,Tem,Pre,Con) data = PlanCompletCY)
summary(RsmModel)


#a partir de que p value se pone una estrella en r, es decir se vuelve significativo este valor
#tdos tienen la misma varianza?
#rep grafica de efectos de interaccion, las baritas seran paralelas


runif(1,-1,1)

########################################            #TD7
#1
h<-10
c1<-c(h,0,0,4,4,0)
c2<-c(0,4,0,0,0,0)
c3<-c(0,0,4,0,0,0)
c4<-c(4,0,0,4,4,0)
c5<-c(4,0,0,4,4,0)
c6<-c(0,0,0,0,0,4)

tXX<-matrix(cbind(c1,c2,c3,c4,c5,c6),6,6)
a<-seq(0,sqrt(2), by=0.2)
y<-c()
alphas<-c()
for (i in a) {
  tXX<-matrix(cbind(c1,c2,c3,c4,c5,c6),6,6)
  alpha<-i
  alphadiag2<- 2*((alpha)^2)  
  alphadiag4<- 2*((alpha)^4)  
  #print(alphadiag)
tXX[4,1]<- tXX[4,1]+ alphadiag2
tXX[5,1]<- tXX[5,1]+ alphadiag2
tXX[2,2]<- tXX[2,2]+ alphadiag2
tXX[3,3]<- tXX[3,3]+ alphadiag2
tXX[1,4]<- tXX[1,4]+ alphadiag2
tXX[4,4]<- tXX[4,4]+ alphadiag4
tXX[1,5]<- tXX[1,5]+ alphadiag2
tXX[5,5]<- tXX[5,5]+ alphadiag4
#print(tXX)
det0<-det(tXX)
det<-(det0)^(1/6)
y<-c(y,det)
alphas<-c(alphas,alpha)
#print(paste0("det0 ",det0,i))
#print(det)
print(alphas)
print(y)
}
plot(alphas,y)

############
#2
c1<-c(h,0,0,8,8,0)
c2<-c(0,8,0,0,0,0)
c3<-c(0,0,8,0,0,0)
c4<-c(8,0,0,12,4,0)
c5<-c(8,0,0,4,12,0)
c6<-c(0,0,0,0,0,4)
tXX<-matrix(cbind(c1,c2,c3,c4,c5,c6),6,6)
inversa<-solve(tXX)

fact1<-c(-1,1,-1,1,sqrt(2),-sqrt(2),0,0,0,0)
fact2<-c(-1,-1,1,1,0,0,srqt(2),-sqrt(2),0,0)
reponse<-c(12.4,10.2,8.6,6.8,5,7.5,10.2,14.2,4.3,4.5)

#####
#cc
b0<-rep(1,4)
d1<-matrix(c(-1,-1,1,-1,-1,1,1,1),4,2,byrow = TRUE)
d2<-matrix(c(1,0,-1,0,0,1,0,-1),4,2,byrow = TRUE)
d3<-matrix(c(-1,1,-1,-1,1,0,0,0),4,2,byrow = TRUE)
x1<-cbind(b0,d1)
x2<-cbind(b0,d2)
x3<-cbind(b0,d3)
x4<-cbind(b0,d4)
d02<-det(t(x2)%*%x2)
d03<-det((t(x3)%*%x3))
d04<-det((t(x4)%*%x4))
d4<-matrix(c(-1,1,"a","-a",-1,1,0,0),4,2,byrow = TRUE)

yexamen<-c(28,66,16,52,38)
SST0=(yexamen-40)^2
sum(SST0)