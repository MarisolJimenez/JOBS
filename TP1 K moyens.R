#rm(list=ls())


###################### 
# 1. Comme d’habitude, les individus sont en ligne et les variables en colonne.
#Expliquez comment sont genere ces donnees et representez le nuage de points 
#correspondant. Combien de groupes voyez-vous ?

n <- 200 # variable fixée
x1 <- rnorm(n/2) #obtenir un échantillon qui a la taille de la moitié du n; 
                  #qui suive la loi normal
y1 <- rnorm(n/2) #obtenir un échantillon qui a la taille de la moitié du n
                 #qui suive la loi normal
x2 <- rnorm(n/2, 4, 2)  #obtenir un échantillon qui a la taille de la moitié du n
#qui suive la loi normal avec des paramétres 4 et 2
y2 <- rnorm(n/2, 4, 2) #obtenir un échantillon qui a la taille de la moitié du n
#qui suive la loi normal avec des paramétres 4 et 2
d <- data.frame(c(x1, x2), c(y1, y2)) #créer une petite bd avec des valeurs juste obtenue
#x2 abajo de x1 la 2da columna es y, nuevamente y1 y debajo y2
names(d) <- c("x", "y")  #nom des columns

#Comme d’habitude, les individus sont en ligne et les variables en colonne.
#Expliquez comment sont genere ces donnees et representez le nuage de points 
#correspondant. Combien de groupes voyez-vous ?
plot(d)


######################
# 2. Nous allons maintenant utiliser la fonction kmeans pour identifier 
#automatiquement deux groupes “homogenes” dans les donneees. Ideealement, parmi 
#toutes les partitions possibles en deux classes, on souhaite obtenir la 
#partition qui a la plus petite inertie intraclasse dentro de clase (ou,de facon  ́equivalente,
#la plus forte inertie interclasse entre clases). Le code R `a utiliser est :
km <- kmeans(d, 2)
km
#se obtuvieron 93 datos en el 1er cluster; entonces hay 7 datos que pertenecen
#a la primera muestra que fueron clasificados con datos de la segunda muestra
#cuyo comportamiento es una normal con media 4; justamente los valores extremos del
#grupo 1 fueron los que se adhirieron al grupo 2
km$cluster
#La partie “Clustering vector” de la sortie donne la partition obtenue. Verifiez
#que cela est ́equivalent au vecteur km$cluster.

######################
# 3.Nous allons maintenant representer le resultat de la classification dans le 
#plan :
plot(d, pch="") # phc no grafica nada
text(d, label=km$cluster)
#La classification obtenue automatiquement a-t-elle du sens d’apres vous ? 
#Essayez aussi
plot(d, col=km$cluster)

#4. Les centres de gravite des classes sont donnes par 
km$centers 
#et leurs tailles par, cardinalidad
km$size
#Si on divise km$withinss par km$size, on obtient l’inertie de chaque classe. Laquelle des deux 
#classes a la plus forte inertie ? la 2eme classe est mieux. Est-ce que cela est conforme a 
#l’intuition ? Expliquez : Oui, pc il sont tous plus proches les uns de les autres
inertie_chaque_class<-km$withinss/km$size
km$withinss/km$size
#Calculez l’inertie intraclasse qui est la moyenne des inerties des classes 
#ponderes par la proportion d’individus dans les classes. Verifiez que cela est egal a 
km6$tot.withinss/n
#L’inertie intraclasse est donc donnee par km$tot.withinss/n.

#5. L’inertie interclasse est quant a elle donne par 
km$betweenss/n
#Calculez l’inertie totale
inertie_total<-(km$tot.withinss/n)+(km$betweenss/n)
inertie_total


#6.- C’est uniquement parce que l’on travaille sur des donnees bivariees que l’on a pu ici 
#suggerer un nombre de classes pertinent. En general, on ne sait pas quelle valeur donner au 
#deuxieme argument de kmeans. Une approche possible consiste a essayer plusieurs valeurs 
#consecutives pour le nombre de classes est a regarder ĺ'evolution de l’inertie intraclasse en
#fonction du nombre de classes (ou, de facon  equivalente, l’ ́evolution de l’inertie interclasse 
#en fonction du nombre de classes). Faites varier le nombre de classes et, lors de chaque 
#execution, calculez l’inertie intraclasse, l’inertie interclasse et l’inertie totale. Par exemple :


km1 <- kmeans(d, 1)
intra1 <- km1$tot.withinss/n
inter1 <-  km1$betweenss/n
tot1 <- intra1 + inter1
tot1
km2 <- kmeans(d, 2)
intra2 <- km2$tot.withinss/n
inter2 <-  km2$betweenss/n
tot2 <- intra2 + inter2

d<-USArrests[-c(22:31),]
d1 <- scale(d, center = TRUE, scale = TRUE)
n<-nrow(d1)
n
#d1<-d[-c(22:31),]
km3 <- kmeans(d1, 3)
intra3 <- km3$tot.withinss/n
intra3
inter3 <-  km3$betweenss/40
tot3 <- intra3 + inter3

km4 <- kmeans(d, 4)
intra4 <- km4$tot.withinss/n
inter4 <-  km4$betweenss/n
tot4 <- intra4 + inter4

km5 <- kmeans(d,5 )
intra5 <- km5$tot.withinss/n
inter5 <-  km5$betweenss/n
tot5 <- intra5 + inter5
#km2 <- ...
#Representez l’evolution des trois inerties en tapant :
plot(c(intra1,intra2,intra3,intra4,intra5),type="l") 
points(c(inter1,inter2,inter3,inter4,inter5),type="l",lty=2) 
points(c(tot1,tot2,tot3,tot4,tot5),type="l",lty=3)
#Combien de classes suggeerez-vous de prendre ? Comme en ACP, on recherchera des coudes dans ce 
#graphique.
#2 pues es donde vemos un cambio más drastico tanto en inter como intra
#el 2do caso seria 4 pues es el ultimo donde se ve un cambio sustancial, que valga la pena
#para inercia total vemos que no hace muhca diferencia
#esto se ve en los codos de inter e intra

#7. Pour terminer, representez la partition en trois classes dans le plan. 
#Que pourrait-on re- procher a cette partition ?

plot(d, pch="") # phc no grafica nada
text(d, label=km3$cluster,col = km3$cluster)
km3$size
#los datos que vienen de una normal con var 4 y media 2 se dividieron en 2 subgrupos,
#aqui obtenemos los datos de la normal estandar se agruparon perfectamente


#########
#2 Les donnees USArrests
#i)Chargez les donnees USArrests. Pour eviter que les diferences de dispersion entre variables 
#n’affectent trop la classification automatique, travaillez sur les donnees centrees-reduites.
data(USArrests)
head(USArrests)
d <- scale(USArrests, center = TRUE, scale = TRUE)
d1<-d[-c(22:31),]
d1
km6 <- kmeans(d1, 3)
#ii). Appliquez la fonction kmeans aux donnes pour 2 a 6 classes et comparez les partitions en 
#terme d’inertie intraclasse. Combien de classes proposez-vous de garder ? 2 es donde hay un cambio considerable
km<-c()
for (i in 1:6){
  km[i]<-kmeans(d,i)
}
km6 <- kmeans(d, 6)
intra6 <- km6$tot.withinss/n
inter6 <-  km6$betweenss/n
tot6 <- intra6 + inter6

#iii). Caracterisez et interpretez chaque classe en examinant son centre de gravite et son inertie.
#Les centres de gravite des classes sont donnes par 
km2$centers 

inertie_chaque_class<-km2$withinss/km2$size
inertie_chaque_class
#iv)Effectuez une ACP des donnees et repreesentez les classes obtenues par kmeans dans les plans 
#factoriels retenus afin d’inspecter visuellement la qualite de la classification.

plot(c(intra1,intra2,intra3,intra4,intra5,intra6),type="l") 
points(c(inter1,inter2,inter3,inter4,inter5,inter6),type="l",lty=2) 
points(c(tot1,tot2,tot3,tot4,tot5,tot6),type="l",lty=3)

plot(d, pch="") # phc no grafica nada
text(d, label=km2$cluster,col = km2$cluster)