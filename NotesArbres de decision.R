#8 nov
#elementos totales en el nivel
tot1<-c(98,178)#distribucion papa
hoja1<-c(9,48) #dist en hojas
hoja2<-c(15,13)
hoja3<-c(74,117)
tot<-sum(tot1) #total en el papa, total de la suma de hojas tambn
#tot
#hojas_tot<-matrix(rbind(sum(hoja1),sum(hoja2),sum(hoja3)))
prop_1<-sum(hoja1)/tot #proportion en cada hoja respecto a todo el nivel
prop_2<-sum(hoja2)/tot
prop_3<-sum(hoja3)/tot
prop_papa<-tot1[1]/tot

#obtener el gain de elegir esa rama
gain<-sh(prop_papa)-(prop_1 * sh(hoja1[1]/sum(hoja1))+prop_2 * sh(hoja2[2]/sum(hoja2))+prop_3 *sh(hoja3[2]/sum(hoja3)))
gain

#Shannon fonction

sh<-function (x) {
  ifelse(x==0 | x==1,
         0,
  (-x * log(x) - (1-x)*log(1-x)) / log(2))
}

curve(sh(x),0,1)


tot1<-c(3,5)#distribucin papa
hoja1<-c(3,2)
hoja2<-c(0,3)
#hoja3<-c(7,123)
tot<-sum(tot1)
tot
prop_1<-sum(hoja1)/tot
prop_2<-sum(hoja2)/tot
prop_3<-sum(hoja3)/tot
prop_papa<-tot1[1]/tot

gain<-sh(prop_papa)-(prop_1 * sh(hoja1[1]/sum(hoja1))+prop_2 * sh(hoja2[2]/sum(hoja2))+prop_3 *sh(hoja3[2]/sum(hoja3)))
gain
#evaluqmos el 1er arbol de decision; donde partimos de M est 0.2657121 = 0.27
#sh(3/8)-(3/8 * sh(1)+3/8 * sh(2)+2/8 *sh(3))
sh(3/8)-(3/8 * sh(1/3)+3/8 * sh(2/3)+2/8 *sh(0))

#evaluqmos el 2do arbol de decision; donde partimos de A #0.45
sh(3/8)-(1/8 * sh(1)+4/8 * sh(2/4)+3/8 *sh(0))
#

sh(3/8)-(1/8 * sh(1)+4/8 * sh(2/4)+3/8 *sh(0))

#evaluqmos el 2do arbol de decision; donde partimos de R #0.015

#evaluqmos el 2do arbol de decision; donde partimos de E #0.34