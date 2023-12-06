N <- 400 # number of points per class
D <- 2 # dimensionality
K <- 2 # number of classes
X <- data.frame() # data matrix (each row = single example) Y <- data.frame() # class labels
set.seed(308)
for (j in (1:2)) {
  r <- seq(0.05, 1, length.out = N) # radius
  t <- seq((j - 1) * 4.7, j * 4.7, length.out = N) + rnorm(N, sd = 0.3) # theta
  Xtemp <- data.frame(x = r * sin(t), y = r * cos(t))
  ytemp <- data.frame(matrix(j, N, 1))
  X <- rbind(scale(X), Xtemp)
  Y <- rbind(Y, ytemp)
}
data <- cbind(X, Y)


PlanCompletC<-ccd(2,n0 = c(0,3),randomize = FALSE, oneblock = TRUE,alpha = "rotable",coding = list(x1~Pre,x2~Rat))
Y<-c(6.66,7.58,7.27,8.33,6.49,8.04,7.16,8.19,7.87,7.69,7.9)
LinearModel<-lm(Y~(x1+x2)^2 +  I(x1^2) + I(x2^2), data = PlanCompletC)
summary(LinearModel)

RsmModel<-rsm(Y~FO(x1,x2) + TWI(x1,x2) PQ(x1,x2), data = PlanCompletC)
summary(RsmModel)
predict(RsmModel)
residuals(RsmModel)

contour(RsmModel, ~x2*x1,image = TRUE)




PlanCompletC<-bbd(3,n0 = 3,randomize = FALSE, coding = list(x1~Dil,x2~Sta,x3~Con))
Y<-c(42.2,50.4,51.3,42.6,40.7,41.3,41.5,40.8,35.3,39.5,39.8,35.2,50.8,50.1,49.4)
LinearModel<-lm(Y~(x1+x2+x3)^2 +  I(x1^2) + I(x2^2)+ I(x3^2), data = PlanCompletC) #interaction
summary(LinearModel)

RsmModel<-rsm(Y~FO(x1,x2,x3) + TWI(x1,x2,x3) + PQ(x1,x2,x3), data = PlanCompletC)
summary(RsmModel) #on a les minimums en eigenanalysis #no es utilizable pq no todo slos elementos tienen el mismo signo
predict(RsmModel)

#on cherce extrema sous contrainte, bajo restricción avec RIDGE REGRESSION
 #le minimum globla est à rechercher à la limite du domaine experimental
