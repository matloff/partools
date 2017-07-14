# An example of using quantile regression
# See Github issue 15 by @znmeb

library(partools)
library(quantreg)

cls <- makeCluster(2)
setclsinfo(cls)

set.seed(73129)

n = 10000
p = 50
true_coefs = 1:p
randomdata = matrix(rnorm(n * p), nrow = n)
response = data.frame(y = randomdata %*% true_coefs + rnorm(n))

d = cbind(response, randomdata)

fit = rq(y ~ ., data = d)

fit2 = ca(cls, d, rq, coef)

# Comparing coefficients of serial versus parallel computations
diff = coef(fit) - fit2$tht
hist(diff)
