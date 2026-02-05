f <- function(x) {
   x <- x + 1
   y <- g(x)
   dbsmsg(y)
   x^2 + y^2
}

g <- function(t) {
   if (t > 0) return(5)
   6
}

