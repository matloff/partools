library(partools)

cls = makeCluster(2)
setclsinfo(cls)

# Sort on airport, the 5th column
t_partools = system.time(
    filesort(cls, "~/data/airline_delay.csv", 5, "~/data/airline_delay2.csv"
         , ndigs = 1, header = TRUE, sep = ",", usefread = TRUE
         , stringsAsFactors = FALSE
         ))
