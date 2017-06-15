library(partools)

cls = makeCluster(2)
setclsinfo(cls)

# Sort on airport, the 5th column
time_partools = system.time(
    filesort(cls, "~/data/airline_delay.csv", 5, "airline_delay"
         , ndigs = 1, header = TRUE, sep = ",", usefread = FALSE
         , stringsAsFactors = FALSE
         ))


clusterEvalQ(cls, head(airline_delay))
