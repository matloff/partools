library(partools)

airline_file = "~/data/airline_delay.csv"

cls = makeCluster(2)
setclsinfo(cls)


# Sort on airport, the 5th column
# Takes 1.3 secs for fread and 5.4 secs for read.table
# Data has 223000 rows and 22 cols

time_read_table = system.time(
    filesort(cls, airline_file, 5, "airline_delay"
         , ndigs = 1, header = TRUE, sep = ",", usefread = FALSE
         , stringsAsFactors = FALSE
         ))

time_fread = system.time(
    filesort(cls, airline_file, 5, "airline_delay"
         , ndigs = 1, header = TRUE, sep = ",", usefread = TRUE
         , stringsAsFactors = FALSE
         ))

clusterEvalQ(cls, head(airline_delay))
