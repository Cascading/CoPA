dat_folder <- '~/src/concur/CoPA/out/shade'
data <- read.table(file=paste(dat_folder, "part-00000", sep="/"), sep="\t", 
                   quote="", na.strings="NULL", header=FALSE, encoding="UTF8")

colnames(data) <- c("road_name", "sum_tree_moment", "albedo", "geohash", 
                    "road_lat","road_lng", "road_alt", 
                    "traffic_count", "traffic_class")

head(data)
summary(data)

summary(data$sum_tree_moment)
plot(ecdf(data$sum_tree_moment))

summary(data$albedo)
plot(ecdf(data$albedo))

summary(data$road_lat)
summary(data$road_lng)

summary(data$traffic_count)
hist(data$traffic_count)

x <- log(data$traffic_count / 200) / 5
boxplot(x)

summary(as.factor(data$traffic_index))
plot(summary(as.factor(data$traffic_index)))

summary(as.factor(data$traffic_class))
plot(summary(as.factor(data$traffic_class)))
