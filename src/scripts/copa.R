# uncomment and run the "install.packages()" the first time
# for more info about ggplot2, see http://had.co.nz/ggplot2/
#install.packages("ggplot2")
library(ggplot2)

# point this to your Hadoop "output" directory
dat_folder <- "out"  # if pwd is root of project

# examine the "tree" results
d <- read.table(file=paste(dat_folder, "tree/part-00000", sep="/"), sep="\t", quote="", na.strings="NULL", header=TRUE, encoding="UTF8")
dim(d)
head(d)
nrow(d)

# CoPA GIS dataset does not list tree heights, but we can estimate the distribution
d$avg_height <- (d$max_height + d$min_height) / 2
m <- ggplot(d, aes(x=avg_height))
m <- m + ggtitle("Estimated Tree Height (meters)")
m + geom_histogram(aes(y = ..density.., fill = ..count..)) + geom_density()

# what are the N most popular trees?
x <- as.data.frame(head(sort(table(d$species), decreasing=TRUE), n=10))
x

# examine the "tree X road segment" results
d <- read.table(file=paste(dat_folder, "shade/part-00000", sep="/"), sep="\t", quote="", na.strings="NULL", header=TRUE, encoding="UTF8")
dim(d)
head(d)
nrow(d)

summary(d$tree_dist)
m <- ggplot(d, aes(x=tree_dist))
m <- m + opts(title = "Tree-to-Road Distance (meters)")
m + geom_histogram(aes(y = ..density.., fill = ..count..)) + geom_density()
