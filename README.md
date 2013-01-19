CMU Workshop on Cascading + City of Palo Alto Data Open Data
============================================================
We have built an example app in [Cascading](http://www.cascading.org/) and [Apache Hadoop](http://hadoop.apache.org/), based on the City of Palo Alto open data provided via Junar: [http://paloalto.opendata.junar.com/dashboards/7576/geographic-information/](http://paloalto.opendata.junar.com/dashboards/7576/geographic-information/)

Students can extend the example workflow to build derivative apps, or use it as a starting point for other ways to leverage this data.

We will also draw some introductory material from these two previous talks:

* ["Intro to Data Science"](http://www.slideshare.net/pacoid/intro-to-data-science-for-enterprise-big-data)
* ["Cascading for the Impatient"](http://www.slideshare.net/pacoid/cascading-for-the-impatient)

For more details, please read the accompanying [wiki page](https://github.com/Cascading/CoPA/wiki).


Build Instructions
==================
To generate an IntelliJ project use:

    gradle ideaModule

To build the sample app from the command line use:

    gradle clean jar

Before running this sample app, be sure to set your `HADOOP_HOME` environment variable. Then clear the `output` directory. To run on a desktop/laptop with Apache Hadoop in standalone mode:

    rm -rf output
    hadoop jar ./build/libs/copa.jar data/copa.csv data/meta_tree.tsv data/meta_road.tsv data/gps.csv output/trap output/tsv output/tree output/road output/park output/shade output/reco

To view the results, for example the output recommendations in `reco`:

    ls output
    more output/reco/part-00000

An example of log captured from a successful build+run is at [https://gist.github.com/3660888](https://gist.github.com/3660888)

To run the R script, load `src/scripts/copa.R` into [RStudio](http://rstudio.org/) or from the command line run:

    R --vanilla -slave < src/scripts/copa.R

...and then check output in the file `Rplots.pdf`


Cascalog Build
==============
See the Leiningen build script in `project.clj` and the Cascalog source in the `src/main/clj/copa` directory.

To build and run:

    lein clean
    lein uberjar
    rm -rf output/ 
    hadoop jar ./target/copa.jar data/copa.csv data/meta_tree.tsv data/meta_road.tsv data/gps.csv \
      out/trap out/park out/tree out/road out/shade out/gps out/reco


About Cascading
===============
There is a tutorial about getting started with Cascading in the blog post series called [Cascading for the Impatient](http://www.cascading.org/category/impatient/). Other documentation is available at [http://www.cascading.org/documentation/](http://www.cascading.org/documentation/).

For more discussion, see the [cascading-user](https://groups.google.com/forum/?fromgroups#!forum/cascading-user) email forum 
or check out one of our [meetups](http://www.meetup.com/cascading/).
