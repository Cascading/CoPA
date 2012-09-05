CMU Workshop on Cascading + City of Palo Alto Data
=================================================
We built an example app in [Cascading](http://www.cascading.org/) and [Apache Hadoop](http://hadoop.apache.org/), based on the City of Palo Alto open data provided via Junar: [http://paloalto.opendata.junar.com/dashboards/7576/geographic-information/](http://paloalto.opendata.junar.com/dashboards/7576/geographic-information/)

Students can extend the example workflow to build derivative apps, or use it as a starting point for other ways to leverage this data.

We will draw some introductory material from these two previous talks:

* ["Intro to Data Science"](http://www.slideshare.net/pacoid/intro-to-data-science-for-enterprise-big-data)
* ["Cascading for the Impatient"](http://www.slideshare.net/pacoid/cascading-for-the-impatient)

Example App
-----------
We used some of the CoPA open data for parks, roads, trees, etc., and have shown how to Cascading and Hadoop to clean up the raw, unstructured download. Based on that initial ETL workflow, we get geolocation + metadata for each item of interest:

* trees w/ species
* road pavement w/ traffic conditions
* parks

One use case could be *“Find a shady spot on a summer day in which to walk near downtown Palo Alto. While on a long conference call. Sippin’ a [latte](http://www.coupacafe.com/) or enjoying some [fro-yo](http://www.fraicheyogurt.com/).”*
In other words, we could determine estimates for [albedo](http://en.wikipedia.org/wiki/Albedo#Trees) vs. relative shade.
Perhaps as the starting point for a mobile killer app. Or something.

Additional data is included [here](https://docs.google.com/spreadsheet/ccc?key=0AmyGHRB4Ee9KdEs0LTR1aHYxN1RuVUlDVnRneE5PeGc), to be joined with the cleaned-up CoPA data about trees and roads.
We will also use log data collected using [GPS Tracks](http://gpstracksapp.com/).

Relevant data science aspects include:
* [Bayesian point estimates](http://en.wikipedia.org/wiki/Point_estimation#Bayesian_point-estimation) on the GPS logs
* [Kriging](http://en.wikipedia.org/wiki/Kriging) the geo distribution of estimated metrics
* [Dirichlet tessellation](http://mathworld.wolfram.com/VoronoiDiagram.html) to optimize recommendations, if someone feels especially ambitious, etc.

Caveats
-------
* data quality: some species names have spelling errors or misclassifications
* missing data
* needs: common names for trees, photos, natives vs. invasives, toxicity, etc.

Enriching Data
--------------
We could combine this CoPA open data with access to external APIs:

*  [Trulia](http://developer.trulia.com/page/gallery) neighborhood data, housing prices [uses Cascading]
*  [Factual](http://developer.factual.com/display/docs/Factual+Developer+APIs+Version+3) local business (FB Places, etc.) [uses Cascading]
*  [Google](https://developers.google.com/maps/documentation/geocoding/index) geocoding
*  [Wunderground](http://www.wunderground.com/weather/api/) local weather data    
*  [WalkScore](http://www.walkscore.com/professional/walk-score-apis.php) neighborhood data, walkability
*  [Beer](http://beermapping.com/api/) need we say more?
*  [Data.gov](http://geo.data.gov/geoportal/catalog/gallery/gallery.page) US federal open data
*  [Data.NASA.gov](http://data.nasa.gov/) NASA open data
*  [DBpedia](http://wiki.dbpedia.org/Datasets) datasets derived from Wikipedia
* [CommonCrawl](http://commoncrawl.org/) open source full web crawl
* [GeoWordNet](http://geowordnet.semanticmatching.org/) semantic knowledge base about localized terminology
* [CityData](http://www.city-data.com/) US city profiles
* [Geolytics](http://www.geolytics.com/) demographics, GIS, etc.
* [Foursquare](https://developer.foursquare.com/),
    [Yelp](http://www.yelp.com/developers/getting_started),
    [CityGrid](http://developer.citygridmedia.com/),
    [Localeze](http://www.localeze.com/),
    [YP](http://developer.yp.com/apis)
* [Programmable Web](http://www.programmableweb.com/) API mashup directory
* various photo sharing

Other Potential Use Cases
-------------------------
*  [Trulia](http://www.trulia.com/)

1.  estimate allergy zones, for real estate preferences
2.  optimize sales leads

1.  target sites for conversion to residential solar
2.  target sites for an urban agriculture venture

*  [Calflora](http://www.calflora.org/entry/mycalflora.html):

3.  report observations of natives on endangered species list
4.  report new observations of invasives / toxicology
5.  infer regions of affinity for beneficial insects

*  [City of Palo Alto](http://www.cityofpaloalto.org/)

1.  premium payment / bid system for an open parking spot in the shade
2.  welcome services for visitors (ecotourism, translated park info, etc.)
3.  city planning: expected rates for tree replanting, natives vs. invasives, etc.
4.  liabilities: e.g., oleander (common, highly toxic) near day care centers
5.  epidemiology, e.g. there are outbreaks of disastrous tree diseases -- with big impact on property values

*  community organizations

1.  volunteer events: harvest edibles to donate to shelters

*  start-ups

1.  some of the invasive species are valuable in Chinese medicine and others can be converted to biodiesel -- potential win-win for targeted harvest services

Extending The Data
------------------
Looks like this data would be even more valuable if it included ambient noise levels. Somehow.

Question: How could your new business obtain data for ambient noise levels in Palo Alto?

*  infer from road data
*  infer from bus lines, rail schedule
*  sample/aggregate from mobile devices in exchange for micropayments
*  buy/aggregate data from home security networks
*  fly nano quadrotors, DIY "Street View" for audio
*  fly micro aerostats, with Arduino-based accelerometer and positioned parabolic mic
*  partner with City of Palo Alto to deploy a simple audio sensor grid

App Development Process
-----------------------
1.  Clean up the raw, unstructured data from CoPA download… aka [ETL](http://en.wikipedia.org/wiki/Extract,_transform,_load)
2.  Perform sampling before modeling
3.  Perform visualization and summary statistics in [RStudio](http://rstudio.org/)
4.  Ideation and research for potential use cases
5.  Iterate on business process for the app workflow

1.  [TDD](http://en.wikipedia.org/wiki/Test-driven_development) at scale
2.  [best practices](http://www.cascading.org/category/impatient/)

6.  Integrate with end use cases as the workflow endpoints
7.  …
8.  PROFIT!

Some caveats:
-------------
*  Arguably, this is not a large data set; however, it’s early for the open data initiative, and besides [Palo Alto](http://en.wikipedia.org/wiki/Palo_Alto,_California) has only 65K population.
*  This provides a good area for a POC, prior to deploying in other, larger metro areas.
*  This example helps illustrate how in terms of “Big Data”, complexity is more important to consider than big.

Build Instructions
==================
To generate an IntelliJ project use:

    gradle ideaModule

To build the sample app from the command line use:

    gradle clean jar

Before running this sample app, be sure to set your `HADOOP_HOME` environment variable. Then clear the `output` directory, then to run on a desktop/laptop with Apache Hadoop in standalone mode:

    rm -rf output
    hadoop jar ./build/libs/copa.jar data/copa.csv data/meta_tree.tsv data/meta_road.tsv output/trap output/tsv output/tree output/road output/park

To view the results, for example the cleaned-up trees data:

    ls output
    more output/tree/part-00000

An example of log captured from a successful build+run is at https://gist.github.com/3020297

About Cascading
===============
There is a tutorial about getting started with Cascading in the blog post series called [Cascading for the Impatient](http://www.cascading.org/category/impatient/). Other documentation is available at [http://www.cascading.org/documentation/](http://www.cascading.org/documentation/).

For more discussion, see the [cascading-user](https://groups.google.com/forum/?fromgroups#!forum/cascading-user) email forum. We also have a [meetup](http://www.meetup.com/cascading/) started.

