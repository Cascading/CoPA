(defproject cascading-copa "0.1.0-SNAPSHOT"
  :description "City of Palo Alto Open Data recommender in Cascalog"
  :url "https://github.com/Cascading/CoPA"
  :license {:name "Apache License, Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"
            :distribution :repo}
  :uberjar-name "copa.jar"
  :aot [copa.core]
  :main copa.core
  :min-lein-version "2.0.0"
  :source-paths ["src/main/clj"]
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [cascalog "1.10.1-SNAPSHOT"]
                 [cascalog-more-taps "0.3.1-SNAPSHOT"]
                 [clojure-csv/clojure-csv "2.0.0-alpha2"]
                 [org.clojars.sunng/geohash "1.0.1"]
                 [date-clj "1.0.1"]]
  :exclusions [org.clojure/clojure]
  :profiles {:dev {:dependencies [[midje-cascalog "0.4.0"]]}
             :provided {:dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]]}})
