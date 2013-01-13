(ns copa.core
  (:use [cascalog.api]
        [cascalog.checkpoint]
        [cascalog.more-taps :only (hfs-delimited)]
   )
  (:require [clojure.string :as s]
            [cascalog [ops :as c] [vars :as v]]
            [clojure-csv.core :as csv]
   )
  (:gen-class))


(defn parse-gis [line]
  "use parse-csv to parse complex CSV format in GIS export"
  (first (csv/parse-csv line))
 )


(defn etl-gis [gis trap]
  "generator to parse fields from the GIS source tap"
  (<- [?blurb ?misc ?geo ?kind]
      (gis ?line)
      (parse-gis ?line :> ?blurb, ?misc, ?geo, ?kind)
      (:distinct false)
      (:trap (hfs-textline trap))
   )
 )


(defn get-parks [src]
  "filter/parse the park data"
  (<- [?blurb, ?misc, ?geo, ?kind]
      (src ?blurb, ?misc, ?geo, ?kind)
      (re-matches #"\s+Community Type\:\s+Park.*" ?misc)
   )
 )


(defn -main [in out trap & args]
  (let [gis (hfs-delimited in)
        src (etl-gis gis trap)]
    (?- (hfs-delimited out)
        (get-parks src)
     )
   )
 )
