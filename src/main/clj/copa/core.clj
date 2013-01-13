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
  (first (csv/parse-csv line))
 )


(defn -main [in out trap & args]
  (?<- (hfs-delimited out)
       [?blurb ?misc ?geo ?kind]
       ((hfs-delimited in) ?line)
       (parse-gis ?line :> ?blurb, ?misc, ?geo, ?kind)
       (:trap (hfs-textline trap))
       (:distinct false)
       (re-matches #"\s+Community Type\:\s+Park.*" ?misc)
   )
 )
