(ns copa.core
  (:use [cascalog.api]
        [cascalog.more-taps :only (hfs-delimited)])
  (:require [clojure.string :as s]
            [cascalog.ops :as c]
            [clojure-csv.core :as csv])
  (:gen-class))


(defn parse-gis [line]
  (first (csv/parse-csv line))
 )


(defn -main [in out trap & args]
  (?<- (hfs-delimited out)
       [?blurb ?misc ?geo ?kind]
       ((hfs-delimited in) ?line)
       (:trap (hfs-textline trap))
       (parse-gis ?line :> ?blurb, ?misc, ?geo, ?kind)
  ))
