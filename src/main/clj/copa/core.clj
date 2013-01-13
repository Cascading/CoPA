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


(defn get-parks [src trap]
  "filter/parse the park data"
  (<- [?blurb, ?misc, ?geo, ?kind]
      (src ?blurb, ?misc, ?geo, ?kind)
      (re-matches
        #"\s+Community Type\:\s+Park.*"
        ?misc)
   )
 )


(defn parse-tree [misc]
  "parse the special fields in the tree format"
  (let [x (re-seq
    #"^\s+Private\:\s+(\S+)\s+Tree ID\:\s+(\d+)\s+.*Situs Number\:\s+(\d+)\s+Tree Site\:\s+(\d+)\s+Species\:\s+(\S.*\S)\s+Source.*"
    misc)]
    (> (count x) 0)
    (> (count (first x)) 1)
    (first x))
 )


(defn get-trees [src trap]
  "filter/parse the tree data"
  (<- [?blurb, ?misc, ?geo, ?kind ?priv, ?tree_id, ?situs, ?tree_site, ?tree_species]
      (src ?blurb, ?misc, ?geo, ?kind)
      (re-matches #"^\s+Private\:\s+(\S+)\s+Tree ID\:\s+.*" ?misc)
      (parse-tree ?misc :> _ ?priv, ?tree_id, ?situs, ?tree_site, ?raw_species)
      ((c/comp s/trim s/lower-case) ?raw_species :> ?tree_species)
      (:trap (hfs-textline trap))
   )
 )


(defn -main [in meta_tree meta_road trap park tree & args]
  (let [gis (hfs-delimited in)
        tree_meta (hfs-delimited meta_tree :skip-header? true)
        road_meta (hfs-delimited meta_road :skip-header? true)
        src (etl-gis gis trap)]
    (?- (hfs-delimited park)
        (get-parks src trap)
     )
    (?- (hfs-delimited tree)
        (get-trees src trap)
     )
   )
 )
