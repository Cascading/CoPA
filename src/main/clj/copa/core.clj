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
      (parse-tree ?misc :> _ 
        ?priv, ?tree_id, ?situs, ?tree_site, ?raw_species)
      ((c/comp s/trim s/lower-case) ?raw_species :> ?tree_species)
      (:trap (hfs-textline trap))
   )
 )


(defn parse-road [misc]
  "parse the special fields in the road format"
  (let [x (re-seq
    #"^\s+Sequence\:.*\s+Year Constructed\:\s+(\d+)\s+Traffic Count\:\s+(\d+)\s+Traffic Index\:\s+(\w.*\w)\s+Traffic Class\:\s+(\w.*\w)\s+Traffic Date.*\s+Paving Length\:\s+(\d+)\s+Paving Width\:\s+(\d+)\s+Paving Area\:\s+(\d+)\s+Surface Type\:\s+(\w.*\w)\s+Surface Thickness.*\s+Bike Lane\:\s+(\w+)\s+Bus Route\:\s+(\w+)\s+Truck Route\:\s+(\w+)\s+Remediation.*$"
    misc)]
    (> (count x) 0)
    (> (count (first x)) 1)
    (first x))
 )


(defn get-roads [src trap]
  "filter/parse the road data"
  (<- [?blurb, ?misc, ?geo, ?kind
       ?year_construct ?traffic_count ?traffic_index ?traffic_class ?paving_length ?paving_width
       ?paving_area ?surface_type ?bike_lane ?bus_route ?truck_route]
      (src ?blurb, ?misc, ?geo, ?kind)
      (re-matches #"^\s+Sequence\:.*\s+Year Constructed\:\s+(\d+)\s+Traffic.*" ?misc)
      (parse-road ?misc :> _
        ?year_construct ?traffic_count ?traffic_index ?traffic_class ?paving_length ?paving_width
        ?paving_area ?surface_type ?bike_lane ?bus_route ?truck_route)
      (:trap (hfs-textline trap))
   )
 )


(defn -main [in meta_tree meta_road trap park tree road & args]
  (let [gis (hfs-delimited in)
        tree_meta (hfs-delimited meta_tree :skip-header? true)
        road_meta (hfs-delimited meta_road :skip-header? true)
        src (etl-gis gis (s/join "/" [trap "gis"]))]
    (?- (hfs-delimited park)
        (get-parks src (s/join "/" [trap "park"]))
     )
    (?- (hfs-delimited tree)
        (get-trees src (s/join "/" [trap "tree"]))
     )
    (?- (hfs-delimited road)
        (get-roads src (s/join "/" [trap "road"]))
     )
   )
 )
