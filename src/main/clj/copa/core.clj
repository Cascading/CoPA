(ns copa.core
  (:use [cascalog.api]
        [cascalog.more-taps :only (hfs-delimited)]
        [clojure.contrib.generic.math-functions]
        [date-clj]
   )
  (:require [clojure.string :as s]
            [cascalog [ops :as c] [vars :as v]]
            [clojure-csv.core :as csv]
            [geohash.core :as geo]
   )
  (:gen-class))


(defn parse-gis [line]
  "leverages parse-csv to parse complex CSV format in the (unclean) GIS export"
  (first (csv/parse-csv line))
 )


(defn etl-gis [gis trap]
  "subquery to parse data sets from the GIS source tap"
  (<- [?blurb ?misc ?geo ?kind]
      (gis ?line)
      (parse-gis ?line :> ?blurb ?misc ?geo ?kind)
      (:trap (hfs-textline trap))
   ))


(defn avg [a b]
  "calculates the average of two decimals"
  (/ (+ (read-string a) (read-string b)) 2.0)
 )


(defn geohash [lat lng]
  "calculates a geohash, at a common resolution"
  (geo/encode lat lng 6)
  )


(defn parse-tree [misc]
  "parses the special fields in the tree format"
  (let [x (re-seq
    #"^\s+Private\:\s+(\S+)\s+Tree ID\:\s+(\d+)\s+.*Situs Number\:\s+(\d+)\s+Tree Site\:\s+(\d+)\s+Species\:\s+(\S.*\S)\s+Source.*"
    misc)]
    (> (count x) 0)
    (> (count (first x)) 1)
    (first x)
    ;; backflips to trap data quality issues in the GIS export
   ))


(defn geo-tree [geo]
  "parses geolocation for tree format"
  (let [x (re-seq #"^(\S+),(\S+),(\S+)\s*$" geo)]
    (> (count x) 0)
    (> (count (first x)) 1)
    (first x)
    ;; backflips to trap data quality issues in the GIS export
   ))


(defn get-trees [src trap tree_meta]
  "subquery to parse/filter the tree data"
  (<- [?blurb ?tree_id ?situs ?tree_site
       ?species ?wikipedia ?calflora ?avg_height
       ?tree_lat ?tree_lng ?tree_alt ?geohash
       ]
      (src ?blurb ?misc ?geo ?kind)
      (re-matches #"^\s+Private.*Tree ID.*" ?misc)
      (parse-tree ?misc :> _ ?priv ?tree_id ?situs ?tree_site ?raw_species)
      ((c/comp s/trim s/lower-case) ?raw_species :> ?species)
      (tree_meta ?species ?wikipedia ?calflora ?min_height ?max_height)
      (avg ?min_height ?max_height :> ?avg_height)
      (geo-tree ?geo :> _ ?tree_lat ?tree_lng ?tree_alt)
      (read-string ?tree_lat :> ?lat)
      (read-string ?tree_lng :> ?lng)
      (geohash ?lat ?lng :> ?geohash)
      (:trap (hfs-textline trap))
   ))


(defn parse-road [misc]
  "parses the special fields in the road format"
  (let [x (re-seq
            #"^\s+Sequence.*Traffic Count\:\s+(\d+)\s+Traffic Index\:\s+(\w.*\w)\s+Traffic Class\:\s+(\w.*\w)\s+Traffic Date.*\s+Paving Length\:\s+(\d+)\s+Paving Width\:\s+(\d+)\s+Paving Area\:\s+(\d+)\s+Surface Type\:\s+(\w.*\w)\s+Surface Thickness.*\s+Overlay Year\:\s+(\d+)\s.*Bike Lane\:\s+(\w+)\s+Bus Route\:\s+(\w+)\s+Truck Route\:\s+(\w+)\s+Remediation.*$"
            misc)]
    (> (count x) 0)
    (> (count (first x)) 1)
    (first x)
    ;; backflips to trap data quality issues in the GIS export
   ))


(defn estimate-albedo [overlay_year albedo_new albedo_worn]
  "calculates an estimator for road albedo, based on road surface age"
  (cond
    (>= (read-string overlay_year) (- (year (today)) 10.0))
      (read-string albedo_new)
    :else
      (read-string albedo_worn)
   ))


(defmapcatop bigram [s]
  "generator for bi-grams, from a space-separated list"
  (partition 2 1 (s/split s #"\s"))
 )


(defn midpoint [pt0 pt1]
  "calculates the midpoint of two geolocation coordinates"
  (let [l0 (s/split pt0 #",")
        l1 (s/split pt1 #",")
        lat0 (read-string (nth l0 1))
        lng0 (read-string (nth l0 0))
        alt0 (read-string (nth l0 2))
        lat1 (read-string (nth l1 1))
        lng1 (read-string (nth l1 0))
        alt1 (read-string (nth l1 2))
        ]
    [ (/ (+ lat0 lat1) 2.0) (/ (+ lng0 lng1) 2.0) (/ (+ alt0 alt1) 2.0) ]
   ))


(defn get-roads [src trap road_meta]
  "subquery to parse/filter the road data"
  (<- [?blurb ?bike_lane ?bus_route ?truck_route ?albedo
       ?min_lat ?min_lng ?min_alt ?geohash
       ?traffic_count ?traffic_index ?traffic_class
       ?paving_length ?paving_width ?paving_area ?surface_type
       ]
      (src ?blurb ?misc ?geo ?kind)
      (re-matches #"^\s+Sequence.*Traffic Count.*" ?misc)
      (parse-road ?misc :> _
        ?traffic_count ?traffic_index ?traffic_class 
        ?paving_length ?paving_width ?paving_area ?surface_type
        ?overlay_year ?bike_lane ?bus_route ?truck_route)
      (road_meta ?surface_type ?albedo_new ?albedo_worn)
      (estimate-albedo ?overlay_year ?albedo_new ?albedo_worn :> ?albedo)
      (bigram ?geo :> ?pt0 ?pt1)
      (midpoint ?pt0 ?pt1 :> ?lat ?lng ?alt)
      ;; why filter for min? because there are geo duplicates..
      (c/min ?lat :> ?min_lat)
      (c/min ?lng :> ?min_lng)
      (c/min ?alt :> ?min_alt)
      (geohash ?min_lat ?min_lng :> ?geohash)
      (:trap (hfs-textline trap))
   ))


(defn get-parks [src trap]
  "subquery to parse/filter the park data"
  (<- [?blurb ?misc ?geo ?kind]
      (src ?blurb ?misc ?geo ?kind)
      (re-matches #"\s+Community Type\:\s+Park.*" ?misc)
   ))


(defn tree-distance [tree_lat tree_lng road_lat road_lng]
  "calculates distance from a tree to the midpoint of a road segment; TODO IMPROVE GEO MODEL"
  (let [y (- (read-string tree_lat) (read-string road_lat))
        x (- (read-string tree_lng) (read-string road_lng))
        ]
    (sqrt (+ (pow y 2.0) (pow x 2.0)))
   ))


(defn road-metric [traffic_class traffic_count albedo]
  "calculates a metric for comparing road segments, approximating a decision tree; TODO USE PMML"
  [[(cond 
      (= traffic_class "local residential") 1.0
      (= traffic_class "local business district") 0.5
      :else 0.0)
    ;; scale traffic_count based on distribution mean
    (/ (log (/ (read-string traffic_count) 200.0)) 5.0)
    (- 1.0 (read-string albedo))
    ]]
    ;; in practice, we'd probably train a predictive model using decision trees, 
    ;; regression, etc., plus incorporate customer feedback, QA of the data, etc.
  )


(defn get-shade [trees roads]
  "subquery to join the tree and road estimates, to maximize for shade"
  (<- [?road_name ?geohash ?road_lat ?road_lng ?road_alt ?road_metric ?tree_metric]
      (roads ?road_name _ _ _ ?albedo ?road_lat ?road_lng ?road_alt ?geohash ?traffic_count _ ?traffic_class  _ _ _ _)
      (road-metric ?traffic_class ?traffic_count ?albedo :> ?road_metric)
      (trees _ _ _ _ _ _ _ ?avg_height ?tree_lat ?tree_lng ?tree_alt ?geohash)
      (read-string ?avg_height :> ?height)
      ;; limit to trees which are higher than people
      (> ?height 2.0)
      (tree-distance ?tree_lat ?tree_lng ?road_lat ?road_lng :> ?distance)
      ;; limit to trees within a one-block radius (not meters)
      (<= ?distance 25.0)
      (/ ?height ?distance :> ?tree_moment)
      (c/sum ?tree_moment :> ?sum_tree_moment)
      ;; magic number 200000.0 used to scale tree moment, based on median
      (/ ?sum_tree_moment 200000.0 :> ?tree_metric)
   ))


(defn date-num [date]
  "converts an RFC 3339 timestamp to a monotonically increasing number"
  (apply
   (fn [yr mon day hr min sec]
       (+ (* (+ (* (+ (* (+ (* (+ (* yr 366) mon) 31) day) 24) hr) 60) min) 60) sec))
   (map #(Integer/parseInt %) (re-seq #"\d+" date))
   ))


(defn get-gps [gps_logs trap]
  "subquery to aggregate and rank GPS tracks per user"
  (<- [?uuid ?geohash ?gps_count ?recent_visit]
      (gps_logs ?date ?uuid ?gps_lat ?gps_lng ?alt ?speed ?heading ?elapsed ?distance)
      (read-string ?gps_lat :> ?lat)
      (read-string ?gps_lng :> ?lng)
      (geohash ?lat ?lng :> ?geohash)
      (c/count :> ?gps_count)
      (date-num ?date :> ?visit)
      (c/max ?visit :> ?recent_visit)
 ))


(defn get-reco [tracks shades]
  "subquery to recommend road segments based on GPS tracks"
  (<- [?uuid ?road ?geohash ?lat ?lng ?alt ?gps_count ?recent_visit ?road_metric ?tree_metric]
      (tracks ?uuid ?geohash ?gps_count ?recent_visit)
      (shades ?road ?geohash ?lat ?lng ?alt ?road_metric ?tree_metric)
   ))


(defn -main
  [in meta_tree meta_road logs trap park tree road shade gps reco & args]

  (let [gis (hfs-delimited in)
        tree_meta (hfs-delimited meta_tree :skip-header? true)
        road_meta (hfs-delimited meta_road :skip-header? true)
        gps_logs (hfs-delimited logs :delimiter "," :skip-header? true)
        src (etl-gis gis (s/join "/" [trap "gis"]))
        ]

    (?- (hfs-delimited tree)
        (get-trees src (s/join "/" [trap "tree"]) tree_meta)
     )

    (?- (hfs-delimited road)
        (get-roads src (s/join "/" [trap "road"]) road_meta)
     )

    (?- (hfs-delimited park)
        (get-parks src (s/join "/" [trap "park"]))
     )

    (?- (hfs-delimited shade)
        (let [trees (hfs-delimited tree)
              roads (hfs-delimited road)
              ]
          (get-shade trees roads)
         ))

    (?- (hfs-delimited gps)
        (get-gps gps_logs (s/join "/" [trap "logs"]))
     )

    (?- (hfs-delimited reco)
        (let [tracks (hfs-delimited gps)
              shades (hfs-delimited shade)
              ]
          (get-reco tracks shades)
         ))
   ))
