(ns copa.core
  (:use [cascalog.api]
        [cascalog.more-taps :only (hfs-delimited)]
        [date-clj])
  (:require [clojure.string :as s]
            [cascalog [ops :as c]]
            [clojure-csv.core :as csv]
            [geohash.core :as geo])
  (:gen-class))

(def parse-csv
  "parse complex CSV format in the unclean GIS export"
  (comp first csv/parse-csv))

(defn load-gis
  "Parse GIS csv data"
  [in trap]
  (<- [?blurb ?misc ?geo ?kind]
      ((hfs-textline in) ?line)
      (parse-csv ?line :> ?blurb ?misc ?geo ?kind) ;; hfs-delimited is not forgiving
      (:trap (hfs-textline trap))))

(defn avg [& more]
  "calculates the average of given numbers"
  (div (apply + more) (count more)))

(def geo-precision "a common resolution" 6)

(defn re-seq-chunks [pattern s]
  (rest (first (re-seq pattern s))))

(def parse-tree
  "parses the special fields in the tree format"
  (partial re-seq-chunks
           #"^\s+Private\:\s+(\S+)\s+Tree ID\:\s+(\d+)\s+.*Situs Number\:\s+(\d+)\s+Tree Site\:\s+(\d+)\s+Species\:\s+(\S.*\S)\s+Source.*"))

(def geo-tree
  "parses geolocation for tree format"
  (partial re-seq-chunks #"^(\S+),(\S+),(\S+)\s*$"))

(def trees-fields ["?blurb" "?tree_id" "?situs" "?tree_site" "?species" "?wikipedia"
                   "?calflora" "?avg_height" "?tree_lat" "?tree_lng" "?tree_alt" "?geohash"])

(defn get-trees [src tree-meta trap]
  "subquery to parse/filter the tree data"
  (<- trees-fields
      (src ?blurb ?misc ?geo ?kind)
      (re-matches #"^\s+Private.*Tree ID.*" ?misc)
      (parse-tree ?misc :> ?priv ?tree_id ?situs ?tree_site ?raw_species)
      ((c/comp s/trim s/lower-case) ?raw_species :> ?species)
      (tree-meta ?species ?wikipedia ?calflora ?min_height ?max_height)
      (avg ?min_height ?max_height :> ?avg_height)
      (geo-tree ?geo :> ?tree_lat ?tree_lng ?tree_alt)
      ((c/each read-string) ?tree_lat ?tree_lng :> ?lat ?lng)
      (geo/encode ?lat ?lng geo-precision :> ?geohash)
      (:trap (hfs-textline trap))))

(def parse-road
  "parses the special fields in the road format"
  (partial re-seq-chunks
           #"^\s+Sequence.*Traffic Count\:\s+(\d+)\s+Traffic Index\:\s+(\w.*\w)\s+Traffic Class\:\s+(\w.*\w)\s+Traffic Date.*\s+Paving Length\:\s+(\d+)\s+Paving Width\:\s+(\d+)\s+Paving Area\:\s+(\d+)\s+Surface Type\:\s+(\w.*\w)\s+Surface Thickness.*\s+Overlay Year\:\s+(\d+)\s.*Bike Lane\:\s+(\w+)\s+Bus Route\:\s+(\w+)\s+Truck Route\:\s+(\w+)\s+Remediation.*$"))

(defn estimate-albedo [overlay_year albedo_new albedo_worn]
  "calculates an estimator for road albedo, based on road surface age"
  (if (>= overlay_year (- (year (today)) 10.0)) ;; TODO use clj-time
    albedo_new
    albedo_worn))

(defmapcatop bigram [s]
  "generator for bi-grams, from a space-separated list"
  (partition 2 1 (s/split s #"\s")))

(defn point->coord
  "Takes a point string returns [lng lat alt]"
  [p]
  (map read-string (s/split p #",")))

(defn midpoint [pt0 pt1]
  "calculates the midpoint of two geolocation coordinates"
  (let [[lng0 lat0 alt0] (point->coord pt0)
        [lng1 lat1 alt1] (point->coord pt1)]
    [(avg lat0 lat1) (avg lng0 lng1) (avg alt0 alt1)]))

(def roads-fields ["?road_name" "?bike_lane" "?bus_route" "?truck_route" "?albedo"
                   "?road_lat" "?road_lng" "?road_alt" "?geohash"
                   "?traffic_count" "?traffic_index" "?traffic_class"
                   "?paving_length" "?paving_width" "?paving_area" "?surface_type"])

(defn get-roads [src road-meta trap]
  "subquery to parse/filter the road data"
  (<- roads-fields
      (src ?road_name ?misc ?geo ?kind)
      (re-matches #"^\s+Sequence.*Traffic Count.*" ?misc)
      (parse-road ?misc :>
                  ?traffic_count ?traffic_index ?traffic_class
                  ?paving_length ?paving_width ?paving_area ?surface_type
                  ?overlay_year_str ?bike_lane ?bus_route ?truck_route)
      (road-meta ?surface_type ?albedo_new ?albedo_worn)
      ((c/each read-string) ?overlay_year_str :> ?overlay_year)
      (estimate-albedo ?overlay_year ?albedo_new ?albedo_worn :> ?albedo)
      (bigram ?geo :> ?pt0 ?pt1)
      (midpoint ?pt0 ?pt1 :> ?lat ?lng ?alt)
      ;; why filter for min? because there are geo duplicates..
      ((c/each c/min) ?lat ?lng ?alt :> ?road_lat ?road_lng ?road_alt)
      (geo/encode ?road_lat ?road_lng geo-precision :> ?geohash)
      (:trap (hfs-textline trap))))

(defn get-parks [src trap]
  "subquery to parse/filter the park data"
  (<- [?blurb ?misc ?geo ?kind]
      (src ?blurb ?misc ?geo ?kind)
      (re-matches #"\s+Community Type\:\s+Park.*" ?misc)))

(defn tree-distance [tree_lat tree_lng road_lat road_lng]
  "calculates distance from a tree to the midpoint of a road segment; TODO IMPROVE GEO MODEL"
  (let [y (- tree_lat road_lat)
        x (- tree_lng road_lng)]
    (Math/sqrt (+ (Math/pow y 2.0) (Math/pow x 2.0)))))

(defn road-metric [traffic_class traffic_count albedo]
  "calculates a metric for comparing road segments, approximating a decision tree; TODO USE PMML"
  [[(condp = traffic_class
      "local residential"       1.0
      "local business district" 0.5
      0.0)
    (-> traffic_count (/ 200.0) (Math/log) (/ 5.0)) ;; scale traffic_count based on distribution mean
    (- 1.0 albedo)]])
    ;; in practice, we'd probably train a predictive model using decision trees,
    ;; regression, etc., plus incorporate customer feedback, QA of the data, etc.

(defn get-shade [trees roads]
  "subquery to join the tree and road estimates, to maximize for shade"
  (<- [?road_name ?geohash ?road_lat ?road_lng ?road_alt ?road_metric ?tree_metric]
      ((select-fields roads ["?road_name" "?albedo" "?road_lat" "?road_lng" "?road_alt" "?geohash" "?traffic_count" "?traffic_class"])
       ?road_name ?albedo ?road_lat ?road_lng ?road_alt ?geohash ?traffic_count ?traffic_class)
      (road-metric ?traffic_class ?traffic_count ?albedo :> ?road_metric)
      ((select-fields trees ["?avg_height" "?tree_lat" "?tree_lng" "?tree_alt" "?geohash"])
       ?height ?tree_lat ?tree_lng ?tree_alt ?geohash)
      (> ?height 2.0) ;; limit to trees which are higher than people
      (tree-distance ?tree_lat ?tree_lng ?road_lat ?road_lng :> ?distance)
      (<= ?distance 25.0) ;; limit to trees within a one-block radius (not meters)
      (/ ?height ?distance :> ?tree_moment)
      (c/sum ?tree_moment :> ?sum_tree_moment)
      ;; magic number 200000.0 used to scale tree moment, based on median
      (/ ?sum_tree_moment 200000.0 :> ?tree_metric)))

(defn date-num [date] ;; TODO use clj-time
  "converts an RFC 3339 timestamp to a monotonically increasing number"
  (apply
    (fn [yr mon day hr min sec]
      (-> yr (* 366)
          (+ mon) (* 31)
          (+ day) (* 24)
          (+ hr)  (* 60)
          (+ min) (* 60)
          (+ sec)))
    (map #(Integer/parseInt %) (re-seq #"\d+" date))))

(def gps-fields ["?uuid" "?geohash" "?gps_count" "?recent_visit"])

(defn get-gps [gps_logs trap]
  "subquery to aggregate and rank GPS tracks per user"
  (<- gps-fields
      (gps_logs ?date ?uuid ?lat ?lng ?alt ?speed ?heading ?elapsed ?distance)
      (geo/encode ?lat ?lng geo-precision :> ?geohash)
      (c/count :> ?gps_count)
      (date-num ?date :> ?visit)
      (c/max ?visit :> ?recent_visit)))

(defn get-reco [tracks shades]
  "subquery to recommend road segments based on GPS tracks"
  (<- [?uuid ?road ?geohash ?lat ?lng ?alt ?gps_count ?recent_visit ?road_metric ?tree_metric]
      (tracks :>> gps-fields)
      (shades ?road ?geohash ?lat ?lng ?alt ?road_metric ?tree_metric)))

(defn -main
  [in meta_tree meta_road logs trap park tree road shade gps reco & args]
  (let [gis-stage "out/gis"] ;; should use workflow but local hadoop can't run concurrent tasks
    (?- "etl gis data"
        (hfs-seqfile gis-stage)
        (load-gis in (str trap "/" "gis")))
    (?- "parse tree data"
        (hfs-delimited tree)
        (get-trees (hfs-seqfile gis-stage)
                   (hfs-delimited meta_tree :skip-header? true
                                  :classes [String String String Integer Integer])
                   (str trap "/" "tree")))
    (?- "parse road data"
        (hfs-delimited road)
        (get-roads (hfs-seqfile gis-stage)
                   (hfs-delimited meta_road :skip-header? true
                                  :classes [String Float Float])
                   (str trap "/" "road")))
    (?- "parse parks data"
        (hfs-delimited park)
        (get-parks (hfs-seqfile gis-stage)
                   (str trap "/" "park")))
    (?- "calculate shades"
        (hfs-delimited shade)
        (get-shade (name-vars
                     (hfs-delimited tree  ;; save as hfq-seqfile to keep field types to avoid :classes
                                    :classes [String Integer Integer Integer String String String
                                              Double Double Double Double String])
                     trees-fields)
                   (name-vars
                     (hfs-delimited road
                                    :classes [String String String String Double Double Double Double
                                              String Integer String String Integer Integer Integer String])
                     roads-fields)))
    (?- "parse gps data"
        (hfs-delimited gps)
        (get-gps (hfs-delimited logs :delimiter "," :skip-header? true
                                :classes [String String Double Double Double Double Double Double Double])
                 (str trap "/" "logs")))
    (?- "recommend road segments"
        (hfs-delimited reco)
        (get-reco (hfs-delimited gps) (hfs-delimited shade)))))
