(ns copa.core-test
  (:use [copa.core]
        [midje sweet cascalog]
        [clojure.test]))

(deftest parse-tree-test
  (is (= ["-1" "29" "203" "2" "Celtis australis"] (parse-tree "   Private:   -1    Tree ID:   29    Street_Name:   ADDISON AV    Situs Number:   203    Tree Site:   2    Species:   Celtis australis    Source:   davey tree    Protected:       Designated:       Heritage:       Appraised Value:       Hardscape:   None    Identifier:   40    Active Numeric:   1    Location Feature ID:   13872    Provisional:       Install Date:      "))))

(deftest geo-tree-test
  (is (= ["37.4409634615283" "-122.15648458861" "0.0"] (geo-tree "37.4409634615283,-122.15648458861,0.0 "))))

(deftest point->coord-test
  (is (= [-122.138274762396 37.4228142494056 0.0] (point->coord "-122.138274762396,37.4228142494056,0.0"))))

(deftest date-num-test
  (is (= 1972376670917 (date-num "2012-09-02T16:35:17Z"))))
