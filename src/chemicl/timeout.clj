(ns chemicl.timeout
  (:require
   [chemicl.reagents :as rea]
   [chemicl.concurrency :as conc]))

(defn timeout [msec]
  (rea/->reagent
   (conc/timeout msec)))
