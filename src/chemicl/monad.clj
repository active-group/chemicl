(ns chemicl.monad
  (:require
   [active.clojure.monad :as m]
   [chemicl.concurrency :as conc]))

(defmacro defmonadic
  "like defn but monadic"
  {:style/indent :defn}
  [name argsvec & body]
  `(defn ~name ~argsvec
     (m/monadic ~@body)))

(defmacro whenm
  "like when but monadic"
  {:style/indent :defn}
  [pred & body]
  `(if ~pred
     (m/monadic
      ~@body)
     (m/return nil)))

(defn mask [coll m]
  (let [mask-map (zipmap coll m)]
    (filter mask-map coll)))

(defmonadic filterm [predm coll]
  [msk (m/sequ (map predm coll))]
  (m/return 
   (mask coll msk)))
