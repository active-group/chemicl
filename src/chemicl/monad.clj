(ns chemicl.monad
  (:require
   [active.clojure.monad :as m]))

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
