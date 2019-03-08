(ns chemicl.monad
  (:require
   [active.clojure.monad :as m]))

(defmacro effect! [f]
  `(m/monadic
    ;; shield the thunk from eager evaluation
    (m/return nil)
    (let [r# ~f]
      (m/return r#))))

(defn monadic? [x]
  (or (m/free-return? x)
      (m/free-bind? x)))

(defn maybe-unwrap-monadic [m]
  (if (monadic? m)
    ;; run monadic program res
    m
    ;; else return value res
    (m/return m)))

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

(defmacro fm
  "like fn but monadic"
  {:style/indent :defn}
  [args & bodies]
  `(fn ~args
     (m/monadic
      ~@bodies)))

(defn mask [coll m]
  (let [mask-map (zipmap coll m)]
    (filter mask-map coll)))

(defmonadic filterm [predm coll]
  [msk (m/sequ (map predm coll))]
  (m/return 
   (mask coll msk)))
