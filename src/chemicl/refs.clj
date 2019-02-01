(ns chemicl.refs
  (:require
   [chemicl.michael-scott-queue :as msq]
   [chemicl.offers :as o]
   [chemicl.monad :as cm :refer [defmonadic whenm]]
   [chemicl.concurrency :as conc]
   [active.clojure.record :as acr]
   [active.clojure.monad :as m]))


(acr/define-record-type Ref
  (make-ref data-ref offers)
  ref?
  [data-ref ref-data-ref
   offers ref-offers])

(defmonadic new-ref [init]
  [r (conc/new-ref init)]
  [q (msq/create)]
  (m/return
   (make-ref r q)))

(defn read [ref]
  (conc/read (ref-data-ref ref)))

(defn cas [ref ov nv]
  (conc/cas (ref-data-ref ref) ov nv))

(defn reset [ref nv]
  (conc/reset (ref-data-ref ref) nv))

(defn add-offer [ref oref]
  (whenm oref
    (msq/push
     (ref-offers ref) oref)))

(defmonadic rescind-offers [ref]
  [oref (msq/try-pop
         (ref-offers ref))]

  (whenm oref
    (o/rescind oref)
    (rescind-offers ref)))
