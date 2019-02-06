(ns chemicl.refs
  (:require
   [chemicl.michael-scott-queue :as msq]
   [chemicl.offers :as o]
   [chemicl.monad :as cm :refer [defmonadic whenm]]
   [chemicl.kcas :as kcas]
   [active.clojure.record :as acr]
   [active.clojure.monad :as m]))


(acr/define-record-type Ref
  (make-ref data-ref offers)
  ref?
  [data-ref ref-data-ref
   offers ref-offers])

(defmonadic new-ref [init]
  [r (kcas/new-ref init)]
  [q (msq/create)]
  (m/return
   (make-ref r q)))

(defn read [ref]
  (kcas/read (ref-data-ref ref)))

(defn cas [ref ov nv]
  (kcas/cas (ref-data-ref ref) ov nv))

(defn reset [ref nv]
  (kcas/reset (ref-data-ref ref) nv))

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
