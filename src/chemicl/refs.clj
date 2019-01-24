(ns chemicl.refs
  (:require
   [chemicl.offers :as o]
   [chemicl.monad :as cm :refer [defmonadic whenm]]
   [chemicl.concurrency :as conc]
   [active.clojure.record :as acr]
   [active.clojure.lens :as lens]
   [active.clojure.monad :as m]))

(acr/define-record-type Ref
  (make-ref data offers)
  ref?
  [data ref-data
   offers ref-offers])

(defn new-ref [init]
  (conc/new-ref (make-ref nil [])))

(defmonadic read [refref]
  (conc/read refref))

(defmonadic cas [refref ov nv]
  (conc/cas refref ov nv))

;; the new refs api

(defn add-offer [refref offer-ref]
  (whenm offer-ref
    (conc/swap
     refref
     (fn [ref]
       (make-ref
        (ref-data ref)
        (conj (ref-offers ref)
              offer-ref))))))

(defmonadic offers-rescind-offers [offers]
  (m/sequ_ (map o/rescind offers)))

(defmonadic rescind-offers [refref]
  [ref (conc/read refref)]
  (offers-rescind-offers (ref-offers ref))
  ;; TODO: offers are now rescinded but the offers refs are still in refrefs
  ;; Maybe fork a garbage collecting process here
  )
