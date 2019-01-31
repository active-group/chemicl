(ns chemicl.refs
  (:require
   [chemicl.michael-scott-queue :as msq]
   [chemicl.offers :as o]
   [chemicl.monad :as cm :refer [defmonadic whenm]]
   [chemicl.concurrency :as conc]
   [active.clojure.record :as acr]
   [active.clojure.monad :as m]))


;; new

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













;; FIXME: replace offers with MS Queue

;; (acr/define-record-type Ref
;;   (make-ref data offers)
;;   ref?
;;   [data ref-data
;;    offers ref-offers])


;; (defn new-ref [init]
;;   (conc/new-ref (make-ref init [])))

;; (defmonadic read [refref]
;;   (conc/read refref))

;; (defmonadic cas [refref ov nv]
;;   (conc/cas refref ov nv))

;; (defmonadic reset [refref nv]
;;   (conc/reset refref nv))

;; ;; the new refs api

;; (defn add-offer [refref offer-ref]
;;   (whenm offer-ref
;;     (conc/swap
;;      refref
;;      (fn [ref]
;;        (make-ref
;;         (ref-data ref)
;;         (conj (ref-offers ref)
;;               offer-ref))))))

;; (defmonadic rescind-offers [refref]
;;   [ref (conc/read refref)]
;;   (m/sequ_ (map o/rescind (ref-offers ref)))
;;   ;; TODO: offers are now rescinded but the offers refs are still in refrefs
;;   ;; Maybe fork a garbage collecting process here
;;   )
