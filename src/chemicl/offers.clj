(ns chemicl.offers
  (:require
   [chemicl.monad :as cm :refer [defmonadic whenm]]
   [chemicl.concurrency :as conc]
   [active.clojure.record :as acr]
   [active.clojure.lens :as lens]
   [active.clojure.monad :as m]))


;; --- Offers ---------

(defmonadic new-offer []
  [o (conc/new-ref {:status :pending
                    :payload nil})]
  (m/return o))

(defmonadic get-answer [oref]
  [o (conc/read oref)]
  (m/return (:payload o)))

(defmonadic rescind-offer [oref]
  [o (conc/read oref)]
  (if (or (= :completed (:status o))
          (= :rescinded (:status o)))
    ;; failed to rescind
    false
    ;; try to rescind
    (m/monadic
     [succ (conc/cas oref o (assoc o :status :rescinded))]
     (if succ
       true
       (rescind-offer oref)))))

(defmonadic answer-offer [oref ans]
  (conc/swap
   oref
   (fn [o]
     (-> o
         (assoc :payload ans)
         (assoc :status :completed)))))
