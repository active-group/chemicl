(ns chemicl.offers
  (:require
   [chemicl.monad :as cm :refer [defmonadic whenm]]
   [chemicl.concurrency :as conc]
   [active.clojure.record :as acr]
   [active.clojure.lens :as lens]
   [active.clojure.monad :as m]))


;; --- Offers ---------
;; An offer is a simple non-deterministic state machine

(defn empty? [[status]]
  (= status
     :empty))

(defn completed? [[status]]
  (= status
     :completed))

(defn rescinded? [[status]]
  (= status
     :rescinded))

(defn waiting? [[status]]
  (= status
     :waiting))

;; accessors

(defn offer-answer [o]
  (assert (completed? o))
  (second o))

(defn offer-waiter [o]
  (assert (waiting? o))
  (second o))

;; ... beginning with :empty

(defmonadic new-offer []
  [oref (conc/new-ref [:empty])]
  (m/return oref))

;; state transitions

(defmonadic rescind [oref]
  [o (conc/read oref)]
  (whenm (or (empty? o)
             (waiting? o))
    ;; cas to rescinded
    [succ (conc/cas oref o [:rescinded])]

    ;; unpark when we successfully rescinded the offer
    (whenm (and succ
                (waiting? o))
      (conc/unpark (offer-waiter o) :continue-after-rescinded-offer)))

  ;; here we expect the offer to either be recinded or completed
  [offer (conc/read oref)]
  (cond
    (completed? offer) ;; somebody slid in
    (m/return (offer-answer offer))

    (rescinded? offer) ;; we were successful
    (m/return false)

    :else
    (assert false "We expect offer to be rescinded or completed")))

(defmonadic wait [oref]
  [o (conc/read oref)]
  [me (conc/get-current-task)]
  (let [[status arg] o])
  [succ
   (cond
     (empty? o)
     (conc/cas oref o [:waiting me])

     (completed? o)
     false

     (rescinded? o)
     false

     (waiting? o)
     (assert false "Cannot wait twice on a ref"))]

  (if succ
    (conc/park)
    ;; else continue
    (m/return nil)))
