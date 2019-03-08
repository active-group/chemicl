(ns chemicl.offers
  (:require
   [chemicl.monad :as cm :refer [defmonadic whenm]]
   [chemicl.reaction-data :as rx-data]
   [chemicl.concurrency :as conc]
   [chemicl.post-commit :as pc]
   [chemicl.kcas :as kcas]
   [chemicl.backoff :as backoff]
   [chemicl.maybe :as maybe]
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

(defn active? [o]
  (or (waiting? o)
      (empty? o)))

;; accessors

(defn offer-answer [o]
  (assert (completed? o))
  (second o))

(defn offer-waiter [o]
  (assert (waiting? o))
  (second o))

;; ... beginning with :empty

(defmonadic new-offer []
  [oref (kcas/new-ref [:empty])]
  (m/return oref))

;; state transitions

;; Returns a Maybe Answer
(defmonadic rescind [oref]
  (backoff/with-exp-backoff
    [o (kcas/read oref)]
    (cond
      (completed? o)
      (m/return (backoff/done (maybe/just (offer-answer o))))

      (rescinded? o)
      (m/return (backoff/done (maybe/nothing)))

      (waiting? o)
      (m/monadic
       [succ (kcas/cas oref o [:rescinded])]
       (if succ
         ;; unpark
         (m/monadic
          (conc/unpark (offer-waiter o) :continue-after-rescinded-offer)
          (m/return (backoff/done (maybe/nothing))))
         ;; else retry
         (m/return (backoff/retry-backoff))))

      (empty? o)
      (m/monadic
       [succ (kcas/cas oref o [:rescinded])]
       (if succ
         (m/return (backoff/done (maybe/nothing)))
         (m/return (backoff/retry-backoff))))
      )))

(defmonadic wait [oref]
  [o (kcas/read oref)]
  [me (conc/get-current-task)]
  [succ
   (cond
     (empty? o)
     (kcas/cas oref o [:waiting me])

     (completed? o)
     (m/return false)

     (rescinded? o)
     (m/return false)

     (waiting? o)
     (assert false "Cannot wait twice on a ref"))]

  (if succ
    (conc/park)
    ;; else continue
    (m/return (maybe/nothing))))

(defmonadic complete [oref v] ;; v final result, returns a reaction
  [o (kcas/read oref)]
  (m/return
   (cond
     (waiting? o)
     (-> (rx-data/empty-rx)
         (rx-data/add-cas [oref o [:completed v]])
         (rx-data/add-action
          (conc/unpark (offer-waiter o) nil)))

     (empty? o) ;; this should not happen (?)
     (-> (rx-data/empty-rx)
         (rx-data/add-cas [oref o [:completed v]]))
     

     :else
     (rx-data/failing-rx))))
