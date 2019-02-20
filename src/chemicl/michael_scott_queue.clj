(ns chemicl.michael-scott-queue
  (:require [active.clojure.record :as acr]
            [active.clojure.monad :as m]
            [chemicl.monad :as cm :refer [defmonadic whenm]]
            [chemicl.concurrency :as conc]
            [chemicl.backoff :as backoff]))

(acr/define-record-type MSQueue
  (make-ms-queue hd tl)
  ms-queue?
  [hd ms-queue-head
   tl ms-queue-tail])

(acr/define-record-type Node
  (make-node value next)
  node?
  [value node-value
   next node-next])

(def sentinel-value ::sentinel)

;; create

(defmonadic create []
  [next (conc/new-ref nil)]
  (let [sentinel (make-node sentinel-value next)])
  [hd (conc/new-ref sentinel)]
  [tl (conc/new-ref sentinel)]
  (m/return
   (make-ms-queue hd tl)))

;; pop

(defmonadic try-pop [q]
  (backoff/with-exp-backoff
   [old-head (conc/read (ms-queue-head q))]
   [next (conc/read (node-next old-head))]
   (if next
     ;; pop: hd points to next, return val
     (m/monadic
      [succ (conc/cas (ms-queue-head q) old-head next)]
      (if succ
        (m/return (backoff/done (node-value next)))
        (m/return (backoff/retry-backoff))))
     ;; queue is empty
     (m/return (backoff/done nil)))))

;; push

(defmonadic push [q x]
  (backoff/with-exp-backoff
    ;; create new tail node
    [nilref (conc/new-ref nil)]
    (let [new-tail-node (make-node x nilref)])
    
    ;; get tail node
    [tail-node (conc/read (ms-queue-tail q))]

    ;; get tail successor
    [successor-node (conc/read (node-next tail-node))]

    (if successor-node
      ;; true tail lags behind
      (m/monadic
       ;; catch up
       (conc/cas (ms-queue-tail q)
                 tail-node
                 successor-node)
       ;; and retry
       (m/return (backoff/retry-reset)))
      ;; found true tail
      (m/monadic
       [succ (conc/cas (node-next tail-node)
                       successor-node
                       new-tail-node)]
       (if succ
         (m/monadic
          ;; try to cas the tail pointer
          (conc/cas (ms-queue-tail q)
                    tail-node
                    new-tail-node)
          (m/return (backoff/done nil)))
         ;; else retry
         (m/return (backoff/retry-backoff))
         )))))

;; cursor

(defmonadic cursor [q]
  [s (conc/read (ms-queue-head q))]
  (conc/read (node-next s)))

(defn cursor-value [c]
  (when c
    (node-value c)))

(defn cursor-next [c]
  (whenm c
    (conc/read (node-next c))))

;; clean

(defmonadic clean-until [q pred]
  (backoff/with-exp-backoff
    [sentinel-node (conc/read (ms-queue-head q))]
    [head-node (conc/read (node-next sentinel-node))]
    (if head-node
      (m/monadic
       (let [v (node-value head-node)])
       [p? (pred v)]
       (if-not p?
         ;; cas away and continue
         (m/monadic
          [succ (conc/cas (ms-queue-head q) sentinel-node head-node)]
          (if succ
            (m/return (backoff/retry-reset))
            (m/return (backoff/retry-backoff))))
         ;; else done
         (m/return (backoff/done nil))))
      ;; else no nodes
      (m/return (backoff/done nil)))))
