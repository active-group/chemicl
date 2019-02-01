(ns chemicl.michael-scott-queue
  (:require [active.clojure.record :as acr]
            [active.clojure.monad :as m]
            [chemicl.monad :as cm :refer [defmonadic whenm]]
            [chemicl.concurrency :as conc]))

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
  [old-head (conc/read (ms-queue-head q))]
  [next (conc/read (node-next old-head))]
  (if next
    ;; pop: hd points to next, return val
    (m/monadic
     [succ
      (conc/cas (ms-queue-head q) old-head next)]
     (if succ
       (m/return (node-value next))

       ;; FIXME: backoff
       (try-pop q)))
    ;; queue is empty
    (m/return nil)))

;; push

(defmonadic push [q x]
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
     (push q x))
    ;; found true tail
    (m/monadic
     [succ (conc/cas (node-next tail-node)
                     successor-node
                     new-tail-node)]
     (if succ
       ;; try to cas the tail pointer
       (conc/cas (ms-queue-tail q)
                 tail-node
                 new-tail-node)
       ;; else retry
       ;; FIXME: backoff
       (push q x)
       ))))

;; cursor

(defmonadic cursor [q]
  [s (conc/read (ms-queue-head q))]
  (conc/read (node-next s)))

(defn cursor-value [c]
  (node-value c))

(defn cursor-next [c]
  (conc/read (node-next c)))

;; clean

(defmonadic clean-until [q pred]
  [sentinel-node (conc/read (ms-queue-head q))]
  [head-node (conc/read (node-next sentinel-node))]
  (whenm head-node
    (let [v (node-value head-node)])
    (if-not (pred v)
      ;; cas away and continue
      (m/monadic
       [succ (conc/cas (ms-queue-head q) sentinel-node head-node)]
       (if succ
         ;; FIXME: backoff reset
         (clean-until q pred)
         ;; FIXME: backoff once
         (clean-until q pred)))
      ;; else done
      (m/return nil)
      )))