(ns chemicl.michael-scott-queue
  (:require [chemicl.gearents :as rea]
            [active.clojure.record :as acr]
            [active.clojure.monad :as m]
            [chemicl.monad :as cm :refer [defmonadic whenm]]
            [chemicl.concurrency :as conc]
            [chemicl.channels :as channels]
            [chemicl.refs :as refs]))

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
  [next (refs/new-ref nil)]
  (let [sentinel (make-node sentinel-value next)])
  [hd (refs/new-ref sentinel)]
  [tl (refs/new-ref sentinel)]
  (m/return
   (make-ms-queue hd tl)))

;; pop

(defn pop [q]
  (rea/upd (ms-queue-head q)
           (fn [[ov a]]
             (m/monadic
              (let [next-ptr (node-next ov)])
              [n (refs/read next-ptr)]
              (let [ndata (refs/ref-data n)])
              (if ndata
                ;; pop: hd points to next, return val
                (m/return [ndata (node-value ndata)])
                ;; queue is empty
                (m/return nil))))))

;; push

(defmonadic find-and-enqueue [n tail-ptr]
  ;; get tail node
  [tail-node-1 (refs/read tail-ptr)]

  ;; unpack
  (let [tail-node (refs/ref-data tail-node-1)])

  ;; get tail successor
  [successor-1 (-> tail-node
                   (node-next)
                   (refs/read))]

  ;; unpack
  (let [s (refs/ref-data successor-1)])

  (letfn [(fwd-tail [nv]
            (refs/cas tail-ptr tail-node-1 nv))]
    (if s
      ;; true tail not found yet
      (m/monadic
       (fwd-tail successor-1)         ;; advance tail-ptr
       (find-and-enqueue n tail-ptr)) ;; retry

      ;; found true tail -> enqueue
      (m/return
       (rea/>>> (rea/cas (node-next tail-node) s n)
                (rea/post-commit (fn [_]
                                   (fwd-tail
                                    (refs/make-ref n [])))))))))

(defn push [q]
  (rea/computed
   (fn [a]
     (m/monadic
      [r (refs/new-ref nil)]
      (let [node (make-node a r)])
      (find-and-enqueue node (ms-queue-tail q))))))
