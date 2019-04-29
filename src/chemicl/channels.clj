(ns chemicl.channels
  (:require
   [chemicl.monad :as cm :refer [defmonadic whenm]]
   [chemicl.concurrency :as conc]
   [chemicl.message-queue :as mq]
   [active.clojure.record :as acr]
   [active.clojure.lens :as lens]
   [active.clojure.monad :as m]))

(acr/define-record-type Endpoint
  (make-endpoint incoming outgoing)
  endpoint?
  [incoming endpoint-incoming
   outgoing endpoint-outgoing])

(defmonadic new-channel []
  [l1 (mq/new-message-queue)
   l2 (mq/new-message-queue)]
  (m/return
   [(make-endpoint l2 l1) 
    (make-endpoint l1 l2)])) 

(defn channel? [v]
  (and (vector? v)
       (= 2 (count v))
       (instance? Endpoint (first v))
       (instance? Endpoint (second v))))
