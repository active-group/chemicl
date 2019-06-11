(ns chemicl.jqueue
  (:require [active.clojure.record :as acr]
            [active.clojure.monad :as m]
            [chemicl.monad :as cm :refer [defmonadic whenm]]
            [chemicl.concurrency :as conc])
  (:import (java.util.concurrent ConcurrentLinkedQueue)))

(acr/define-record-type JQueue
  (make-jqueue size q)
  jqueue?
  [size q-size ;; approx.
   q q-q])

(defmonadic create []
  (cm/effect! (make-jqueue (atom 0) (new ConcurrentLinkedQueue))))

(defn push [q v]
  (assert false))

;; Note: although ConcurrentLinkedQueue::size is not O(1), it seems to be not worth it to keep track of the size ourselves.
(def ^:private track-size? false)
(def ^:private clean-every 128)

(defmonadic clean [q active?]
  (let [^ConcurrentLinkedQueue qq (q-q q)])
  [deletions (m/sequ (map (fn [x]
                            (m/monadic
                             [p (active? x)]
                             (cm/effect! (when-not p
                                           (.remove qq x)))
                             (m/return (if p 0 1))))
                          (iterator-seq (.iterator qq))))]
  (cm/effect! (when track-size? (swap! (q-size q) - (reduce + 0 deletions)))))

(defmonadic push&clean [q v active?]
  (let [^ConcurrentLinkedQueue qq (q-q q)])
  (cm/effect! (do (.offer qq v)
                  (when track-size? (swap! (q-size q) inc))))
  ;; clean up from time to time.
  (if (= (dec clean-every) (mod (if track-size?
                                  @(q-size q)
                                  (.size qq))
                                clean-every))
    (clean q active?)
    (m/return nil)))

(defmonadic cursor [q]
  [sq (cm/effect! (iterator-seq (.iterator (q-q q))))]
  (m/return sq))

(defn cursor-value [qq]
  (first qq))

(defmonadic cursor-next [qq]
  (m/return (seq (rest qq))))
