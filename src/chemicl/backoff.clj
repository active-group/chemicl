(ns chemicl.backoff
  (:require [chemicl.concurrency :as conc]
            [active.clojure.monad :as m]
            [chemicl.monad :as cm :refer [defmonadic whenm]]))

(defn- pow [base exponent]
  (loop [i exponent
         res 1]
    (if (zero? i)
      res
      (recur (dec i) (* res base)))))

(defn- maximum-for [counter]
  ;; Note: return value are used as milliseconds:
  (pow 2 (min counter 14)))

(defn done [x]
  [:done x])

(let [v [:retry-backoff]]
  (defn retry-backoff []
    v))

(let [v [:retry-reset]]
  (defn retry-reset []
    v))

(defn- nano-sleep [i]
  ;; these are all not much different. TODO: add/try a conc/yield maybe?
  (java.util.concurrent.locks.LockSupport/parkNanos i)
  #_(Thread/yield)
  #_(dotimes [n 10000] nil))

(defn timeout-with-counter [counter]
  (m/return
   (nano-sleep
    (rand-int
     (maximum-for counter)))))

(defmonadic with-exponential-backoff-counter [c m]
  [result m]
  (case (first result)
    :done
    (m/return (second result))

    :retry-backoff
    (m/monadic
     (timeout-with-counter c)
     (with-exponential-backoff-counter (inc c) m))

    :retry-reset
    (m/monadic
     (with-exponential-backoff-counter 0 m))))

(defn with-exponential-backoff [m]
  (with-exponential-backoff-counter 0 m))

(defmacro with-exp-backoff [& ms]
  `(with-exponential-backoff
     (m/monadic ~@ms)))

