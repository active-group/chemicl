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
  (* 10 (pow 2 (min counter 14))))

(defn done [x]
  [:done x])

(defn retry-backoff []
  [:retry-backoff])

(defn retry-reset []
  [:retry-reset])

(defmonadic with-exponential-backoff-counter [c m]
  [result m]
  (case (first result)
    :done
    (m/return (second result))

    :retry-backoff
    (m/monadic
     (conc/timeout (rand-int (maximum-for c)))
     (with-exponential-backoff-counter (inc c) m))

    :retry-reset
    (m/monadic
     (with-exponential-backoff-counter 0 m))))

(defn with-exponential-backoff [m]
  (with-exponential-backoff-counter 0 m))

(defmacro with-exp-backoff [& ms]
  `(with-exponential-backoff
     (m/monadic ~@ms)))

(defn timeout-with-counter [counter]
  (conc/timeout (rand-int (maximum-for counter))))
