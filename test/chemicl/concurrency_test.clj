(ns chemicl.concurrency-test
  (:require
   [chemicl.concurrency :as conc]
   [active.clojure.record :as acr]
   [active.clojure.lens :as lens]
   [active.clojure.monad :as m]
   [chemicl.monad :as cm :refer [defmonadic whenm]]
   [clojure.test :as t :refer [deftest testing is]]))

(defn log [a x]
  (m/monadic
   (let [_ (swap! a conj x)])
   (m/return true)))

(deftest m2n-park-unpark-t
  (let [a (atom []) 

        m (m/monadic
           (log a 1)
           [child (conc/fork
                   (m/monadic
                    (log a 2)
                    (conc/park)
                    (log a 4)
                    ))]
           (conc/timeout 20)
           (log a 3)
           (conc/unpark child nil)
           (log a 5)
           )]
    ;; Run m
    (conc/run-many-to-many m)

    ;; wait
    (Thread/sleep 100)

    ;; check log
    (is (= @a [1 2 3 4 5]))))
