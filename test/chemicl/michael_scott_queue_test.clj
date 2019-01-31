(ns chemicl.michael-scott-queue-test
  (:require [chemicl.michael-scott-queue :as msq]
            [active.clojure.monad :as m]
            [chemicl.monad :as cm :refer [defmonadic whenm]]
            [chemicl.concurrency :as conc]
            [clojure.test :as t :refer [deftest testing is]]))

(deftest push-then-pop-t
  (let [res-1 (atom nil)
        res-2 (atom nil)
        m (m/monadic
           [q (msq/create)]
           (msq/push q 42)
           (msq/push q 23)
           [r1 (msq/try-pop q)]
           [r2 (msq/try-pop q)]
           (let [_ (reset! res-1 r1)])
           (let [_ (reset! res-2 r2)])
           (conc/print "done")
           )]

    ;; run
    (conc/run-many-to-many m)

    ;; wait
    (Thread/sleep 20)

    ;; check
    (is (= 42 @res-1))
    (is (= 23 @res-2))
    ))
