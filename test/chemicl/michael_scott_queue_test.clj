(ns chemicl.michael-scott-queue-test
  (:require [chemicl.michael-scott-queue :as msq]
            [active.clojure.monad :as m]
            [chemicl.monad :as cm :refer [defmonadic whenm]]
            [chemicl.concurrency :as conc]
            [clojure.test :as t :refer [deftest testing is]]))

(deftest push-then-pop-t
  (let [res-1 (atom nil)
        res-2 (atom nil)
        res-3 (atom nil)
        m (m/monadic
           [q (msq/create)]
           (msq/push q 42)
           (msq/push q 23)
           [r1 (msq/try-pop q)]
           [r2 (msq/try-pop q)]
           [r3 (msq/try-pop q)]
           (let [_ (reset! res-1 r1)])
           (let [_ (reset! res-2 r2)])
           (let [_ (reset! res-3 r3)])
           (conc/print "done")
           )]

    ;; run
    (conc/run-many-to-many m)

    ;; wait
    (Thread/sleep 20)

    ;; check
    (is (= 42 @res-1))
    (is (= 23 @res-2))
    (is (= nil @res-3))
    ))

(deftest clean-until-t
  (let [res-1 (atom nil)
        res-2 (atom nil)
        res-3 (atom nil)
        m (m/monadic
           [q (msq/create)]
           (msq/push q 3) ;; odd
           (msq/push q 5) ;; odd
           (msq/push q 4) ;; even
           (msq/push q 9) ;; odd
           (msq/clean-until q even?)

           [r1 (msq/try-pop q)]
           [r2 (msq/try-pop q)]
           [r3 (msq/try-pop q)]
           (let [_ (reset! res-1 r1)])
           (let [_ (reset! res-2 r2)])
           (let [_ (reset! res-3 r3)])
           (conc/print "done")
           )]

    ;; run
    (conc/run-many-to-many m)

    ;; wait
    (Thread/sleep 20)

    ;; check
    (is (= 4 @res-1))
    (is (= 9 @res-2))
    (is (= nil @res-3))
    ))
