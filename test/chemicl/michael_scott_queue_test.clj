(ns chemicl.michael-scott-queue-test
  (:require [chemicl.michael-scott-queue :as msq]
            [chemicl.gearents :as rea]
            [active.clojure.monad :as m]
            [chemicl.monad :as cm :refer [defmonadic whenm]]
            [chemicl.refs :as refs]
            [chemicl.concurrency :as conc]
            [chemicl.channels :as channels]
            [chemicl.message-queue :as mq]
            [clojure.test :as t :refer [deftest testing is]]))

(deftest push-then-pop-t
  (let [res-1 (atom nil)
        res-2 (atom nil)
        m (m/monadic
           [q (msq/create)]
           (rea/react! (msq/push q) 42)
           (rea/react! (msq/push q) 23)
           [r1 (rea/react! (msq/pop q) nil)]
           [r2 (rea/react! (msq/pop q) nil)]
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
