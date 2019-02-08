(ns chemicl.offers-test
  (:require [chemicl.offers :as o]
            [chemicl.concurrency :as conc]
            [chemicl.concurrency-test-runner :as test-runner]
            [chemicl.kcas :as kcas]
            [clojure.test :as t :refer [deftest testing is]]
            [active.clojure.monad :as m]
            [chemicl.monad :as cm :refer [defmonadic whenm]]))

(deftest rescind-rescind-tt
  (test-runner/run
    (m/monadic
     ;; init
     [res-1 (conc/new-ref :nothing)]
     [oref (o/new-offer)]

     [parent (conc/get-current-task)]

     (conc/fork
      (m/monadic
       (test-runner/mark)
       [res (o/rescind oref)]
       (test-runner/unmark)

       ;; reset (1)
       (conc/reset res-1 res)
       (conc/unpark parent nil)))


     (test-runner/mark)
     [r2 (o/rescind oref)]
     (test-runner/unmark)

     ;; wait for reset (1)
     (conc/park)

     ;; check
     [r1 (conc/read res-1)]
     (test-runner/is= r1 nil "rescind result must be nil")
     (test-runner/is= r2 nil "rescind result must be nil")

     ;; FIXME: this is based on assumption about offer internals
     [o (conc/read oref)]
     (test-runner/is (o/rescinded? o) "offer must be rescinded")
     )))
