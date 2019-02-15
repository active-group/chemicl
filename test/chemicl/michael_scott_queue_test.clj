(ns chemicl.michael-scott-queue-test
  (:require [chemicl.michael-scott-queue :as msq]
            [active.clojure.monad :as m]
            [chemicl.monad :as cm :refer [defmonadic whenm]]
            [chemicl.concurrency :as conc]
            [chemicl.concurrency-test-runner :as test-runner]
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

(deftest push-push-tt
  (test-runner/run
    (m/monadic
     [q (msq/create)]

     ;; one pusher
     (conc/fork
      (m/monadic
       (test-runner/mark)
       (msq/push q 42)
       (test-runner/unmark)))

     ;; two pusher
     (m/monadic
      (test-runner/mark)
      (msq/push q 23)
      (test-runner/unmark))

     ;; pop 3
     [r1 (msq/try-pop q)]
     [r2 (msq/try-pop q)]
     [r3 (msq/try-pop q)]

     ;; check
     (test-runner/is (or (and (= r1 42) (= r2 23))
                         (and (= r1 23) (= r2 42))))
     (test-runner/is= r3 nil)
     )))

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

(deftest cursor-t
  (let [res-1 (atom nil)
        res-2 (atom nil)
        res-3 (atom nil)
        m (m/monadic
           ;; populate
           [q (msq/create)]
           (msq/push q 42)
           (msq/push q 23)

           ;; cursor
           [c-1 (msq/cursor q)]
           (let [_ (reset! res-1 (msq/cursor-value c-1))])
           [c-2 (msq/cursor-next c-1)]
           (let [_ (reset! res-2 (msq/cursor-value c-2))])
           [c-3 (msq/cursor-next c-2)]
           (let [_ (reset! res-3 c-3)])
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
