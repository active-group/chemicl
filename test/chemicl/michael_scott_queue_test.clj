(ns chemicl.michael-scott-queue-test
  (:require [chemicl.michael-scott-queue :as msq]
            [active.clojure.monad :as m]
            [chemicl.monad :as cm :refer [defmonadic whenm]]
            [chemicl.concurrency :as conc]
            [chemicl.concurrency-test-runner :as test-runner]
            [clojure.test :as t :refer [deftest testing is]]))



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

(defmacro xor [l r]
  `(and (or ~l ~r)
        (not (and ~l ~r))))

(deftest push-try-pop-tt
  (test-runner/run
    (m/monadic
     [q (msq/create)]

     ;; one pusher
     (conc/fork
      (m/monadic
       (test-runner/mark)
       (msq/push q 42)
       (test-runner/unmark)))

     ;; one try-popper
     (test-runner/mark)
     [r1 (msq/try-pop q)]
     (test-runner/unmark)

     ;; another try-pop
     [r2 (msq/try-pop q)]

     ;; check
     (test-runner/is (xor (= r1 42)
                          (= r2 42)))
     )))

(defmacro imp [l r]
  `(if ~l ~r true))

(deftest push-cursor-tt
  (test-runner/run
    (m/monadic
     [q (msq/create)]

     ;; populate
     (msq/push q 42)
     (msq/push q 23)

     ;; obtain cursor
     [cur-1 (msq/cursor q)]

     ;; one pusher
     (conc/fork
      (m/monadic
       (test-runner/mark)
       (msq/push q 1337)
       (test-runner/unmark)))

     ;; cursor user
     (test-runner/mark)
     (let [r1 (msq/cursor-value cur-1)])
     [cur-2 (msq/cursor-next cur-1)]
     (let [r2 (msq/cursor-value cur-2)])
     [cur-3 (msq/cursor-next cur-2)]
     (test-runner/unmark)

     ;; check
     (test-runner/is= r1 42 "First result must be 42")
     (test-runner/is= r2 23 "Second result must be 23")
     (test-runner/is (imp cur-3
                          (= 1337
                             (msq/cursor-value cur-3))))
     )))

(deftest push-clean-until-tt
  (test-runner/run
    (m/monadic
     [q (msq/create)]

     ;; populate
     (msq/push q 42)
     (msq/push q 44)
     (msq/push q 46)

     ;; one pusher
     (conc/fork
      (m/monadic
       (test-runner/mark)
       (msq/push q 1337)
       (test-runner/unmark)))

     ;; clean
     (test-runner/mark)
     (msq/clean-until q (fn [x] (m/return (odd? x))))
     (test-runner/unmark)

     ;; check
     [r (msq/try-pop q)]
     (test-runner/is= r 1337 "1337 is odd")
     )))

(deftest try-pop-cursor-tt
  (test-runner/run
    (m/monadic
     ;; child result
     [res-1 (conc/new-ref :nothing)]

     ;; init
     [q (msq/create)]

     ;; populate
     (msq/push q 42)
     (msq/push q 23)

     ;; obtain cursor
     [cur-1 (msq/cursor q)]

     ;; one pusher
     (conc/fork
      (m/monadic
       (test-runner/mark)
       [res (msq/try-pop q)]
       (test-runner/unmark)
       (conc/reset res-1 res)
       ))

     ;; cursor user
     (test-runner/mark)
     (let [r1 (msq/cursor-value cur-1)]) ;; 42
     [cur-2 (msq/cursor-next cur-1)] ;; might be nil
     (let [r2 (msq/cursor-value cur-2)]) ;; 23 or nil
     [cur-3 (msq/cursor-next cur-2)] ;; nil
     (test-runner/unmark)

     ;; check
     (test-runner/is= r1 42 "First result must be 42")
     (test-runner/is (imp cur-2
                          (= 23
                             r2)))
     (test-runner/is= nil cur-3)
     )))






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
           (msq/clean-until q (fn [e] (m/return (even? e))))

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
