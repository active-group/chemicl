(ns chemicl.offers-test
  (:require [chemicl.offers :as o]
            [chemicl.concurrency :as conc]
            [chemicl.concurrency-test-runner :as test-runner]
            [chemicl.reaction-data :as rx-data]
            [chemicl.reactions :as rx]
            [chemicl.maybe :as maybe :refer [just nothing]]
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
     (test-runner/is= r1 (nothing) "rescind result must be nothing")
     (test-runner/is= r2 (nothing) "rescind result must be nothing")

     ;; FIXME: this is based on assumption about offer internals
     [o (conc/read oref)]
     (test-runner/is (o/rescinded? o) "offer must be rescinded")
     )))

(deftest rescind-wait-tt
  (test-runner/run
    (m/monadic
     ;; init
     [oref (o/new-offer)]

     (conc/fork
      (m/monadic
       (test-runner/mark)
       [res (o/rescind oref)]
       (test-runner/unmark)
       ))

     (test-runner/mark)
     (o/wait oref)
     (test-runner/unmark)

     ;; check
     [o (conc/read oref)]
     (test-runner/is (o/rescinded? o) "offer must be rescinded")
     )))

(deftest rescind-complete-1-t
  (test-runner/run
    (m/monadic
     ;; init
     [oref (o/new-offer)]

     ;; rescind
     [rres (o/rescind oref)]

     ;; complete
     [cres (o/complete oref :schwenker)]

     (test-runner/is= (nothing) rres)
     (test-runner/is= (rx-data/failing-rx) cres)
     )))

(deftest rescind-complete-2-t
  (test-runner/run
    (m/monadic
     ;; init
     [oref (o/new-offer)]

     ;; complete
     [cres (o/complete oref :schwenker)]

     ;; commit completion
     (rx/try-commit cres)

     ;; rescind
     [rres (o/rescind oref)]

     (test-runner/is (= rres (just :schwenker))))))

(defmacro => [l r]
  `(or (not ~l) ~r))

(deftest rescind-complete-tt
  (test-runner/run
    (m/monadic
     ;; init
     [rescinder-res (conc/new-ref :nothing)]
     [oref (o/new-offer)]

     [parent (conc/get-current-task)]

     ;; rescinder
     (conc/fork
      (m/monadic
       (test-runner/mark)
       [res (o/rescind oref)]
       (test-runner/unmark)

       (conc/reset rescinder-res res)
       (conc/unpark parent nil)
       ))

     (test-runner/mark)

     ;; complete
     [cres (o/complete oref :schwenker)]

     ;; commit
     [succ (rx/try-commit cres)]

     (test-runner/unmark)

     ;; wait for rres
     (conc/park)

     ;; check
     [rres (conc/read rescinder-res)]
     (test-runner/is (=> (not succ) (= (nothing) rres)))
     (test-runner/is (=> succ (= rres (just :schwenker))))
     )))

;; no wait-wait-tt, because wait can only be called by the same thread
;; that created the offer (the "owner")

(deftest wait-complete-t
  (test-runner/run
    (m/monadic
     ;; init
     [oref (o/new-offer)]
     [done-ref (conc/new-ref false)]
     [parent (conc/get-current-task)]

     ;; two scenarios:
     ;; 1. wait is run before complete + commit
     ;;    -> wait returns false, does not block
     ;; 2. complete + commit is run before wait
     ;;    -> wait blocks, commit wakes it up again

     ;; wait
     (conc/fork
      (m/monadic
       (test-runner/mark)
       (o/wait oref)
       (test-runner/unmark)

       (conc/reset done-ref true)
       (conc/unpark parent nil)
       ))

     (test-runner/mark)

     ;; complete
     [cres (o/complete oref :schwenker)]

     ;; commit
     [succ (rx/try-commit cres)]

     (test-runner/unmark)

     ;; wait for child
     (whenm succ
       (conc/park))

     ;; check
     [done? (conc/read done-ref)]
     (test-runner/is (=> succ done?))
     (test-runner/is (=> (not succ) (not done?)))
     )))
