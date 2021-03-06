(ns chemicl.kcas-test
  (:require [active.clojure.monad :as m]
            [chemicl.kcas :as kcas]
            [chemicl.monad :as cm :refer [defmonadic whenm]]
            [chemicl.concurrency :as conc]
            [chemicl.concurrency-test-runner :as test-runner]
            [clojure.test :as t :refer [deftest testing is]]))

(deftest kcas-1-t
  (let [ref-1 (atom :old-1)
        cs [[ref-1 :old-1 :new-1]]

        succ-1 (atom :nothing)

        ;; run reagent
        succ-1
        (conc/run (kcas/kcas cs))]

    ;; wait
    (Thread/sleep 20)

    ;; Check results
    (is (= true @succ-1))
    (is (= :new-1 @ref-1))
    ))

(deftest kcas-2-t
  (let [ref-1 (atom :old-1)
        ref-2 (atom :old-2)
        cs [[ref-1 :old-1 :new-1] 
            [ref-2 :old-2 :new-2]]
        succ-1 (atom :nothing)

        ;; run
        succ-1
        (conc/run (kcas/kcas cs))]

    ;; wait
    (Thread/sleep 50)

    ;; Check results
    (is (= true @succ-1))
    (is (= :new-1 @ref-1))
    (is (= :new-2 @ref-2))
    ))


(deftest kcas-interference-t
  (let [ref-1 (atom :old-1)
        ref-2 (atom :old-2)
        cs-1 [[ref-1 :old-1 :new-1-1] 
              [ref-2 :old-2 :new-2-1]]
        cs-2 [[ref-1 :old-1 :new-1-2] 
              [ref-2 :old-2 :new-2-2]]
        succ-1 (atom false)
        succ-2 (atom false)]

    ;; run two kcas ops
    (conc/run
     ;; 1
     (conc/fork
      (m/monadic
       [succ (kcas/kcas cs-1)]
       (conc/reset succ-1 succ)))
     ;; 2
     (conc/fork
      (m/monadic
       [succ (kcas/kcas cs-2)]
       (conc/reset succ-2 succ))))

    ;; wait
    (Thread/sleep 100)

    ;; One must succeed
    (is (or @succ-1
            @succ-2))

    ))


(defn mk-two-cas! [_]
  (let [ov (rand)
        ref (atom ov)
        nv-1 (rand)
        nv-2 (rand)]
    [[ref ov nv-1]
     [ref ov nv-2]]))

(defn mk-cas-list-pair! [n]
  (let [cas-pairs (map mk-two-cas! (range n))]
    [(map first cas-pairs)
     (map second cas-pairs)]))

;; FIXME: nondeterministic
(deftest kcas-interference-2-t
  (let [[cs-1-all cs-2-all] (mk-cas-list-pair! 8)
        cs-1 (drop 0 cs-1-all)
        cs-2 (drop 7 cs-2-all)

        ;; run one
        succ-1
        (conc/run (kcas/kcas cs-1))

        ;; run two
        succ-2
        (conc/run (kcas/kcas cs-2))]

    ;; wait
    (Thread/sleep 200)

    ;; One must succeed
    (is (or @succ-1
            @succ-2))
    ))

(deftest ^:slow kcas-interference-3-t
  ;; run test
  (test-runner/run
   (m/monadic
    ;; return values
    [succ-1 (conc/new-ref false)]
    [succ-2 (conc/new-ref false)]

    (let [[cs-1 cs-2] (mk-cas-list-pair! 3)])

    ;; self
    [parent (conc/get-current-task)]

    ;; one
    (conc/fork
     (m/monadic
      (test-runner/mark)
      [succ (kcas/kcas cs-1)]
      (test-runner/unmark)
      (conc/reset succ-1 succ)
      (conc/unpark parent)
      ))

    ;; two
    (m/monadic
     (test-runner/mark)
     [succ (kcas/kcas cs-2)]
     (test-runner/unmark)
     (conc/reset succ-2 succ))

    ;; wait for child
    (conc/park)

    [s1 (conc/read succ-1)]
    [s2 (conc/read succ-2)]

    (test-runner/is (or s1 s2))
    )))
