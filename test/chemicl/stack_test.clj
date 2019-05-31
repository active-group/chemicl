(ns chemicl.stack-test
  (:require [chemicl.stack :as stack]
            [active.clojure.monad :as m]
            [chemicl.concurrency :as conc]
            [chemicl.concurrency-test-runner :as test-runner]
            [clojure.test :as t :refer [deftest testing is]]))

(deftest push-try-pop-valid-sequential-tt
  (test-runner/run
   (m/monadic

    (let [pred (fn [x]
                 (m/return
                  (:active? x)))])

    [st (stack/new-stack)]

    ;; push
    (stack/push-or-reuse st {:value "hi"
                             :active? true}
                         pred)

    ;; pop
    [r1 (stack/try-pop-valid st pred)]

    ;; check
    (test-runner/is (and r1
                         (= (:value r1)
                            "hi")))
    )))

(deftest push-try-pop-valid-parallel-tt
  (test-runner/run
   (m/monadic

    [parent (conc/get-current-task)]

    (let [pred (fn [x]
                 (m/return
                  (:active? x)))])

    [st (stack/new-stack)]

    ;; push
    (conc/fork
     (m/monadic
      (test-runner/mark)
      (stack/push-or-reuse st {:value "hi"
                               :active? true}
                           pred)
      (test-runner/unmark)))

    ;; pop
    (test-runner/mark)
    [r1 (stack/try-pop-valid st pred)]
    (test-runner/unmark)
    (conc/unpark parent)

    (conc/park)

    ;; check
    (test-runner/is (or (nil? r1)
                        (and r1
                             (= (:value r1)
                                "hi"))))
    )))

(deftest try-pop-valid-empty-tt
  (test-runner/run
   (m/monadic

    (let [pred (fn [x]
                 (m/return
                  (:active? x)))])

    [st (stack/new-stack)]

    ;; pop
    [r1 (stack/try-pop-valid st pred)]

    ;; check
    (test-runner/is (nil? r1))
    )))

(deftest push-or-reuse-tt
  (test-runner/run
    (m/monadic

     (let [pred (fn [x]
                  (m/return
                   (:active? x)))])

     [st (stack/new-stack)]

     ;; push inactive
     (stack/push-or-reuse st {:value "inactive"
                              :active? false}
                          pred)
     (stack/push-or-reuse st {:value "active"
                              :active? true}
                          pred)

     ;; pop
     [r1 (stack/try-pop-valid st pred)]
     [r2 (stack/try-pop-valid st pred)]

     ;; check
     (test-runner/is (and r1
                          (= (:value r1)
                             "active")))
     (test-runner/is (nil? r2))
     )))

(deftest push-or-reuse-parallel-tt
  (test-runner/run
   (m/monadic

    [parent (conc/get-current-task)]

    (let [pred (fn [x]
                 (m/return
                  (:active? x)))])

    [st (stack/new-stack)]

    ;; push active
    (stack/push-or-reuse st {:value "first active"
                             :active? true}
                         pred)

    ;; push inactive
    (stack/push-or-reuse st {:value "inactive"
                             :active? false}
                         pred)

    (conc/fork
     (m/monadic
      (test-runner/mark)
      (stack/push-or-reuse st {:value "second active"
                               :active? true}
                           pred)
      (test-runner/unmark)
      (conc/unpark parent)))

    ;; pop
    (test-runner/mark)
    [r1 (stack/try-pop-valid st pred)]
    (test-runner/unmark)

    (conc/park)

    [r2 (stack/try-pop-valid st pred)]

    ;; check
    (test-runner/is (or
                     (and
                      (= (:value r1)
                         "second active")
                      (= (:value r2)
                         "first active"))

                     (and
                      (= (:value r1)
                         "first active")
                      (= (:value r2)
                         "second active"))
                     )))))
