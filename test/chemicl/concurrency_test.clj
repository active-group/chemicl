(ns chemicl.concurrency-test
  (:require
   [chemicl.concurrency :as conc]
   [active.clojure.monad :as m]
   [chemicl.monad :as cm :refer [defmonadic whenm]]
   [clojure.test :as t :refer [deftest testing is]]))

(deftest promise-t
  (testing "simple promise"
    (let [m (m/return 123)
          p (conc/run m)]
      (is (= @p 123))
      ))

  (testing "waiting promise"
    (let [m (m/monadic
             (conc/timeout 100)
             (m/return 123))
          p (conc/run m)]
      (is (not (realized? p)))
      (is (= @p 123))
      (is (realized? p))
      )))
