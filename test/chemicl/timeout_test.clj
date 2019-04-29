(ns chemicl.timeout-test
  (:require
   [chemicl.reagents :as rea]
   [chemicl.concurrency :as conc]
   [chemicl.timeout :as timeout]
   [clojure.test :as t :refer [deftest testing is]]))


(deftest timeout-t
  (let [res (atom :nothing)]
    ;; run reagent
    @(conc/run
       [result (rea/react! (rea/>>> (timeout/timeout 100)
                                    (rea/return :something)) nil)]
       (let [_ (reset! res result)])
       (conc/exit))

    (is (= :something @res))
    ))

(deftest choose-timeout-t
  (let [res (atom :nothing)]
    ;; run reagent
    @(conc/run
      [result (rea/react!
               (rea/choose
                (rea/>>>
                 (timeout/timeout 100)
                 (rea/return :left))
                (rea/>>>
                 (timeout/timeout 90)
                 (rea/return :right))) nil)]

      (let [_ (reset! res result)])
      (conc/exit))

    (is (= :right @res))
    ))
