(ns chemicl.timeout-test
  (:require
   [chemicl.reagents :as rea]
   [chemicl.concurrency :as conc]
   [chemicl.timeout :as timeout]
   [clojure.test :as t :refer [deftest testing is]]))


(deftest timeout-t
  (let [res (atom :nothing)]
    ;; run reagent
    (conc/run
      [result (rea/react! (rea/>>> (timeout/timeout 1000)
                                   (rea/return :something)) nil)]
      (let [_ (reset! res result)])
      (conc/exit))

    ;; assert :nothing
    (is (= :nothing @res))

    ;; assert :something
    (Thread/sleep 1100)
    (is (= :something @res))
    ))

(deftest choose-timeout-t
  (let [res (atom :nothing)]
    ;; run reagent
    (conc/run
      [result (rea/react!
               (rea/choose
                (rea/>>>
                 (timeout/timeout 1000)
                 (rea/return :left))
                (rea/>>>
                 (timeout/timeout 900)
                 (rea/return :right))) nil)]

      (let [_ (reset! res result)])
      (conc/exit))

    ;; assert :nothing
    (is (= :nothing @res))

    ;; assert :something
    (Thread/sleep 1100)
    (is (= :right @res))
    ))
