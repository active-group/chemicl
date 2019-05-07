(ns chemicl.io-test
  (:require
   [chemicl.io :as io]
   [chemicl.reagents :as rea]
   [chemicl.concurrency :as conc]
   [chemicl.timeout :as timeout]
   [clojure.java.io :as jio]
   [clojure.test :as t :refer [deftest testing is]]))


(deftest buffered-reader-t
  (let [fr (java.io.StringReader. "Hi wie geht")
        br (java.io.BufferedReader. fr)
        res (atom :nothing)]
    ;; run reagent
    (conc/run
      [result (rea/react! (rea/>>> (io/read-buffered-reader br)
                                   (rea/lift clojure.string/upper-case)) nil)]
      (let [_ (reset! res result)])
      (conc/exit))

    (Thread/sleep 100)

    ;; asert something
    (is (= "HI WIE GEHT" @res))
    ))

(deftest buffered-reader-blocks-t
  (let [fr (java.io.StringReader. "")
        br (java.io.BufferedReader. fr)
        res (atom :nothing)]
    ;; run reagent
    (conc/run
      [result (rea/react! (rea/>>> (io/read-buffered-reader br)
                                   (rea/lift clojure.string/upper-case)) nil)]
      (let [_ (reset! res result)])
      (conc/exit))

    (Thread/sleep 100)

    ;; asert something
    (is (= :nothing @res))
    ))
