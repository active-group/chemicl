(ns chemicl.io-test
  (:require
   [chemicl.io :as io]
   [chemicl.reagents :as rea]
   [chemicl.concurrency :as conc]
   [chemicl.timeout :as timeout]
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


(deftest read-file-t
  (let [url "file:///Users/markusschlegel/foo.txt"
        res (atom :nothing)]

    ;; run reagent
    (conc/run
      [result (rea/react! (io/read-file url) nil)]
      (let [_ (reset! res result)])
      (conc/exit))

    (Thread/sleep 100)

    ;; assert something
    (is (= "hi wie geht\n" @res))
    ))

(deftest choose-read-file-t
  (let [url "file:///Users/markusschlegel/foo.txt"
        res (atom :nothing)]

    ;; run reagent
    (conc/run
      [result (rea/react! (rea/choose
                           (io/read-file url)
                           (rea/return :too-late)) nil)]
      (let [_ (reset! res result)])
      (conc/exit))

    (Thread/sleep 100)

    ;; assert something
    (is (= :too-late @res))
    ))

(defmacro fn->> [& forms]
  `(fn [arg#]
     (->> arg# ~@forms)))

(deftest readf-t
  (let [url "file:///Users/markusschlegel/foo.txt"
        res (atom :nothing)]

    ;; run reagent
    (conc/run
      [result
       (rea/react!
        (rea/>>>
         (io/readf url)
         (rea/lift
          (fn->>
           (.decode (java.nio.charset.Charset/forName "UTF-8"))
           (.toString)))) nil)]

      (let [_ (reset! res result)])
      (conc/exit))

    (Thread/sleep 100)

    ;; assert something
    (is (= "hi wie geht\n" @res))
    ))
