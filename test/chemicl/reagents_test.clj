(ns chemicl.reagents-test
  (:require [chemicl.gearents :as rea]
            [active.clojure.monad :as m]
            [chemicl.monad :as cm :refer [defmonadic whenm]]
            [chemicl.refs :as refs]
            [chemicl.concurrency :as conc]
            [chemicl.channels :as channels]
            [chemicl.message-queue :as mq]
            [clojure.test :as t :refer [deftest testing is]]))

(deftest update-t
  (let [result-res (atom nil)
        ts (atom (refs/make-ref 1337 []))
        rea (rea/upd ts
                     (fn [[ov retv]]
                       [(inc ov) (dec retv)]))]
    (conc/run-many-to-many
     (m/monadic
      [res (rea/react! rea 23)]
      (let [_ (reset! result-res res)])
      (conc/print "done")))
    (Thread/sleep 20)
    (is (= @result-res 22))
    (is (= (refs/ref-data @ts) 1338))))

(deftest update-block-t
  (let [starter-res (atom :nothing)
        blocker-res (atom :nothing)
        ts (atom (refs/make-ref :nobody-was-here []))
        blocking-upd (rea/upd ts
                              (fn [[ov retv]]
                                (when (= ov :starter-was-here)
                                  [:blocker-was-here :blocker-res])))
        other-upd (rea/upd ts
                           (fn [[ov retv]]
                             [:starter-was-here :starter-res]))]

    ;; Run blocker
    (conc/run-many-to-many
     (m/monadic
      [res (rea/react! blocking-upd nil)]
      (let [_ (reset! blocker-res res)])
      (conc/print "blocker done")))

    ;; Check that blocker was not run yet
    (Thread/sleep 20)
    (is (= @blocker-res :nothing))
    (is (= (refs/ref-data @ts) :nobody-was-here))

    ;; Run starter
    (conc/run-many-to-many
     (m/monadic
      [res (rea/react! other-upd nil)]
      (let [_ (reset! starter-res res)])
      (conc/print "starter done"))) 

    ;; Check that blocking upd succeeded
    (Thread/sleep 20)
    (is (= @blocker-res :blocker-res))
    (is (= (refs/ref-data @ts) :blocker-was-here))

    ;; Check that starter upd succeeded
    (is (= @starter-res :starter-res))))











(def mq1 (atom []))
(def mq2 (atom []))

(def ep1 (channels/make-endpoint mq1 mq2))
(def ep2 (channels/make-endpoint mq2 mq1))

(pr-str ep1)

#_(defmonadic swapper [ep v]
  [res (rea/react! (rea/swap ep) v)]
  (conc/print "got: " (pr-str res))
  )

(defmonadic swapper-1 []
  (conc/print "--- swapper-1 ---")
  [res (rea/react! (rea/swap ep1) :from-1)]
  (conc/print "1 got" (pr-str res)))

(defmonadic swapper-2 []
  (conc/print "--- swapper-2 ---")
  [res (rea/react! (rea/swap ep2) :from-2)]
  (conc/print "2 got" (pr-str res)))

(conc/run-many-to-many (swapper-1))
(conc/run-many-to-many (swapper-2))


#_(conc/run-many-to-many (m/monadic
                        (conc/swapm
                         r
                         (fn [v]
                           (m/monadic
                            (conc/print "hi")
                            (m/return (inc v))
                            )))
                        [now (conc/read r)]
                        (conc/print "now: " (pr-str now))))
