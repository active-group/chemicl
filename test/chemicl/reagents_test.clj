(ns chemicl.reagents-test
  (:require [chemicl.gearents :as rea]
            [active.clojure.monad :as m]
            [chemicl.monad :as cm :refer [defmonadic whenm]]
            [chemicl.refs :as refs]
            [chemicl.concurrency :as conc]
            [chemicl.channels :as channels]
            [chemicl.message-queue :as mq]
            [clojure.test :as t :refer [deftest testing is]]))


;; ----------------------
;; --- Update -----------
;; ----------------------

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


;; ----------------------
;; --- Swap -------------
;; ----------------------

(deftest swap-t
  (let [res-1 (atom nil)
        res-2 (atom nil)

        ;; FIXME: this makes too many assumptions about the
        ;; internal structure of message queues and endpoints
        mq1 (atom [])
        mq2 (atom [])

        ep-1 (channels/make-endpoint mq1 mq2)
        ep-2 (channels/make-endpoint mq2 mq1)

        swapper (fn [ep v res-atom]
                  (m/monadic
                   [res (rea/react! (rea/swap ep) v)]
                   (let [_ (reset! res-atom res)])
                   (conc/print "done" (pr-str res))))

        swap-1 (swapper ep-1 :from-1 res-1)
        swap-2 (swapper ep-2 :from-2 res-2)
        ]

    ;; Run swap-1
    (conc/run-many-to-many swap-1)

    ;; Let swap-1 dangle for a while
    (Thread/sleep 100)

    ;; Run swap-2
    (conc/run-many-to-many swap-2)

    ;; Wait
    (Thread/sleep 20)

    ;; Check results
    (is (= @res-1 :from-2))
    (is (= @res-2 :from-1))
    ))


(deftest swap-with-upd-continuation-t
  (let [res-1 (atom nil)
        res-2 (atom nil)

        ref-1 (atom (refs/make-ref :nothing []))
        ref-2 (atom (refs/make-ref :nothing []))

        ;; FIXME: this makes too many assumptions about the
        ;; internal structure of message queues and endpoints
        mq1 (atom [])
        mq2 (atom [])

        ep-1 (channels/make-endpoint mq1 mq2)
        ep-2 (channels/make-endpoint mq2 mq1)

        swapper (fn [ep v res-atom r]
                  (m/monadic
                   [res (rea/react! (rea/>>>
                                     (rea/swap ep)
                                     (rea/upd r (fn [[ov a]]
                                                  ;; store input at ref 
                                                  [(+ a 100)
                                                   (+ a 1000)]))) v)]
                   (let [_ (reset! res-atom res)])
                   (conc/print "done" (pr-str res))))

        swap-1 (swapper ep-1 1 res-1 ref-1)
        swap-2 (swapper ep-2 2 res-2 ref-2)
        ]

    ;; Run swap-1
    (conc/run-many-to-many swap-1)

    ;; Let swap-1 dangle for a while
    (Thread/sleep 100)

    ;; Run swap-2
    (conc/run-many-to-many swap-2)

    ;; Wait
    (Thread/sleep 20)

    ;; Check results
    (is (= @res-1 1002)) ;; 2 (swap) + 1000 (upd)
    (is (= @res-2 1001)) ;; 1 (swap) + 1000 (upd)

    ;; Check refs
    (is (= (refs/ref-data @ref-1) 102))
    (is (= (refs/ref-data @ref-2) 101))
    ))


;; ----------------------
;; --- CAS --------------
;; ----------------------

(deftest cas-t
  (let [res (atom nil)
        ref (atom (refs/make-ref :nothing []))
        rea (rea/cas ref :nothing :something)]

    ;; run cas reagent
    (conc/run-many-to-many
     (m/monadic
      [out (rea/react! rea nil)]
      (let [_ (reset! res out)])
      (conc/print "done")))

    ;; wait
    (Thread/sleep 20)

    ;; Check ref
    (is (= (refs/ref-data @ref) :something))

    ;; Check result
    (is (= nil @res))
    ))


;; ----------------------
;; --- Read -------------
;; ----------------------

(deftest read-t
  (let [res (atom nil)
        ref (atom (refs/make-ref :bounty []))
        rea (rea/read ref)]

    ;; run read reagent
    (conc/run-many-to-many
     (m/monadic
      [out (rea/react! rea nil)]
      (let [_ (reset! res out)])
      (conc/print "done")))

    ;; wait
    (Thread/sleep 20)

    ;; Check result
    (is (= :bounty @res))
    ))
