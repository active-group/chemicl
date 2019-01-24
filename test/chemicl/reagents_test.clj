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


;; ----------------------
;; --- Return -----------
;; ----------------------

(deftest return-t
  (let [res (atom nil)
        rea (rea/return :bounty)]

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


;; ----------------------
;; --- Lift -------------
;; ----------------------

(deftest lift-t
  (let [res (atom :nothing)
        rea (rea/lift inc)]

    ;; run lift reagent
    (conc/run-many-to-many
     (m/monadic
      [out (rea/react! rea 23)]
      (let [_ (reset! res out)])
      (conc/print "done")))

    ;; wait
    (Thread/sleep 10)

    ;; Check result
    (is (= 24 @res))
    ))


;; ----------------------
;; --- Choose -----------
;; ----------------------

(deftest choose-t
  (let [res-1-2 (atom :nothing)
        res-2-1 (atom :nothing)
        res-1-3 (atom :nothing)
        res-3-1 (atom :nothing)
        res-3-4 (atom :nothing)

        ref (atom (refs/make-ref :bounty []))

        rea-1 (rea/return :one)
        rea-2 (rea/return :two)
        rea-3 (rea/upd ref (fn [[ov a]] nil))
        rea-4 (rea/upd ref (fn [[ov a]] nil))

        rea-1-2 (rea/choose rea-1 rea-2)
        rea-2-1 (rea/choose rea-2 rea-1)
        rea-1-3 (rea/choose rea-1 rea-3)
        rea-3-1 (rea/choose rea-3 rea-1)
        rea-3-4 (rea/choose rea-3 rea-4)]

    ;; --- Neither blocks
    ;; run rea-1-2
    (conc/run-many-to-many
     (m/monadic
      [out (rea/react! rea-1-2 nil)]
      (let [_ (reset! res-1-2 out)])
      (conc/print "done-1-2")))

    ;; wait
    (Thread/sleep 10)

    ;; Check res-1-2
    (is (= :one @res-1-2))

    ;; run rea-2-1
    (conc/run-many-to-many
     (m/monadic
      [out (rea/react! rea-2-1 nil)]
      (let [_ (reset! res-2-1 out)])
      (conc/print "done-2-1")))

    ;; wait
    (Thread/sleep 10)

    ;; Check res-2-1
    (is (= :two @res-2-1))


    ;; --- Right blocks
    ;; run rea-1-3
    (conc/run-many-to-many
     (m/monadic
      [out (rea/react! rea-1-3 nil)]
      (let [_ (reset! res-1-3 out)])
      (conc/print "done-1-3")))

    ;; wait
    (Thread/sleep 10)

    ;; Check res-1-3
    (is (= :one @res-1-3))

    ;; --- Left blocks
    ;; run rea-3-1
    (conc/run-many-to-many
     (m/monadic
      [out (rea/react! rea-3-1 nil)]
      (let [_ (reset! res-3-1 out)])
      (conc/print "done-3-1")))

    ;; wait
    (Thread/sleep 40)

    ;; Check res-1-3
    (is (= :one @res-3-1))


    ;; --- Both block
    ;; run rea-3-1
    (conc/run-many-to-many
     (m/monadic
      [out (rea/react! rea-3-4 nil)]
      (let [_ (reset! res-3-4 out)])
      (conc/print "done-3-4")))

    ;; wait
    (Thread/sleep 1000)

    ;; Check res-3-4
    (is (= :nothing @res-3-4))
    ))


;; ----------------------
;; --- Post-Commit ------
;; ----------------------

(deftest post-commit-t
  (let [res (atom :nope)
        ref (atom :nein)
        rea (rea/>>>
             (rea/return 23)
             (rea/post-commit
              (fn [a]
                (m/monadic
                 (conc/reset ref (inc a))))))]

    ;; run reagent
    (conc/run-many-to-many
     (m/monadic
      [out (rea/react! rea nil)]
      (let [_ (reset! res out)])
      (conc/print "done")))

    ;; wait
    (Thread/sleep 40)

    ;; Check results
    (is (= 23 @res))
    (is (= 24 @ref))
    ))
