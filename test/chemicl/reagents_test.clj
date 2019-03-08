(ns chemicl.reagents-test
  (:require [chemicl.reagents :as rea]
            [active.clojure.monad :as m]
            [chemicl.monad :as cm :refer [defmonadic whenm]]
            [chemicl.refs :as refs]
            [chemicl.concurrency :as conc]
            [chemicl.channels :as channels]
            [chemicl.message-queue :as mq]
            [clojure.test :as t :refer [deftest testing is]]
            [chemicl.concurrency-test-runner :as test-runner]))


;; ----------------------
;; --- Swap -------------
;; ----------------------

(deftest swap-t
  (let [res-1 (atom nil)
        res-2 (atom nil)

        ep-1-atom (atom nil)
        ep-2-atom (atom nil)

        swapper (fn [ep v res-atom]
                  (m/monadic
                   [res (rea/react! (rea/swap ep) v)]
                   (let [_ (reset! res-atom res)])
                   (conc/print "done" (pr-str res))))
        ]

    ;; Run initializer
    (conc/run
      [[ep-1 ep-2] (channels/new-channel)]
      (let [_ (reset! ep-1-atom ep-1)])
      (let [_ (reset! ep-2-atom ep-2)])
      (m/return nil))

    (Thread/sleep 20)

    ;; Run swap-1
    (conc/run
      (swapper @ep-1-atom :from-1 res-1))

    ;; Let swap-1 dangle for a while
    (Thread/sleep 100)

    ;; Run swap-2
    (conc/run
      (swapper @ep-2-atom :from-2 res-2))

    ;; Wait
    (Thread/sleep 20)

    ;; Check results
    (is (= @res-1 :from-2))
    (is (= @res-2 :from-1))
    ))


(deftest swap-with-upd-continuation-t
  (let [res-res-1 (atom nil)
        res-res-2 (atom nil)
        mem-res-1 (atom nil)
        mem-res-2 (atom nil)

        ep-1-atom (atom nil)
        ep-2-atom (atom nil)

        ref-1-atom (atom nil)
        ref-2-atom (atom nil)

        swapper (fn [ep v res-res-atom mem-res-atom r]
                  (m/monadic
                   [res (rea/react! (rea/>>>
                                     (rea/swap ep)
                                     (rea/upd r (fn [[ov a]]
                                                  ;; store input at ref 
                                                  [(+ a 100)
                                                   (+ a 1000)]))) v)]
                   (let [_ (reset! res-res-atom res)])
                   [mem (refs/read r)]
                   (let [_ (reset! mem-res-atom mem)])
                   (conc/print "done" (pr-str res))))
        ]

    ;; Run initializer
    (conc/run
      [[ep-1 ep-2] (channels/new-channel)]
      (let [_ (reset! ep-1-atom ep-1)])
      (let [_ (reset! ep-2-atom ep-2)])

      [ref-1 (refs/new-ref :nothing)]
      [ref-2 (refs/new-ref :nothing)]
      (let [_ (reset! ref-1-atom ref-1)])
      (let [_ (reset! ref-2-atom ref-2)])

      (m/return nil))

    (Thread/sleep 20)

    ;; Run swap-1
    (conc/run
      (swapper @ep-1-atom 1 res-res-1 mem-res-1 @ref-1-atom))

    ;; Let swap-1 dangle for a while
    (Thread/sleep 100)

    ;; Run swap-2
    (conc/run
      (swapper @ep-2-atom 2 res-res-2 mem-res-2 @ref-2-atom))

    ;; Wait
    (Thread/sleep 50)

    ;; Check results
    (is (= @res-res-1 1002)) ;; 2 (swap) + 1000 (upd)
    (is (= @res-res-2 1001)) ;; 1 (swap) + 1000 (upd)

    ;; Check refs
    (is (= @mem-res-1 102))
    (is (= @mem-res-2 101))
    ))

(deftest swap-tt
  (test-runner/run-randomized-n
   1000
   (m/monadic
    ;; init
    [[ep1 ep2] (channels/new-channel)]
    [res-1 (conc/new-ref :nothing)]
    [parent (conc/get-current-task)]

    ;; swapper 1
    (conc/fork
     (m/monadic
      (test-runner/mark)
      [res (rea/react! (rea/swap ep1) :from-1)]
      (test-runner/unmark)

      (conc/reset res-1 res)
      (conc/unpark parent nil)
      ))

    ;; swapper 2
    (test-runner/mark)
    [r2 (rea/react! (rea/swap ep2) :from-2)]
    (test-runner/unmark)

    ;; wait for swapper 1
    (conc/park)

    ;; check
    [r1 (conc/read res-1)]
    (test-runner/is (= :from-1 r2))
    (test-runner/is (= :from-2 r1))
    )))

(deftest swap-twice-tt
  (testing "two pairs of swaps on the same channel"
    (test-runner/run-randomized-n
     1000
     (m/monadic
      ;; init
      [[ep1 ep2] (channels/new-channel)]
      [res-1-1 (conc/new-ref :nothing)]
      [res-1-2 (conc/new-ref :nothing)]
      [parent (conc/get-current-task)]

      ;; swapper 1
      (conc/fork
       (m/monadic
        (test-runner/mark)
        [r1 (rea/react! (rea/swap ep1) :from-1-1)]
        [r2 (rea/react! (rea/swap ep1) :from-1-2)]
        (test-runner/unmark)

        (conc/reset res-1-1 r1)
        (conc/reset res-1-2 r2)
        (conc/unpark parent nil)
        ))

      ;; swapper 2
      (test-runner/mark)
      [r2-1 (rea/react! (rea/swap ep2) :from-2-1)]
      [r2-2 (rea/react! (rea/swap ep2) :from-2-2)]
      (test-runner/unmark)

      ;; wait for swapper 1
      (conc/park)

      ;; check
      [r1-1 (conc/read res-1-1)]
      [r1-2 (conc/read res-1-2)]
      (test-runner/is= :from-1-1 r2-1)
      (test-runner/is= :from-1-2 r2-2)
      (test-runner/is= :from-2-1 r1-1)
      (test-runner/is= :from-2-2 r1-2)
      ))))

(deftest swap-triple-tt
  (testing "only a single pair of reagents can swap on an endpoint at any time"
    (test-runner/run-randomized-n
     1000
     (m/monadic
      ;; init
      [[ep1 ep2] (channels/new-channel)]
      [res-1 (conc/new-ref :nothing)]
      [res-2 (conc/new-ref :nothing)]

      [parent (conc/get-current-task)]

      ;; swapper 1
      (conc/fork
       (m/monadic
        (test-runner/mark)
        [res (rea/react! (rea/swap ep1) nil)]
        #_(conc/print "1 got" res)
        (test-runner/unmark)

        (conc/reset res-1 res)
        (conc/unpark parent nil)
        ))

      ;; swapper 2
      (conc/fork
       (m/monadic
        (test-runner/mark)
        [res (rea/react! (rea/swap ep1) nil)]
        #_(conc/print "2 got" res)
        (test-runner/unmark)

        (conc/reset res-2 res)
        (conc/unpark parent nil)
        ))

      ;; swapper on dual endpoint
      (test-runner/mark)
      #_(conc/print "3 gogo")
      [r3 (rea/react! (rea/swap ep2) :bounty)]
      (test-runner/unmark)

      ;; wait for either swapper 1 or swapper 2
      #_(conc/print "sleeping")
      (conc/park)

      ;; check
      [r1 (conc/read res-1)]
      [r2 (conc/read res-2)]
      (test-runner/is (or (= :bounty r1)
                          (= :bounty r2)) "One or both must have found bounty")
      (test-runner/is (not (and (= :bounty r1)
                                (= :bounty r2))) "Not both must find bounty")
      ))))

(deftest swap-then-upd-tt
  (test-runner/run-randomized-n
   1000
   (m/monadic
    ;; init
    [[ep1 ep2] (channels/new-channel)]
    [r-1 (refs/new-ref :nothing)]
    [r-2 (refs/new-ref :nothing)]
    [res-1 (conc/new-ref :nothing)]

    [parent (conc/get-current-task)]

    ;; swap-then-upd 1
    (conc/fork
     (m/monadic
      (test-runner/mark)
      [res (rea/react! (rea/>>>
                        (rea/swap ep1)
                        (rea/upd r-1 (fn [[ov a]]
                                       [a 42]))) :hi-from-1)]
      (test-runner/unmark)

      (conc/reset res-1 res)
      (conc/unpark parent nil)))

    ;; swap-then-upd 2
    (test-runner/mark)
    [res (rea/react! (rea/>>>
                      (rea/swap ep2)
                      (rea/upd r-2 (fn [[ov a]]
                                     [a 23]))) :hi-from-2)]
    (test-runner/unmark)

    ;; wait for 1
    (conc/park)

    [r1 (refs/read r-1)]
    [r2 (refs/read r-2)]
    [retv-1 (conc/read res-1)]
    (let [retv-2 res])

    (test-runner/is= :hi-from-2 r1)
    (test-runner/is= :hi-from-1 r2)
    (test-runner/is= 42 retv-1)
    (test-runner/is= 23 retv-2)
    )))

;; ----------------------
;; --- Update -----------
;; ----------------------

(deftest update-t
  (let [res-res (atom nil)
        mem-res (atom nil)]

    ;; run
    (conc/run
      [ts (refs/new-ref 1337)]

      [res (rea/react!
            (rea/upd ts
                     (fn [[ov retv]]
                       [(inc ov) (dec retv)])) 23)]

      [ts-val (refs/read ts)]
      (let [_ (reset! res-res res)
            _ (reset! mem-res ts-val)])
      (conc/print "done"))

    ;; wait
    (Thread/sleep 20)

    ;; check
    (is (= @res-res 22))
    (is (= @mem-res 1338))))

(deftest update-monadic-t
  (let [res-res (atom nil)
        mem-res (atom nil)]

    ;; run
    (conc/run
      [ts (refs/new-ref 1337)]

      [res (rea/react!
            (rea/upd ts
                     (fn [[ov retv]]
                       (m/return
                        [(inc ov) (dec retv)]))) 23)]

      [ts-val (refs/read ts)]
      (let [_ (reset! res-res res)
            _ (reset! mem-res ts-val)])
      (conc/print "done"))

    ;; wait
    (Thread/sleep 20)

    ;; check
    (is (= @res-res 22))
    (is (= @mem-res 1338))))

(deftest update-block-t
  (let [starter-res (atom :nothing)
        blocker-res (atom :nothing)
        ts-atom (atom nil)
        blocking-upd (fn [ts] (rea/upd ts
                                       (fn [[ov retv]]
                                         (when (= ov :starter-was-here)
                                           [:blocker-was-here :blocker-res]))))
        other-upd (fn [ts] (rea/upd ts
                                    (fn [[ov retv]]
                                      [:starter-was-here :starter-res])))]

    ;; Run initializer
    (conc/run
      [ts (refs/new-ref :nobody-was-here)]
      (let [_ (reset! ts-atom ts)])
      (m/return nil))

    (Thread/sleep 20)

    ;; Run blocker
    (conc/run
      [res (rea/react! (blocking-upd @ts-atom) nil)]
      (let [_ (reset! blocker-res res)])
      (conc/print "blocker done"))

    ;; Check that blocker was not run yet
    (Thread/sleep 20)
    (is (= @blocker-res :nothing))
    (is (= @(refs/ref-data-ref @ts-atom) :nobody-was-here))

    ;; Run starter
    (conc/run
      [res (rea/react! (other-upd @ts-atom) nil)]
      (let [_ (reset! starter-res res)])
      (conc/print "starter done")) 

    ;; Check that blocking upd succeeded
    (Thread/sleep 20)
    (is (= @blocker-res :blocker-res))
    (is (= @(refs/ref-data-ref @ts-atom) :blocker-was-here))

    ;; Check that starter upd succeeded
    (is (= @starter-res :starter-res))))

(deftest update-tt
  (let [blocking-upd (fn [r] (rea/upd r
                                      (fn [[ov retv]]
                                        (when (= ov :starter-was-here)
                                          [:blocker-was-here :blocker-res]))))
        other-upd (fn [r] (rea/upd r
                                   (fn [[ov retv]]
                                     [:starter-was-here :starter-res])))]

    ;; Run
    (test-runner/run-randomized-n
     1000
     (m/monadic
      [parent (conc/get-current-task)]
      [blocker-res (conc/new-ref :nothing)]
      [r (refs/new-ref :nobody-was-here)]

      ;; run blocking-upd
      (conc/fork
       (m/monadic
        (test-runner/mark)
        [res (rea/react! (blocking-upd r) nil)]
        (test-runner/unmark)

        (conc/reset blocker-res res)
        (conc/unpark parent nil)
        ))

      ;; run other-upd
      (test-runner/mark)
      (rea/react! (other-upd r) nil)
      (test-runner/unmark)

      ;; sleep
      (conc/park)

      ;; check results
      [blr (conc/read blocker-res)]
      [rr (refs/read r)]
      (test-runner/is= blr :blocker-res)
      (test-runner/is= rr :blocker-was-here)
      ))))


;; ----------------------
;; --- CAS --------------
;; ----------------------

(deftest cas-t
  (let [res-res (atom nil)
        mem-res (atom nil)]

    ;; run cas reagent
    (conc/run
      [r (refs/new-ref :nothing)]
      [out (rea/react! (rea/cas r :nothing :something) nil)]
      (let [_ (reset! res-res out)])
      [mem (refs/read r)]
      (let [_ (reset! mem-res mem)])
      (conc/print "done"))

    ;; wait
    (Thread/sleep 20)

    ;; Check ref
    (is (= @mem-res :something))

    ;; Check result
    (is (= nil @res-res))
    ))

(deftest cas-tt
  (test-runner/run
    (m/monadic
     [r (refs/new-ref :nothing)]
     (rea/react! (rea/cas r :nothing :something) nil)
     [v (refs/read r)]
     (test-runner/is= :something v)
     )))


;; ----------------------
;; --- Read -------------
;; ----------------------

(deftest read-t
  (let [res-res (atom nil)
        mem-res (atom nil)]

    ;; run read reagent
    (conc/run
      [r (refs/new-ref :nothing)]
      [out (rea/react! (rea/read r) nil)]
      (let [_ (reset! res-res out)])
      [mem (refs/read r)]
      (let [_ (reset! mem-res mem)])
      (conc/print "done"))

    ;; wait
    (Thread/sleep 20)

    ;; Check ref
    (is (= :nothing @mem-res))

    ;; Check result
    (is (= :nothing @res-res))
    ))

(deftest read-tt
  (test-runner/run
    (m/monadic
     [r (refs/new-ref :bounty)]
     [res (rea/react! (rea/read r) nil)]
     (test-runner/is= :bounty res)
     )))


;; ----------------------
;; --- Choose -----------
;; ----------------------

(defmacro => [l r]
  `(or (not ~l) ~r))

(deftest choose-tt
  (test-runner/run-randomized-n
   1000
   (m/monadic
    ;; init
    [lref (refs/new-ref :nothing)]
    [rref (refs/new-ref :nothing)]

    ;; go left
    (conc/fork
     (test-runner/with-mark
       (rea/react! (rea/upd lref (fn [_] [:go nil])) nil)))

    ;; go right
    (conc/fork
     (test-runner/with-mark
       (rea/react! (rea/upd rref (fn [_] [:go nil])) nil)))

    ;; chooser
    [res (test-runner/with-mark
           (rea/react!
            (rea/choose
             (rea/upd lref
                      (fn [[ov a]]
                        (when (= :go ov)
                          [:done :left])))
             (rea/upd rref
                      (fn [[ov a]]
                        (when (= :go ov)
                          [:done :right])))) nil))]

    ;; check
    [left (refs/read lref)]
    [right (refs/read rref)]

    (test-runner/is (=> (= res :left)
                        (= left :done))
                    (str "When left won the race, lref must be :done but is " left))

    (test-runner/is (=> (= res :left)
                        (not= right :done))
                    (str "When left won the race, rref must not be :done but is " right))

    (test-runner/is (=> (= res :right)
                        (= right :done))
                    (str "When right won the race, rref must be :done but is " right))

    (test-runner/is (=> (= res :right)
                        (not= left :done))
                    (str "When right won the race, lref must be :nothing but is " left))
    )))

(deftest choose-t
  (let [res-1-2 (atom :nothing)
        res-2-1 (atom :nothing)
        res-1-3 (atom :nothing)
        res-3-1 (atom :nothing)
        res-3-4 (atom :nothing)

        ref (atom nil)

        ;; initialize
        _ (conc/run
            [r (refs/new-ref :bounty)]
            (let [_ (reset! ref r)])
            (m/return nil))

        _ (Thread/sleep 20)

        rea-1 (rea/return :one)
        rea-2 (rea/return :two)
        rea-3 (rea/upd @ref (fn [[ov a]] nil))
        rea-4 (rea/upd @ref (fn [[ov a]] nil))

        rea-1-2 (rea/choose rea-1 rea-2)
        rea-2-1 (rea/choose rea-2 rea-1)
        rea-1-3 (rea/choose rea-1 rea-3)
        rea-3-1 (rea/choose rea-3 rea-1)
        rea-3-4 (rea/choose rea-3 rea-4)]

    ;; --- Neither blocks
    ;; run rea-1-2
    (conc/run
      [out (rea/react! rea-1-2 nil)]
      (let [_ (reset! res-1-2 out)])
      (conc/print "done-1-2"))

    ;; wait
    (Thread/sleep 10)

    ;; Check res-1-2
    (is (= :one @res-1-2))

    ;; run rea-2-1
    (conc/run
      [out (rea/react! rea-2-1 nil)]
      (let [_ (reset! res-2-1 out)])
      (conc/print "done-2-1"))

    ;; wait
    (Thread/sleep 10)

    ;; Check res-2-1
    (is (= :two @res-2-1))


    ;; --- Right blocks
    ;; run rea-1-3
    (conc/run
      [out (rea/react! rea-1-3 nil)]
      (let [_ (reset! res-1-3 out)])
      (conc/print "done-1-3"))

    ;; wait
    (Thread/sleep 10)

    ;; Check res-1-3
    (is (= :one @res-1-3))

    ;; --- Left blocks
    ;; run rea-3-1
    (conc/run
      [out (rea/react! rea-3-1 nil)]
      (let [_ (reset! res-3-1 out)])
      (conc/print "done-3-1"))

    ;; wait
    (Thread/sleep 40)

    ;; Check res-1-3
    (is (= :one @res-3-1))


    ;; --- Both block
    ;; run rea-3-1
    (conc/run
      [out (rea/react! rea-3-4 nil)]
      (let [_ (reset! res-3-4 out)])
      (conc/print "done-3-4"))

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
    (conc/run
      [out (rea/react! rea nil)]
      (let [_ (reset! res out)])
      (conc/print "done"))

    ;; wait
    (Thread/sleep 40)

    ;; Check results
    (is (= 23 @res))
    (is (= 24 @ref))
    ))

(deftest post-commit-tt
  (test-runner/run
    (m/monadic
     [ref (conc/new-ref :nein)]
     [res (rea/react! (rea/>>>
                       (rea/return 23)
                       (rea/post-commit
                        (fn [a]
                          (m/monadic
                           (conc/reset ref (inc a)))))) nil)]

     ;; check
     [r (conc/read ref)]
     (test-runner/is= 23 res)
     (test-runner/is= 24 r)
     )))


;; ----------------------
;; --- Return -----------
;; ----------------------

(deftest return-t
  (let [res (atom nil)
        rea (rea/return :bounty)]

    ;; run read reagent
    (conc/run
      [out (rea/react! rea nil)]
      (let [_ (reset! res out)])
      (conc/print "done"))

    ;; wait
    (Thread/sleep 20)

    ;; Check result
    (is (= :bounty @res))
    ))

(deftest return-tt
  (test-runner/run
    (m/monadic
     [res (rea/react! (rea/return :bounty) nil)]
     (test-runner/is= res :bounty)
     )))


;; ----------------------
;; --- Computed ---------
;; ----------------------

(deftest computed-t
  (let [res (atom nil)
        rea (rea/computed
             (fn [a]
               (rea/return (inc a))))]

    ;; run computed reagent
    (conc/run
      [out (rea/react! rea 23)]
      (let [_ (reset! res out)])
      (conc/print "done"))

    ;; wait
    (Thread/sleep 20)

    ;; Check result
    (is (= 24 @res))))

(deftest computed-tt
  (test-runner/run
    (m/monadic
     [res (rea/react!
           (rea/computed
            (fn [a]
              (rea/return (inc a)))) 23)]
     (test-runner/is= 24 res)
     )))


;; ----------------------
;; --- Lift -------------
;; ----------------------

(deftest lift-t
  (let [res (atom :nothing)
        rea (rea/lift inc)]

    ;; run lift reagent
    (conc/run
      [out (rea/react! rea 23)]
      (let [_ (reset! res out)])
      (conc/print "done"))

    ;; wait
    (Thread/sleep 10)

    ;; Check result
    (is (= 24 @res))
    ))

(deftest lift-tt
  (test-runner/run
    (m/monadic
     [res (rea/react! (rea/lift inc) 23)]
     (test-runner/is= 24 res)
     )))


;; ----------------------
;; --- First ------------
;; ----------------------

(deftest first-tt
  (test-runner/run
    (m/monadic
     [res (rea/react!
           (rea/first
            (rea/lift inc)) [42 42])]
     (test-runner/is= [43 42] res))))


;; ----------------------
;; --- Nth --------------
;; ----------------------

(deftest nth-tt
  (test-runner/run
    (m/monadic
     [res (rea/react!
           (rea/nth
            (rea/lift inc) 2) [0 0 0 0])]
     (test-runner/is= [0 0 1 0] res))))


;; ----------------------
;; --- *** --------------
;; ----------------------

(deftest ***-tt
  (test-runner/run
    (m/monadic
     [res (rea/react!
           (rea/*** (rea/lift inc)
                    (rea/lift inc)
                    (rea/lift inc)) [0 0 0])]
     (test-runner/is= [1 1 1] res))))


;; ----------------------
;; --- &&& --------------
;; ----------------------

(deftest &&&-tt
  (test-runner/run
    (m/monadic
     [res (rea/react!
           (rea/&&& (rea/lift inc)
                    (rea/lift dec)) 42)]
     (test-runner/is= [43 41] res))))
