(ns chemicl.concurrency-test
  (:require
   [chemicl.concurrency :as conc]
   [active.clojure.monad :as m]
   [chemicl.monad :as cm :refer [defmonadic whenm effect!]]
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

(defmacro mdo [& forms]
  `(m/monadic
    [~'_ (m/return nil)]
    ~@forms))

(deftest fork-performance-t
  (let [n 100000
        result (promise)]
    (letfn [(mate [i p]
              (mdo
               (if (> i n)
                 (conc/unpark p nil)
                 (conc/fork
                  (mate (inc i) p))
                 )))]

      ;; fork many mates
      (conc/run
        [me (conc/get-current-task)]

        ;; start time
        [start (effect! (java.time.Instant/now))]

        ;; run mate
        (conc/fork
         (mate 0 me))

        ;; wait for mates
        (conc/park)

        ;; end time
        [end (effect! (java.time.Instant/now))]

        (let [millis (.until start end java.time.temporal.ChronoUnit/MILLIS)])
        (effect! (deliver result millis))
        ))
    (println "Spawning" n "threads took" @result "ms")
    (is (< @result 200))
    ))

(deftest cas-performance-t
  (let [n 1000
        result (promise)]
    (letfn [(mate [i p r]
              (m/monadic
               (if (> i n)
                 (conc/unpark p nil)
                 (m/monadic
                  [succ (conc/cas r i (inc i))]
                  (mate (inc i) p r))
                 )))]

      ;; fork many mates
      (conc/run
        [me (conc/get-current-task)]
        [r (conc/new-ref 0)]

        ;; start time
        [start (effect! (java.time.Instant/now))]

        ;; run mate
        (conc/fork
         (mate 0 me r))

        ;; wait for mates
        (conc/park)

        ;; end time
        [end (effect! (java.time.Instant/now))]

        (let [millis (.until start end java.time.temporal.ChronoUnit/MILLIS)])
        (effect! (deliver result millis))
        ))
    (println "Cassing" n "times took" @result "ms")
    (is true)
    ))
