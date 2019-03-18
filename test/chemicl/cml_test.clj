(ns chemicl.cml-test
  (:require [chemicl.cml :as cml]
            [clojure.test :refer [deftest testing is]]))

(def debug false)

(defn sync-eventually [ev & [ms]] ;; for all that require thread changes/a bit of computation.
  ;; Note: no timeout makes more stable tests, but is bad for debugging in case of dead-locks.
  (if debug
    (cml/sync ev
              ;; Note: 100ms is actually the time it needs sometimes - seems really slow?!
              (or ms 100) ::timeout)
    (cml/sync ev)))

(defn sync-now [ev] ;; for all events that should be immedately available.
  (sync-eventually ev 2))

(defn not-sync [ev & [ms]]
  (= (cml/sync ev (or ms 100) ::timeout)
     ::timeout))

(deftest send-receive-test
  (let [c (cml/channel)
        started (promise)]
    (future
      (deliver started true)
      (cml/sync (cml/send c 42)))
    @started
    (is (= (sync-eventually (cml/receive c))
           42))))

(deftest never-test
  (is (not-sync cml/never)))

(deftest always-test
  (is (= (sync-now (cml/always 42))
         42)))

(deftest timeout-test
  (is (not-sync (cml/timeout 20) 1))
  (is (nil? (cml/sync (cml/timeout 1) 10 ::timeout)))
  ;; also, timeouts start when sync is called, not when created:
  (let [ev (cml/timeout 10)]
    (Thread/sleep 15)
    (is (not-sync ev 1))))

(deftest choose-test
  (is (= (sync-now (cml/choose cml/never
                               (cml/always 42)))
         42)))

(deftest guard-test
  (let [called (atom false)
        ev (cml/guard (fn []
                        (reset! called true)
                        (cml/always 42)))]
    (is (not @called))
    (is (= (sync-now ev)
           42))
    (is @called)))

(deftest with-nack-noop-test
  (let [nack-ev (atom nil)]
    (is (= (sync-now (cml/with-nack (fn [v]
                                      (reset! nack-ev v)
                                      (cml/always 42))))
           42))
    (is (not-sync @nack-ev))))

(deftest with-nack-test
  ;; branch not taken:
  (let [nack-ev (atom nil)
        ev (cml/choose (cml/with-nack (fn [v]
                                        (reset! nack-ev v)
                                        cml/never))
                       (cml/always 42))]
    (is (nil? @nack-ev))
    (is (= (sync-now ev)
           42))
    (is (nil? (sync-eventually @nack-ev))))
  ;; branch taken:
  (let [nack-ev (atom nil)
        ev (cml/choose (cml/with-nack (fn [v]
                                        (reset! nack-ev v)
                                        (cml/always 42)))
                       cml/never)]
    (is (nil? @nack-ev))
    (is (= (sync-now ev)
           42))
    (is (not-sync @nack-ev 50))))

(deftest with-nack-repeat-test
  ;; what happens when a with-nack is used again?  (TODO: what happens in the original CML?)
  (let [nack-ev1 (atom nil)
        nack-ev2 (atom nil)
        
        run (atom nack-ev1)
        inner (atom cml/never)
        ev (cml/choose (cml/with-nack (fn [v]
                                        (reset! @run v)
                                        @inner))
                       (cml/always 42))]
    (sync-now ev)
    (is (nil? (sync-eventually @nack-ev1)))

    ;; second time that branch is chosen, so the second nack does not sync:
    (reset! run nack-ev2)
    (reset! inner (cml/always 21)) ;; <- inner could also be a channel that's now available
    (is (= (sync-now ev)
           21))
    (is (not-sync @nack-ev2 50))))

#_(deftest with-nack-nested-test
  ;; Note: not sure if this is valid cml, or makes sense ;-)
  (let [nack-ev2 (atom nil)
        ev (cml/choose (cml/with-nack (fn [v]
                                        (cml/with-nack (fn [v]
                                                         (reset! nack-ev2 v)
                                                         cml/never))))
                       (cml/always 42))]
    (is (= (sync-now ev)
           42))
    (is (nil? (sync-eventually @nack-ev2)))))

(deftest wrap-test
  (let [ev (cml/wrap (cml/always 42)
                     (fn [v]
                       (/ v 2)))]
    (is (= (sync-now ev)
           21))))

(deftest referential-equality-test
  (let [ch (cml/channel)]
    (is (= (cml/receive ch)
           (cml/receive ch)))
    (is (= (cml/send ch 1)
           (cml/send ch 1))))
  (is (= (cml/always 42) (cml/always 42)))
  (is (= (cml/timeout 10) (cml/timeout 10)))
  (is (= (cml/choose cml/never)
         (cml/choose cml/never)))
  (is (= (cml/guard +)
         (cml/guard +)))
  (is (= (cml/with-nack +)
         (cml/with-nack +)))
  (is (= (cml/wrap (cml/always 42) inc)
         (cml/wrap (cml/always 42) inc))))

(deftest performance-test
  (let [run-for-secs 20]
    (println)
    (let [start (delay (java.time.Instant/now))

          result
          (fn [start i]
            (let [ms (.until start (java.time.Instant/now) java.time.temporal.ChronoUnit/MILLIS)
                  msg_per_sec (if (zero? ms)
                                "n/a"
                                (long (/ (double i) (/ (double ms) 1000))))
                  ms_per_msg (if (zero? i)
                               "n/a"
                               (double (/ ms i)))]
              (str msg_per_sec " messages per second, resp. " ms_per_msg " ms per message. ")))
          
          print-throughput
          (fn [i]
            (try (print "\r" (result @start i))
                 (catch Exception e
                   (println "You silly:" e))))

          stop-ch (cml/channel)
          stop-ev (cml/receive stop-ch)

          ch (cml/channel)
          ;; writer thread
          writer (future
                   (loop [i 0]
                     (when-not (cml/sync (cml/choose stop-ev
                                                     (cml/send ch i)))
                       (recur (inc i)))))
          reader (future
                   (loop [i nil]
                     (let [in (cml/sync (cml/choose stop-ev
                                                    (cml/receive ch)))]
                       (if-not (= true in)
                         (do
                           (when-not (or (nil? i) (= (inc i) in))
                             (println "***** INVARIANT VIOLATION: Missed message *****"))
                           (if (zero? in) ;; first as a warmup
                             @start
                             #_(when (zero? (mod (dec in) 1000))
                               (print-throughput in)))                         
                           (recur in))
                         i))))]
      (cml/sync (cml/timeout (* 1000 run-for-secs)))
      (cml/sync (cml/send stop-ch true))
      (cml/sync (cml/send stop-ch true))
      @writer
      (let [total @reader]
        (println "\r" (result @start total))))))
