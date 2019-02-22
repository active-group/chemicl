(ns chemicl.concurrency-test-runner
  (:require [chemicl.concurrency :as conc]
            [active.clojure.monad :as m]
            [active.clojure.record :as acr]
            [active.clojure.lens :as lens]
            [clojure.test :as test]))

(declare make-is-equal-command)
(declare run-)

;; --- Testing API ---------

(defn is=
  "(is= l r) is like (clojure.test/is (= l r))"
  [x a & [msg]]
  (make-is-equal-command x a msg))

(defmacro is
  "Similar to clojure.test/is"
  [c & args]
  `(is= true ~c ~@args))

(defn run
  "Run a concurrent program with the exhaustive test runner.
  Iterates over all scheduling patterns."
  [m]
  (run- m))


(defmacro with-mark
  [& ms]
  `(m/monadic
    (mark)
    [res# (m/monadic ~@ms)]
    (unmark)
    (m/return res#)))


;; --- Special commands for testing ---------

(acr/define-record-type MarkCommand
  (make-mark-command)
  mark-command?
  [])

(def mark make-mark-command)

(acr/define-record-type UnmarkCommand
  (make-unmark-command)
  unmark-command?
  [])

(def unmark make-unmark-command)

(acr/define-record-type IsEqualCommand
  (make-is-equal-command expected actual msg)
  is-equal-command?
  [expected is-equal-command-expected
   actual is-equal-command-actual
   msg is-equal-command-message])


;; --- Internal ---------

(acr/define-record-type ThreadState
  (mk-thread-state m counter annotation)
  thread-state?
  [(m thread-state-m thread-state-m-O)
   (counter thread-state-counter thread-state-counter-O)
   (annotation thread-state-annotation thread-state-annotation-O)])

(defn- make-thread-state
  ([m]
   (mk-thread-state m 1 :unmarked))
  ([m ann]
   (mk-thread-state m 1 ann)))

(defn- set-thread-state-m [ts m]
  (lens/shove ts thread-state-m-O m))

(defn- inc-thread-state-counter [ts]
  (lens/overhaul ts thread-state-counter-O inc))

(defn- dec-thread-state-counter [ts]
  (lens/overhaul ts thread-state-counter-O dec))

(defn- mark-thread-state [ts]
  (lens/shove ts thread-state-annotation-O :marked))

(defn- unmark-thread-state [ts]
  (lens/shove ts thread-state-annotation-O :unmarked))

(defn- marked? [ts]
  (= :marked (thread-state-annotation ts)))

(defn- new-thread-id [log]
  (hash log))

(defn- enact [tid m log threads]
  (loop [m m]
    (cond
      (m/free-bind? m)
      (let [m1 (m/free-bind-monad m)
            c (m/free-bind-cont m)]
        (cond
          (m/free-return? m1)
          [:continue
           (update threads tid set-thread-state-m (c (m/free-return-val m1)))
           nil]

          (mark-command? m1)
          [:continue
           (update threads tid (fn [ts]
                                 (-> ts
                                     (mark-thread-state)
                                     (set-thread-state-m (c nil)))))
           nil]

          (unmark-command? m1)
          [:continue
           (update threads tid (fn [ts]
                                 (-> ts
                                     (unmark-thread-state)
                                     (set-thread-state-m (c nil)))))
           nil]

          (is-equal-command? m1)
          (let [expected (is-equal-command-expected m1)
                actual (is-equal-command-actual m1)
                msg (is-equal-command-message m1)]
            (test/do-report {:type (if (= expected actual) :pass :fail)
                             :message msg
                             :expected expected
                             :actual actual})
            (recur (c nil))
            #_[:continue
               (update threads tid set-thread-state-m (c tid))
               nil])

          (conc/get-current-task-command? m1)
          (recur (c tid))
          #_[:continue
             (update threads tid set-thread-state-m (c tid))
             nil]

          (conc/print-command? m1)
          (do
            (println (conc/print-command-line m1))
            (recur (c nil))
            #_[:continue
               (update threads tid set-thread-state-m (c nil))
               nil])

          (conc/new-ref-command? m1)
          (let [a (atom (conc/new-ref-command-init m1))]
            (recur (c a))
            #_[:continue
             (update threads tid set-thread-state-m (c a))
             nil])

          (conc/cas-command? m1)
          (do
            (let [succ 
                  (compare-and-set! (conc/cas-command-ref m1)
                                    (conc/cas-command-old-value m1)
                                    (conc/cas-command-new-value m1))]
              [:continue
               (update threads tid set-thread-state-m (c succ))
               nil]))

          (conc/read-command? m1)
          (do
            [:continue
             (update
              threads
              tid
              set-thread-state-m
              (c (deref (conc/read-command-ref m1))))
             nil])

          (conc/reset-command? m1)
          (do
            (let [res (reset! (conc/reset-command-ref m1)
                              (conc/reset-command-new-value m1))]
              [:continue
               (update threads tid set-thread-state-m (c res))
               nil]))

          (conc/park-command? m1)
          (do
            #_(println "PARKING")
            #_(println (pr-str threads))
            #_(println (pr-str (update threads tid dec-thread-state-counter)))
            [:continue
             (update threads tid (fn [ts]
                                   (-> ts
                                       (dec-thread-state-counter)
                                       (set-thread-state-m (c nil)))))
             nil])

          (conc/unpark-command? m1)
          (let [otid (conc/unpark-command-task m1)
                v (conc/unpark-command-value m1)]
            #_(println "UNPARKING")
            #_(println (pr-str threads))
            #_(println (pr-str (-> threads
                                   (update tid set-thread-state-m (c nil))
                                   (update otid inc-thread-state-counter))))
            [:continue
             (-> threads
                 (update tid set-thread-state-m (c nil))
                 (update otid inc-thread-state-counter))
             nil])

          (conc/fork-command? m1)
          (let [forked-m (conc/fork-command-monad m1)
                forked-tid (new-thread-id log)
                ann (thread-state-annotation
                     (get threads tid))]
            [:continue
             (-> threads
                 (update tid set-thread-state-m (c forked-tid))
                 (assoc forked-tid (make-thread-state forked-m ann)))
             nil])

          (conc/timeout-command? m1)
          (recur (c nil))
          #_[:continue
             (update threads tid set-thread-state-m (c nil))
             nil]
          ))

      (m/free-return? m)
      [:done
       (dissoc threads tid)
       (m/free-return-val m)]

      (mark-command? m)
      [:done
       (dissoc threads tid)
       nil]

      (unmark-command? m)
      [:done
       (dissoc threads tid)
       nil]

      (is-equal-command? m)
      (let [expected (is-equal-command-expected m)
            actual (is-equal-command-actual m)
            msg (is-equal-command-message m)]
        (test/do-report {:type (if (= expected actual) :pass :fail)
                         :message msg
                         :expected expected
                         :actual actual})
        [:done
         (dissoc threads tid)
         nil])

      (conc/print-command? m)
      (do
        (println (conc/print-command-line m))
        [:done
         (dissoc threads tid)
         nil])

      (conc/new-ref-command? m)
      (let [a (atom (conc/new-ref-command-init m))]
        [:done
         (dissoc threads tid)
         a])

      (conc/cas-command? m)
      (let [succ 
            (compare-and-set! (conc/cas-command-ref m)
                              (conc/cas-command-old-value m)
                              (conc/cas-command-new-value m))]
        [:done
         (dissoc threads tid)
         succ])

      (conc/read-command? m)
      [:done
       (dissoc threads tid)
       (deref (conc/read-command-ref m))]

      (conc/reset-command? m)
      (let [res (reset! (conc/reset-command-ref m)
                        (conc/reset-command-new-value m))]
        [:done
         (dissoc threads tid)
         res])

      (conc/park-command? m)
      [:done
       (dissoc threads tid)
       nil]

      (conc/unpark-command? m)
      (let [otid (conc/unpark-command-task m)
            v (conc/unpark-command-value m)]
        #_(println "UNPARKING")
        #_(println (pr-str threads))
        #_(println (pr-str (-> threads
                               (dissoc tid)
                               (update otid inc-thread-state-counter))))
        [:done
         (-> threads
             (dissoc tid)
             (update otid inc-thread-state-counter))
         nil])

      (conc/fork-command? m)
      (let [forked-m (conc/fork-command-monad m)
            forked-tid (new-thread-id log)
            ann (thread-state-annotation
                 (get threads tid))]
        [:done
         (-> threads
             (dissoc tid)
             (assoc forked-tid (make-thread-state forked-m ann)))
         forked-tid])

      (conc/timeout-command? m)
      [:done
       (dissoc threads tid)
       nil]
      )))


;; --- Threads ---------

(defn- filter-values [pred m]
  (filter (comp pred second) m))

;; predicates on thread states

(defn- active? [ts]
  (> (thread-state-counter ts) 0))

(defn- leaves-trace-period? [ts]
  (let [m (thread-state-m ts)]
    (cond
      (m/free-bind? m)
      (let [m1 (m/free-bind-monad m)]
        (unmark-command? m1))

      (unmark-command? m)
      true

      :else
      false
      )))

(defn- stays-inside-trace-period? [ts]
  (not (leaves-trace-period? ts)))

;; thread maps

(defn- deadlocked? [threads]
  (let [active (filter-values active? threads)]
    (and (> (count threads) 0)
         (= (count active) 0))))

(defn- inside-trace-period? [threads]
  (every? marked? (vals threads)))

(defn- schedulable-threads [threads]
  (let [active-threads (filter-values active? threads)
        active+staying-traced-threads (filter-values
                                       stays-inside-trace-period?
                                       active-threads)]
    (if (and (inside-trace-period? threads)
             (not-empty active+staying-traced-threads))
      active+staying-traced-threads
      active-threads)))

(test/deftest schedulable-threads-t
  (let [
        ;; active, outside trace period
        t1 (mk-thread-state
            (m/monadic
             (conc/print "eins")
             (conc/print "zwei"))
            1
            :unmarked)

        ;; active, inside trace period, stays inside
        t2 (mk-thread-state
            (m/monadic
             (conc/print "eins")
             (conc/print "zwei"))
            1
            :marked)

        ;; active, inside trace period, leaves trace period
        t3 (mk-thread-state
            (m/monadic
             (unmark)
             (conc/print "zwei"))
            1
            :marked)

        ;; inactive, outside trace period
        t4 (mk-thread-state
            (m/monadic
             (conc/print "eins")
             (conc/print "zwei"))
            0
            :unmarked)

        ;; inactive, inside trace period, stays inside
        t5 (mk-thread-state
            (m/monadic
             (conc/print "eins")
             (conc/print "zwei"))
            0
            :marked)

        ;; inactive, inside trace period, leaves trace period
        t6 (mk-thread-state
            (m/monadic
             (unmark)
             (conc/print "zwei"))
            0
            :marked)

        ;; untraced, both active
        sts1 (schedulable-threads {1 t1 11 t1})

        ;; untraced, both active
        sts2 (schedulable-threads {1 t1 3 t2})

        ;; untraced, 1 active
        sts3 (schedulable-threads {1 t1 4 t4})

        ;; traced, 3 wants to leave -> only 1
        sts4 (schedulable-threads {2 t2 3 t3})

        ;; traced, both staying
        sts5 (schedulable-threads {2 t2 22 t2})
        ]
    (test/is (= [1 11] (map first sts1)))
    (test/is (= [1 3] (map first sts2)))
    (test/is (= [1] (map first sts3)))
    (test/is (= [2] (map first sts4)))
    (test/is (= [2 22] (map first sts5)))
    ))


;; --- Breadth-first search test runner ---------

(defn- glue [prefix threads]
  (set (map (fn [tid]
              (concat prefix [tid]))
            (keys threads))))

(defn- prefixes [prefix threads m-log]
  (when (deadlocked? threads)
    (test/do-report {:type :fail
                     :message (str "Found a deadlock!\n\n"
                                   "M-log:\n"
                                   (clojure.string/join "\n" (map pr-str prefix m-log))
                                   "Threads:\n"
                                   (with-out-str (clojure.pprint/pprint threads)))
                     :expected :no-deadlock
                     :actual :deadlock
                     }))
  
  (let [active-threads (filter-values active? threads)
        active+staying-traced-threads (filter-values
                                       (complement leaves-trace-period?)
                                       active-threads)

        pres (glue prefix active-threads)]
    (if (inside-trace-period? threads)
      ;; inside trace period: return every combination
      (let [tier-1-prefixes (glue prefix active+staying-traced-threads)]
        (if-not (empty? tier-1-prefixes)
          tier-1-prefixes
          ;; else every thread wants to leave the trace period
          ;; transition to deterministic mode
          (set [(first pres)])
          ))
      ;; outside trace period: choose only one prefix, deterministically
      (set [(first pres)]))))

(defn- run-with-trace-prefix
  "Consumes prefix, may produce a set of new prefixes"
  [prefix m]
  (loop [threads {:init (make-thread-state m)}
         pre prefix
         log []
         m-log []]
    (if-let [next-tid (first pre)]
      ;; run
      (let [ts (get threads next-tid)

            [code new-threads return-val]
            (enact next-tid (thread-state-m ts) log threads)]

        (case code
          :continue
          (recur
           new-threads
           (rest pre)
           (conj log next-tid)
           (conj m-log (thread-state-m ts)))

          :done
          (if (= next-tid :init)
            ;; initial thread is done
            ;; we can omit the others and return
            [:done return-val]
            ;; else continue with another thread
            (recur
             new-threads
             (rest pre)
             (conj log next-tid)
             (conj m-log (thread-state-m ts))))))
      ;; else done
      [:new-prefixes
       (prefixes prefix threads m-log)]
      )))

(defn- run- [m]
  (loop [prefixes #{[]}
         ndone 0]
    (when-let [prefix (first prefixes)]
      (print "\033[1A")
      (print "\033[1A")
      (print "\033[2K\r")
      (print "nprefixes:" (pr-str (count prefixes)))
      (print "\n")
      (print "prefixlength:" (pr-str (count prefix)))
      (print "\n")
      (print "ndone:" (pr-str ndone))
      (flush)
      (let [[code arg] (run-with-trace-prefix prefix m)]
        (case code
          :new-prefixes
          (recur (clojure.set/union (set (rest prefixes)) arg) ndone)

          :done
          (recur (set (rest prefixes)) (inc ndone))))
      )))


;; --- Random depth-first-search test runner ---------

(defn random-thread [threads]
  (rand-nth (schedulable-threads threads)))

(defn run-randomized [m]
  (loop [threads {:init (make-thread-state m)}
         log []]

    (when (deadlocked? threads)
      (test/do-report {:type :fail
                       :message (str "Found a deadlock!\n\n"
                                     "Threads:\n"
                                     (with-out-str (clojure.pprint/pprint threads)))
                       :expected :no-deadlock
                       :actual :deadlock
                       }))

    (if-let [[tid ts] (random-thread threads)]
      ;; run
      (let [[code new-threads return-val]
            (enact tid (thread-state-m ts) log threads)]
        (case code
          :continue ;; this thread is not yet finished
          (recur new-threads (conj log tid))

          :done ;; this thread is finished
          (if (= tid :init)
            ;; initial thread is done
            ;; we can omit the others and return
            log
            ;; else continue with another thread
            (recur new-threads (conj log tid)))))

      ;; else done
      log
      )))

(defn run-randomized-n [n m]
  (let [logs (atom #{})]
    (doall 
     (pmap (fn [m]
             (let [log (run-randomized m)]
               (swap! logs conj log)))
           (repeat n m)))
    (println "n unique runs:" (pr-str (count @logs)))
    ))

#_(test/deftest run-randomized-n-t
  (let [failing-m (m/monadic
                   [r (conc/new-ref :nothing)]

                   (conc/fork
                    (m/monadic
                     (mark)
                     (conc/reset r :one)
                     (unmark)))

                   (mark)
                   (conc/reset r :two)
                   (unmark)

                   [v (conc/read r)]
                   (is= v :two))]
    (run-randomized-n 10 failing-m)
    ))
