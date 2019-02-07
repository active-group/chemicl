(ns chemicl.concurrency-test-runner
  (:require [chemicl.concurrency :as conc]
            [active.clojure.monad :as m]
            [active.clojure.record :as acr]
            [active.clojure.lens :as lens]))

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

(acr/define-record-type AssertCommand
  (make-assert-command pred s)
  assert-command?
  [pred assert-command-predicate
   s assert-command-string])

(def assert make-assert-command)

;; --- Internal ---------

(acr/define-record-type ThreadState
  (mk-thread-state m counter annotation)
  thread-state?
  [(m thread-state-m thread-state-m-O)
   (counter thread-state-counter thread-state-counter-O)
   (annotation thread-state-annotation thread-state-annotation-O)])

(defn make-thread-state
  ([m]
   (mk-thread-state m 1 :unmarked))
  ([m ann]
   (mk-thread-state m 1 ann)))

(defn set-thread-state-m [ts m]
  (lens/shove ts thread-state-m-O m))

(defn inc-thread-state-counter [ts]
  (lens/overhaul ts thread-state-counter-O inc))

(defn dec-thread-state-counter [ts]
  (lens/overhaul ts thread-state-counter-O dec))

(defn mark-thread-state [ts]
  (lens/shove ts thread-state-annotation-O :marked))

(defn unmark-thread-state [ts]
  (lens/shove ts thread-state-annotation-O :unmarked))

(defn marked? [ts]
  (= :marked (thread-state-annotation ts)))

(defn new-thread-id [log]
  (hash log))

(defn enact [tid m log threads]
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

        (assert-command? m1)
        (do
          (when-not (assert-command-predicate m1)
            (println "Assertion failed")
            (println (pr-str tid))
            (println (assert-command-string m1))
            (clojure.core/assert false "An assertion failed"))
          [:continue
           (update threads tid set-thread-state-m (c tid))
           nil])

        (conc/get-current-task-command? m1)
        [:continue
         (update threads tid set-thread-state-m (c tid))
         nil]

        (conc/print-command? m1)
        (do
          (println (conc/print-command-line m1))
          [:continue
           (update threads tid set-thread-state-m (c nil))
           nil])

        (conc/new-ref-command? m1)
        (let [a (atom (conc/new-ref-command-init m1))]
          [:continue
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
        [:continue
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

    (assert-command? m)
    (do
      (when-not (assert-command-predicate m)
        (println "Assertion failed")
        (println (pr-str tid))
        (println (assert-command-string m))
        (clojure.core/assert false "An assertion failed"))
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
    ))

(defn filter-values [pred m]
  (filter (comp pred second) m))

(defn active? [ts]
  (> (thread-state-counter ts) 0))

(defn leaves-trace-period? [ts]
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

(defn inside-trace-period? [threads]
  (every? marked? (vals threads)))

(defn glue [prefix threads]
  (set (map (fn [tid]
              (concat prefix [tid]))
            (keys threads))))

(defn prefixes [prefix threads]
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

(defn run-with-trace-prefix
  "Consumes prefix, may produce a set of new prefixes"
  [prefix m]
  (loop [threads {:init (make-thread-state m)}
         pre prefix
         log []]
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
           (conj log next-tid))

          :done
          (if (= next-tid :init)
            ;; initial thread is done
            ;; we can omit the others and return
            [:done return-val]
            ;; else continue with another thread
            (recur
             new-threads
             (rest pre)
             (conj log next-tid)))))
      ;; else done
      [:new-prefixes
       (prefixes prefix threads)]
      )))

(defn run-with-reducer [m reduce-fn acc]
  (loop [prefixes #{[]}
         acc acc]
    #_(println "-- " (pr-str (count prefixes)))
    (if-let [prefix (first prefixes)]
      (let [[code arg] (run-with-trace-prefix prefix m)]
        (case code
          :new-prefixes
          (recur (clojure.set/union (set (rest prefixes)) arg)
                 acc)

          :done
          (recur (set (rest prefixes))
                 (reduce-fn acc arg))))
      acc)))

(defn run [m]
  (loop [prefixes #{[]}]
    (when-let [prefix (first prefixes)]
      (let [[code arg] (run-with-trace-prefix prefix m)]
        (case code
          :new-prefixes
          (recur (clojure.set/union (set (rest prefixes)) arg))

          :done
          (recur (set (rest prefixes)))))
      )))

#_(run-with-reducer
 (m/monadic
  (conc/print "---")
  (conc/fork
   (m/monadic
    (conc/print "a")
    (make-mark-command)
    (conc/print "b")
    (conc/print "c")
    (conc/print "c")
    (make-unmark-command)
    (conc/print "d")
    ))
  (m/monadic
   (conc/print "1")
   (make-mark-command)
   (conc/print "2") (conc/print "3")
   (conc/print "3")
   (conc/print "3")
   (make-unmark-command)
   (conc/print "4")
   )

  (m/return 1))
 #(+ %1 %2) 0)

#_(run-with-reducer
 (m/monadic
  (conc/print "---")
  (make-mark-command)
  [r (conc/new-ref 1)]

  [child (conc/fork
          (m/monadic
           (conc/park)
           (conc/cas r 23 42)
           (conc/print "foo")
           ))]

  (conc/cas r 1 23)
  (conc/print "bar")
  (conc/unpark child nil)

  (make-unmark-command)

  [n (conc/read r)]
  (conc/print "n: " (pr-str n))
  (m/return (= n 42)))
 #(and %1 %2) true)

#_(run-with-reducer
 (m/monadic
  (conc/print "---")
  (make-mark-command)
  [tid (conc/get-current-task)]
  (conc/fork
   (m/monadic
    (conc/print "child")
    (conc/unpark tid nil)))

  (conc/park)
  (conc/print "parent")
  (m/return 1))
 #(+ %1 %2) 0)
