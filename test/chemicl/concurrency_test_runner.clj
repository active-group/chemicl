(ns chemicl.concurrency-test-runner
  (:require [chemicl.concurrency :as conc]
            [active.clojure.monad :as m]))

(defn new-thread-id [log]
  (hash log))

(defn update-m [threads tid m]
  (update threads tid (fn [[n _]]
                        [n m])))

(defn enact-3 [tid m log threads]
  (cond
    (m/free-bind? m)
    (let [m1 (m/free-bind-monad m)
          c (m/free-bind-cont m)]
      (cond
        (m/free-return? m1)
        [:continue
         (update-m threads tid
                   (c (m/free-return-val m1)))
         nil]

        (conc/get-current-task-command? m1)
        [:continue
         (update-m
          threads
          tid
          (c tid))
         nil]

        (conc/print-command? m1)
        (do
          (println (conc/print-command-line m1))
          [:continue
           (update-m threads tid (c nil))
           nil])

        (conc/new-ref-command? m1)
        (let [a (atom (conc/new-ref-command-init m1))]
          [:continue
           (update-m threads tid (c a))
           nil])

        (conc/cas-command? m1)
        (let [succ 
              (compare-and-set! (conc/cas-command-ref m1)
                                (conc/cas-command-old-value m1)
                                (conc/cas-command-new-value m1))]
          [:continue
           (update-m threads tid (c succ))
           nil])

        (conc/read-command? m1)
        [:continue
         (update-m
          threads
          tid
          (c (deref (conc/read-command-ref m1))))
         nil]

        (conc/reset-command? m1)
        (let [res (reset! (conc/reset-command-ref m1)
                          (conc/reset-command-new-value m1))]
          [:continue
           (update-m threads tid (c res))
           nil])

        (conc/park-command? m1)
        (do
          [:continue
           (update threads tid (fn [[n _]]
                                 [(dec n) (c nil)]))
           nil])

        (conc/unpark-command? m1)
        (let [otid (conc/unpark-command-task m1)
              v (conc/unpark-command-value m1)]
          [:continue
           (-> threads
               (update-m tid (c nil))
               (update otid (fn [[n m]]
                              [(inc n) m])))
           nil])

        (conc/fork-command? m1)
        (let [forked-m (conc/fork-command-monad m1)
              forked-tid (new-thread-id log)]
          [:continue
           (-> threads
               (update-m tid (c forked-tid))
               (assoc forked-tid [1 forked-m]))
           nil])

        (conc/timeout-command? m1)
        [:continue
         (update-m threads tid (c nil))
         nil]
        ))

    (m/free-return? m)
    [:done
     (dissoc threads tid)
     (m/free-return-val m)]

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
      [:done
       (-> threads
           (dissoc tid)
           (update otid (fn [[n m]]
                          [(inc n) m])))
       nil])

    (conc/fork-command? m)
    (let [forked-m (conc/fork-command-monad m)
          forked-tid (new-thread-id log)]
      [:done
       (-> threads
           (dissoc tid)
           (assoc forked-tid [1 forked-m]))
       forked-tid])

    (conc/timeout-command? m)
    [:done
     (dissoc threads tid)
     nil]
    ))

(defn active? [[n _]]
  (> n 0))

(defn active-threads [threads]
  (filter (comp active? second) threads))

(defn prefixes [prefix threads]
  (map (fn [tid]
         (concat prefix [tid]))
       (keys (active-threads threads))))

(defn run-with-trace-prefix
  "Consumes prefix, may produce a set of new prefixes"
  [prefix m]
  (loop [threads {:init [1 m]}
         pre prefix
         log []]
    (if-let [next-tid (first pre)]
      ;; run
      (let [[_state m]
            (get threads next-tid)

            [code new-threads return-val]
            (enact-3 next-tid m log threads)]

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
  (loop [prefixes [[]]
         acc acc]
    (if-let [prefix (first prefixes)]
      (let [[code arg] (run-with-trace-prefix prefix m)]
        (case code
          :new-prefixes
          (recur (concat (rest prefixes) arg)
                 acc)

          :done
          (recur (rest prefixes)
                 (reduce-fn acc arg))))
      acc)))

(run-with-reducer
 (m/monadic
  (conc/print "---")
  (conc/fork
   (m/monadic
    (conc/print "a")
    (conc/print "b")
    (conc/print "c")
    ))
  (m/monadic
   (conc/print "1")
   (conc/print "2")
   (conc/print "3")
   )

  (m/return true))
 #(and %1 %2) true)

(run-with-reducer
 (m/monadic
  (conc/print "---")
  [r (conc/new-ref 1)]

  [child (conc/fork
          (m/monadic
           (conc/park)
           (conc/cas r 23 42)
           ))]

  (conc/cas r 1 23)
  (conc/unpark child nil)

  [n (conc/read r)]
  (m/return (or (= n 23)
                (= n 42))))
 #(and %1 %2) true)

(run-with-reducer
 (m/monadic
  [tid (conc/get-current-task)]
  (conc/fork
   (m/monadic
    (conc/print "child")
    (conc/unpark tid nil)
    (conc/unpark tid nil)))

  (conc/park)
  (conc/park)
  (conc/print "parent"))
 #(and %1 %2) true)
