(ns chemicl.concurrency
  (:require
   [chemicl.exec :as x]
   [active.clojure.record :as acr]
   [active.clojure.lens :as lens]
   [active.clojure.monad :as m]))


;; --- Task locks ----------

(acr/define-record-type TaskLock
  (make-task-lock s)
  task-lock?
  [s task-lock-state-ref])

(defn new-task-lock! []
  (make-task-lock (atom nil)))

(defn block-on-task-lock!
  "Block on lock until a permit is available.
  Returns a monad by applying cont to a value delivered by the signaller."
  [lock cont]
  (let [ref (task-lock-state-ref lock)]
    (loop []
      (let [{:keys [permit? continuation value] :as state} @ref]
        (if permit?
          ;; Lock has been unlocked, we are allowed to proceed by consuming the permit
          (if (compare-and-set!
               ref 
               state
               (assoc state :permit? false))
            (cont value) ;; deliver value
            (recur))
          ;; No permit -> enter cont as continuation
          (if (compare-and-set!
               ref 
               state
               (assoc state :continuation cont))
            nil
            (recur)))))))

(defn signal-on-task-lock!
  "Signal and deliver a value. Returns a monad value."
  [lock v]
  (let [ref (task-lock-state-ref lock)]
    (loop []
      (let [{:keys [permit? continuation value] :as state} @ref]
        (if continuation
          ;; blocked continuation found -> remove it and return it
          (if (compare-and-set! ref state {:permit? false
                                           :continuation nil
                                           :value nil})
            (continuation v)
            (recur))
          ;; no blocked continuation found -> leave a permit
          (if (compare-and-set! ref state {:permit? true
                                           :continuation nil
                                           :value v})
            nil
            (recur)))))))


;; --- Task abstraction ---------

(acr/define-record-type Task
  (make-task tl bo)
  task?
  [(tl task-lock task-lock-lens)
   (bo task-backoff task-backoff-lens)])

(defn new-task! []
  (make-task (new-task-lock!)
             0))

(defn signal-task! [t v]
  (signal-on-task-lock!
   (task-lock t) v))

(defn block-task! [t cont]
  (block-on-task-lock!
   (task-lock t)
   cont))

(defn set-backoff [t v]
  (lens/shove t task-backoff-lens v))







;; --- Cont concurrency monad: commands ---------

(acr/define-record-type CASCommand
  (make-cas-command ref ov nv)
  cas-command?
  [ref cas-command-ref
   ov cas-command-old-value
   nv cas-command-new-value])

(defn cas [ref ov nv] (make-cas-command ref ov nv))

(acr/define-record-type ReadCommand
  (make-read-command ref)
  read-command?
  [ref read-command-ref])

(defn read [ref] (make-read-command ref))

(acr/define-record-type NewRefCommand
  (make-new-ref-command init)
  new-ref-command?
  [init new-ref-command-init])

(defn new-ref [init] (make-new-ref-command init))

(acr/define-record-type ParkCommand
  (make-park-command)
  park-command?
  [])

(defn park [] (make-park-command))

(acr/define-record-type UnparkCommand
  (make-unpark-command task value)
  unpark-command?
  [task unpark-command-task
   value unpark-command-value])

(defn unpark [task value] (make-unpark-command task value))

(acr/define-record-type ForkCommand
  (make-fork-command m)
  fork-command?
  [m fork-command-monad])

(defn fork [m] (make-fork-command m))

(acr/define-record-type GetCurrentTaskCommand
  (make-get-current-task-command)
  get-current-task-command?
  [])

(defn get-current-task [] (make-get-current-task-command))

(acr/define-record-type ExitCommand
  (make-exit-command)
  exit-command?
  [])

(defn exit [] (make-exit-command))

;; Backoff

(acr/define-record-type TimeoutCommand
  (make-timeout-command msec)
  timeout-command?
  [msec timeout-command-msec])

(defn timeout [msec] (make-timeout-command msec))

(acr/define-record-type ResetBackoffCommand
  (make-reset-backoff-command)
  reset-backoff-command?
  [])

(defn reset-backoff [] (make-reset-backoff-command))

(acr/define-record-type BackoffOnceCommand
  (make-backoff-once-command)
  backoff-once-command?
  [])

(defn backoff-once [] (make-backoff-once-command))

;; Debugging only

(acr/define-record-type PrintCommand
  (make-print-command l)
  print-command?
  [l print-command-line])

(defn print [& s] (make-print-command (str s)))


;; --- Cont concurrency monad: status return values ---------

(acr/define-record-type ParkStatus
  (make-park-status c)
  park-status?
  [c park-status-continuation])

(acr/define-record-type UnparkStatus
  (make-unpark-status c unpark-task value)
  unpark-status?
  [c unpark-status-continuation
   unpark-task unpark-status-unpark-task
   value unpark-status-value])

(acr/define-record-type BackoffStatus
  (make-backoff-status c counter)
  backoff-status?
  [c backoff-status-continuation
   counter backoff-status-counter])

(acr/define-record-type TimeoutStatus
  (make-timeout-status c timeout)
  timeout-status?
  [c timeout-status-continuation
   timeout timeout-status-timeout])

(acr/define-record-type ForkStatus
  (make-fork-status c m)
  fork-status?
  [c fork-status-continuation
   m fork-status-monad])

(acr/define-record-type ExitStatus
  (make-exit-status)
  exit-status?
  [])


;; --- (Inner) monad runner ---------

(defn run-cont [m task]
  (loop [m m
         task task]
    (cond

      (m/free-bind? m)
      (let [m1 (m/free-bind-monad m)
            c (m/free-bind-cont m)]
        (cond
          (m/free-return? m1)
          (recur (c (m/free-return-val m1)) task)

          ;; Read & CAS

          (new-ref-command? m1)
          (do (println "got new ref command")
              (recur (c (atom (new-ref-command-init m1))) task))

          (cas-command? m1)
          (do
            (println "--- CAS COMMAND: " (pr-str m1))
            (let [succ 
                  (compare-and-set! (cas-command-ref m1)
                                    (cas-command-old-value m1)
                                    (cas-command-new-value m1))]
              (recur (c succ) task)))

          (read-command? m1)
          (recur
           (c (deref (read-command-ref m1)))
           task)

          ;; Concurrency control

          (park-command? m1)
          (make-park-status c)

          (unpark-command? m1)
          (make-unpark-status
           c (unpark-command-task m1)
           (unpark-command-value m1))

          (fork-command? m1)
          (make-fork-status c (fork-command-monad m1))

          (get-current-task-command? m1)
          (recur (c task) task)

          (exit-command? m1)
          (make-exit-status)

          ;; Backoff

          (timeout-command? m1)
          (make-timeout-status
           c (timeout-command-msec m1))

          (reset-backoff-command? m1)
          (recur (c nil) (set-backoff task 0))

          (backoff-once-command? m1)
          (make-backoff-status c (task-backoff task))

          ;; Debugging only

          (print-command? m1)
          (do
            (println (print-command-line m1))
            (recur (c nil) task))))

      (m/free-return? m)
      (make-exit-status)

      (new-ref-command? m)
      (make-exit-status)

      (cas-command? m)
      (compare-and-set! (cas-command-ref m)
                        (cas-command-old-value m)
                        (cas-command-new-value m))

      (read-command? m)
      (make-exit-status)

      (park-command? m)
      (make-exit-status)

      (unpark-command? m)
      ;; beware: continuation is nil, which needs to be handled in outer runner
      (make-unpark-status
       nil
       (unpark-command-task m)
       (unpark-command-value m))

      (fork-command? m)
      ;; Run the forked task on the same thread but with a different task lock
      (recur (fork-command-monad m) (new-task!))

      (get-current-task-command? m)
      ;; what do you want to do with it mate?
      (make-exit-status)

      (exit-command? m)
      (make-exit-status)

      (print-command? m)
      (do
        (println (print-command-line m))
        (make-exit-status))
      )))


;; --- Spin and Backoff ---------

(defn spin-until! [pred! counter k]
  (if (pred!)
    (k)
    (x/run-after (* (max counter 14) 200) ;; todo: exponential
                 (fn []
                   (spin-until! pred! (inc counter) k)
                   ))))

(defn backoff-once! [counter k]
  (x/run-after (* (max counter 14) 200)
               (fn [] (k))))


;; --- Outside runner ---------

(declare run-many-to-many)
(declare run-many-to-many-after)

(defn run-mn [m task runner]
  (runner
    (fn []
      (let [res (run-cont m task)]
        (cond

          ;; PARKING

          (park-status? res)
          (when-let [mm (block-task!
                         task
                         (park-status-continuation res))]
            ;; is this going to overflow the stack??
            (run-many-to-many mm task))

          (unpark-status? res)
          (let [cont (unpark-status-continuation res)
                utask (unpark-status-unpark-task res)
                uvalue (unpark-status-value res)]
            ;; Unpark the parked task
            (when-let [mm (signal-task! utask uvalue)]
              (run-many-to-many mm utask))

            ;; Continue the unparking task
            (when cont
              (run-many-to-many (cont true) task)))


          ;; FORKING

          (fork-status? res)
          (let [cont (fork-status-continuation res)
                fm (fork-status-monad res)
                new-task (new-task!)]
            ;; Run the forked task
            (run-many-to-many fm new-task)

            ;; Continue the parent task
            (run-many-to-many (cont new-task) task))


          ;; BACKOFF

          (timeout-status? res)
          (let [cont (timeout-status-continuation res)
                msec (timeout-status-timeout res)]
            (run-many-to-many-after (cont nil) task msec))

          (backoff-status? res)
          (let [counter (backoff-status-counter res)
                cont (backoff-status-continuation res)
                k #(run-many-to-many
                    (cont nil)
                    (lens/shove task
                                task-backoff-lens
                                (inc counter)))]
              ;; backoff once
              (backoff-once! counter k))


          ;; QUITTING

          (exit-status? res)
          :quit
          )))))

(defn run-many-to-many
  ([m]
   (run-many-to-many m (new-task!)))

  ([m task]
   (run-mn m task x/run)))

(defn run-many-to-many-after [m task delay]
  (run-mn m task (partial x/run-after delay)))



;; Combinations

(defn backoff-until
  ([pred!]
   (backoff-until pred! 1))

  ([pred! i]
   (m/monadic
    (if (pred!)
      ;; done
      (m/return true)
      ;; else
      (m/monadic
       ;; wait
       (print "waiting")
       (let [seed (.getId (Thread/currentThread))])
       (let [upper (bit-shift-left
                    8 (min i 14))
             r (rand-int upper)])
       (print (str  "waiting for " (pr-str r)))
       (timeout r)
       (backoff-until pred! (inc i))
       )))))

(defn swap
  [ref f]
  (m/monadic
   [ov (read ref)]
   (let [nv (f ov)])
   (print "cassing")
   [succ (cas ref ov nv)]
   (print "succ: " succ)
   (if succ
     (m/return nv)
     ;; maybe should backoff?
     (swap ref f))))




;; --- Log run ---------

#_(defn append [log item]
  (conj log item))

#_(acr/define-record-type FakeRef
  (make-fake-ref v)
  fake-ref?
  [v fake-ref-value])

#_(defn uuid! []
  (java.util.UUID/randomUUID))

#_(defn log-run [m task]
  (loop [m m
         task task
         log []
         refs {}
         parked {}
         waiting {}]
    (cond

      (m/free-bind? m)
      (let [m1 (m/free-bind-monad m)
            c (m/free-bind-cont m)]
        (cond
          (m/free-return? m1)
          (recur (c (m/free-return-val m1)) task log refs parked waiting)

          (new-ref-command? m1)
          (let [r (make-fake-ref
                   (new-ref-command-init m1))
                id (uuid!)]
            (recur (c id)
                   task
                   (append log "new-ref-command")
                   (assoc refs id r)
                   parked waiting))

          (park-command? m1)
          (let [[t k] (first waiting)]
            (recur (k nil) t (append log "parked")
                   refs (assoc parked task c)
                   (dissoc waiting t)))
          )



          )
        ))))







;; ---------------------------------------------

(defmacro mdo [& args]
  `(m/monadic
    ~@args))


(def p1
  (m/monadic
   [t (get-current-task)]
   (let [real-tid (.getId (Thread/currentThread))])
   (print "what")
   (park)
   (print (str "my task: " t))
   (print (str "real id: " real-tid))))

(def parentp
  (m/monadic
   (print "------")
   [child-tid (fork p1)]
   (print (str "child tid: " child-tid))
   (unpark child-tid nil)
   (print (str "jajajaj"))
   ))

(run-cont parentp 0)
