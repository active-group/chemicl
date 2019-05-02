(ns chemicl.concurrency
  (:require
   [chemicl.exec :as x]
   [active.clojure.record :as acr]
   [active.clojure.monad :as m]
   [chemicl.monad :refer [defmonadic whenm]])
  (:refer-clojure :exclude [read print]))

(declare run-many-to-many)

;; --- Cont concurrency monad: commands ---------

(acr/define-record-type ForkCommand
  (make-fork-command m)
  fork-command?
  [m fork-command-monad])

(acr/define-record-type CallCC
  (make-call-cc f)
  call-cc?
  [f call-cc-function])

(acr/define-record-type Throw
  (make-throw k v)
  throw?
  [k throw-k
   v throw-value])

(acr/define-record-type ExitCommand
  (make-exit-command)
  exit-command?
  [])

(acr/define-record-type NewRefCommand
  (make-new-ref-command init)
  new-ref-command?
  [init new-ref-command-init])

(acr/define-record-type ReadCommand
  (make-read-command ref)
  read-command?
  [ref read-command-ref])

(acr/define-record-type ResetCommand
  (make-reset-command ref nv)
  reset-command?
  [ref reset-command-ref
   nv reset-command-new-value])

(acr/define-record-type CASCommand
  (make-cas-command ref ov nv)
  cas-command?
  [ref cas-command-ref
   ov cas-command-old-value
   nv cas-command-new-value])

(acr/define-record-type TimeoutCommand
  (make-timeout-command msec)
  timeout-command?
  [msec timeout-command-msec])

(acr/define-record-type GetCurrentTaskCommand
  (make-get-current-task-command)
  get-current-task-command?
  [])

(acr/define-record-type ParkCommand
  (make-park-command)
  park-command?
  [])

(acr/define-record-type UnparkCommand
  (make-unpark-command task)
  unpark-command?
  [task unpark-command-task])


;; --- API ---------

;; This namespace provides a simple language for multithreaded programming.
;; Programs are monadic. You can use the usual active.clojure.monad tools.

(defn fork-f
  "Run the monadic program returned by `(f & args)` asynchronously on a new thread."
  ([thunk] (make-fork-command (m/free-bind (m/return nil) (fn [_] (thunk)))))
  ([f arg] (make-fork-command (m/free-bind arg f)))
  ([f a1 a2 & args] (make-fork-command (m/free-bind (m/return nil) (fn [_] (apply f a1 a2 args))))))

(defmacro fork [m]
  "Run the monadic program `m` asynchronously on a new thread."
  `(fork-f (fn [] ~m)))

(defn call-cc
  "Call a function with the current continuation as an argument"
  [f] (make-call-cc f))

(defn throw
  "Divert execution to the given continuation"
  [k v] (make-throw k v))

(let [v (make-exit-command)]
  (defn exit
    "Quit the current thread"
    [] v))

;; The concurrency language is shared-memory-based.

(defn new-ref
  "Create a new shared-memory cell"
  [init] (make-new-ref-command init))

(defn read
  "Read the value inside a shared-memory cell"
  [ref] (make-read-command ref))

(defn reset
  "Set the value inside a shared-memory cell"
  [ref nv] (make-reset-command ref nv))

(defn cas
  "Compare-and-set the value inside a shared-memory cell.
  Returns falsey iff the cas failed."
  [ref ov nv] (make-cas-command ref ov nv))

;; Timeout

(defn timeout
  "Sleep for msec"
  [msec] (make-timeout-command msec))

;; Native Parking & Unparking
;; The following three commands provide a convenient parking
;; and unparking scheme. When performance is paramount, you
;; should use `call-cc` and `throw` instead.

(let [v (make-get-current-task-command)]
  (defn get-current-task
    "Obtain the current task handle, which you can call `unpark` on"
    [] v))

(let [v (make-park-command)]
  (defn park
    "Park the current thread, returning nil when it is unparked."
    [] v))

(defn unpark
  "Unpark a given thread, returning nil."
  [task] (make-unpark-command task))

;; Run the monadic program on a fixed size thread pool with `run`

(defmacro run [& ms]
  `(run-many-to-many
    (m/monadic
     ~@ms
     )))



;; -------------------------------------------------------------

;; --- Task locks ----------

(acr/define-record-type ^:private TaskLock
  (make-task-lock s)
  task-lock?
  [s task-lock-state-ref])

(defn- new-task-lock! []
  (make-task-lock (atom nil)))

(defn- block-on-task-lock!
  "Block on lock until a permit is available.
  If it is immediately, returns `(cont nil)`, or nil otherwise."
  [lock cont]
  (let [ref (task-lock-state-ref lock)]
    (loop []
      (let [{:keys [permit?] :as state} @ref]
        (if permit?
          ;; Lock has been unlocked, we are allowed to proceed by consuming the permit
          (if (compare-and-set!
               ref 
               state
               (assoc state :permit? false))
            (cont nil)
            (recur))
          ;; No permit -> enter cont as continuation
          (if (compare-and-set!
               ref 
               state
               (assoc state :continuation cont))
            nil
            (recur)))))))

(defn- signal-on-task-lock!
  "Signal a blocked task. Returns the monadic continuation if there is one, or nil otherwise."
  [lock]
  (let [ref (task-lock-state-ref lock)]
    (loop []
      (let [{:keys [continuation] :as state} @ref]
        (if continuation
          ;; blocked continuation found -> remove it and return it
          (if (compare-and-set! ref state {:permit? false
                                           :continuation nil})
            (continuation nil)
            (recur))
          ;; no blocked continuation found -> leave a permit
          (if (compare-and-set! ref state {:permit? true
                                           :continuation nil})
            nil
            (recur)))))))


;; --- Task abstraction ---------

(acr/define-record-type Task
  (make-task tl ret)
  task?
  [tl task-lock
   ret task-return-value-promise])

(defn new-task! []
  (make-task (new-task-lock!) (promise)))

(defn- new-detached-task! []
  (make-task (new-task-lock!) nil))

(defn- signal-task! [t]
  (signal-on-task-lock!
   (task-lock t)))

(defn- block-task! [t cont]
  (block-on-task-lock!
   (task-lock t)
   cont))


;; Debugging only

(acr/define-record-type PrintCommand
  (make-print-command l)
  print-command?
  [l print-command-line])

(defn print [& s] (make-print-command s))


;; --- Cont concurrency monad: status return values ---------

(acr/define-record-type ^:private ParkStatus
  (make-park-status c)
  park-status?
  [c park-status-continuation])

(acr/define-record-type ^:private UnparkStatus
  (make-unpark-status c unpark-task)
  unpark-status?
  [c unpark-status-continuation
   unpark-task unpark-status-unpark-task])

(acr/define-record-type ^:private TimeoutStatus
  (make-timeout-status c timeout)
  timeout-status?
  [c timeout-status-continuation
   timeout timeout-status-timeout])

(acr/define-record-type ^:private ForkStatus
  (make-fork-status c m)
  fork-status?
  [c fork-status-continuation
   m fork-status-monad])

(acr/define-record-type ^:private ExitStatus
  (make-exit-status v)
  exit-status?
  [v exit-status-value])


;; --- (Inner) monad runner ---------

(defn- safe-println [& more]
  (.write *out* (str (clojure.string/join " " more) "\n")))

(defn- run-cont [m task]
  (loop [m m
         task task]
    (cond

      (m/free-bind? m)
      (let [m1 (m/free-bind-monad m)
            c (m/free-bind-cont m)]
        (cond
          (m/free-return? m1)
          (recur (c (m/free-return-val m1)) task)

          (call-cc? m1)
          (let [f (call-cc-function m1)]
            (recur (f c) task))

          (throw? m1)
          (let [k (throw-k m1)
                v (throw-value m1)]
            (recur (k v) task))

          ;; Read & CAS & Reset

          (new-ref-command? m1)
          (let [a (atom (new-ref-command-init m1))]
            (recur (c a) task))

          (cas-command? m1)
          (do
            (let [succ 
                  (compare-and-set! (cas-command-ref m1)
                                    (cas-command-old-value m1)
                                    (cas-command-new-value m1))]
              (recur (c succ) task)))

          (read-command? m1)
          (recur
           (c (deref (read-command-ref m1)))
           task)

          (reset-command? m1)
          (let [res (reset! (reset-command-ref m1)
                            (reset-command-new-value m1))]
            (recur (c res) task))

          ;; Concurrency control

          (park-command? m1)
          (make-park-status c)

          (unpark-command? m1)
          (make-unpark-status
           c (unpark-command-task m1))

          (fork-command? m1)
          (make-fork-status c (fork-command-monad m1))

          (get-current-task-command? m1)
          (recur (c task) task)

          (exit-command? m1)
          (make-exit-status nil)

          ;; Timeout

          (timeout-command? m1)
          (make-timeout-status
           c (timeout-command-msec m1))

          ;; Debugging only

          (print-command? m1)
          (do
            (apply safe-println (print-command-line m1))
            (recur (c nil) task))

          :else
          (throw (ex-info (str "Unknown monad command: " m1) {:command m1}))))

      (m/free-return? m)
      (make-exit-status
       (m/free-return-val m))

      (new-ref-command? m)
      (make-exit-status (atom (new-ref-command-init m)))

      (cas-command? m)
      (make-exit-status
       (compare-and-set! (cas-command-ref m)
                         (cas-command-old-value m)
                         (cas-command-new-value m)))

      (read-command? m)
      (make-exit-status
       (deref (read-command-ref m)))

      (reset-command? m)
      (make-exit-status
       (reset! (reset-command-ref m)
               (reset-command-new-value m)))

      (park-command? m)
      (make-exit-status nil)

      (unpark-command? m)
      ;; beware: continuation is nil, which needs to be handled in outer runner
      (make-unpark-status
       nil
       (unpark-command-task m))

      (fork-command? m)
      ;; Run the forked task on the same thread but with a different task lock
      (recur (fork-command-monad m) (new-detached-task!))

      (get-current-task-command? m)
      ;; what do you want to do with it mate?
      (make-exit-status nil)

      (exit-command? m)
      (make-exit-status nil)

      ;; callcc

      (call-cc? m)
      (let [f (call-cc-function m)]
        (recur (f (fn [_] (make-exit-command))) task))

      (throw? m)
      (let [k (throw-k m)
            v (throw-value m)]
        (recur (k v) task))

      ;; debugging only

      (print-command? m)
      (do
        (apply safe-println (print-command-line m))
        (make-exit-status nil))

      :else
      (throw (ex-info (str "Unknown monad command: " m) {:command m})))))


;; --- Outside runner ---------

(declare run-many-to-many-after)

(defn- run-nm-0 [m task runner]
  ;; Note: this should loop to continue with the same tasks. Other
  ;; tasks should be handed to the thread-pool via
  ;; run-many-to-many.
  (loop [m m]
    (let [res (run-cont m task)]
      (cond

        ;; PARKING

        (park-status? res)
        (when-let [mm (block-task!
                       task
                       (park-status-continuation res))]
          (recur mm))

        (unpark-status? res)
        (let [cont (unpark-status-continuation res)
              utask (unpark-status-unpark-task res)]
          ;; Unpark the parked task on a new thread
          (when-let [mm (signal-task! utask)]
            (run-many-to-many mm utask))

          ;; Continue the unparking task on this thread
          (when cont
            (recur (cont true))))


        ;; FORKING

        (fork-status? res)
        (let [cont (fork-status-continuation res)
              fm (fork-status-monad res)
              new-task (new-detached-task!)]
          ;; Run the forked task on a new thread
          (run-many-to-many fm new-task)

          ;; Continue the parent task on this thread
          (recur (cont new-task)))


        ;; TIMEOUT

        (timeout-status? res)
        (let [cont (timeout-status-continuation res)
              msec (timeout-status-timeout res)]
          (run-many-to-many-after (cont nil) task msec))


        ;; QUITTING

        (exit-status? res)
        (when-let [p (task-return-value-promise task)]
          (deliver p (exit-status-value res)))

        :else
        (throw (ex-info (str "Unknown exit status " res) {:status res}))))))

(defn- run-mn [m task runner]
  (runner
   (fn []
     (run-nm-0 m task runner)))

  (task-return-value-promise task))

(defn run-many-to-many
  ([m]
   (run-many-to-many m (new-task!)))

  ([m task]
   (run-many-to-many m task x/run))

  ([m task runner]
   (run-mn m task runner)))

(defn run-many-to-many-after [m task delay]
  (run-mn m task (partial x/run-after delay)))

(defn- this-thread-runner [f]
  ((x/wrap f)))

(defn run-many-to-many!
  ;; Note: this 
  ([m]
   (deref (run-many-to-many m (new-task!) this-thread-runner)))
  ([m timeout-ms timeout-val]
   (deref (run-many-to-many m (new-task!) this-thread-runner) timeout-ms timeout-val)))
