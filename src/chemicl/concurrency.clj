(ns chemicl.concurrency
  (:require
   [chemicl.exec :as x]
   [active.clojure.record :as acr]
   [active.clojure.lens :as lens]
   [active.clojure.monad :as m]
   [chemicl.monad :refer [defmonadic whenm]]))

(declare run-many-to-many)
(declare make-fork-command)
(declare make-call-cc)
(declare make-throw)
(declare make-exit-command)
(declare make-new-ref-command)
(declare make-read-command)
(declare make-reset-command)
(declare make-cas-command)
(declare make-timeout-command)
(declare make-get-current-task-command)
(declare make-park-command)
(declare make-unpark-command)


;; --- API ---------

;; This namespace provides a simple language for multithreaded programming.
;; Programs are monadic. You can use the usual active.clojure.monad tools.

(defn fork
  "Run a monadic program asynchronously"
  [m] (make-fork-command m))

(defn call-cc
  "Call a function with the current continuation as an argument"
  [f] (make-call-cc f))

(defn throw
  "Divert execution to the given continuation"
  [k v] (make-throw k v))

(defn exit
  "Quit the current thread"
  [] (make-exit-command))

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

(defn get-current-task
  "Obtain the current task handle, which you can call `unpark` on"
  [] (make-get-current-task-command))

(defn park
  "Park the current thread"
  [] (make-park-command))

(defn unpark
  "Unpark a given thread and optionally pass a value to it."
  [task value] (make-unpark-command task value))

;; Run the monadic program on a fixed size thread pool with `run`

(defmacro run [& ms]
  `(run-many-to-many
    (m/monadic
     ~@ms
     )))



;; -------------------------------------------------------------

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
  (make-task tl ret)
  task?
  [(tl task-lock task-lock-lens)
   (ret task-return-value-ref task-return-value-ref-lens)])

(defn new-task! []
  (make-task (new-task-lock!) (atom nil)))

(defn signal-task! [t v]
  (signal-on-task-lock!
   (task-lock t) v))

(defn block-task! [t cont]
  (block-on-task-lock!
   (task-lock t)
   cont))


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
  (make-unpark-command task value)
  unpark-command?
  [task unpark-command-task
   value unpark-command-value])

;; Debugging only

(acr/define-record-type PrintCommand
  (make-print-command l)
  print-command?
  [l print-command-line])

(defn print [& s] (make-print-command s))


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
  (make-exit-status v)
  exit-status?
  [v exit-status-value])


;; --- (Inner) monad runner ---------

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
           c (unpark-command-task m1)
           (unpark-command-value m1))

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
            (apply println (print-command-line m1))
            (recur (c nil) task))))

      (m/free-return? m)
      (make-exit-status
       (m/free-return-val m))

      (new-ref-command? m)
      (make-exit-status nil)

      (cas-command? m)
      (compare-and-set! (cas-command-ref m)
                        (cas-command-old-value m)
                        (cas-command-new-value m))

      (read-command? m)
      (make-exit-status
       (deref (read-command-ref m)))

      (reset-command? m)
      (reset! (reset-command-ref m)
              (reset-command-new-value m))

      (park-command? m)
      (make-exit-status nil)

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
        (apply println (print-command-line m))
        (make-exit-status nil))
      )))


;; --- Outside runner ---------

(declare run-many-to-many-after)

(defn- run-mn [m task runner]
  (runner
    (fn []
      (let [res (run-cont m task)]
        (cond

          ;; PARKING

          (park-status? res)
          (when-let [mm (block-task!
                         task
                         (park-status-continuation res))]
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


          ;; TIMEOUT

          (timeout-status? res)
          (let [cont (timeout-status-continuation res)
                msec (timeout-status-timeout res)]
            (run-many-to-many-after (cont nil) task msec))


          ;; QUITTING

          (exit-status? res)
          (reset! (task-return-value-ref task)
                  (exit-status-value res))
          ))))
  (task-return-value-ref task))

(defn run-many-to-many
  ([m]
   (run-many-to-many m (new-task!)))

  ([m task]
   (run-mn m task x/run)))

(defn run-many-to-many-after [m task delay]
  (run-mn m task (partial x/run-after delay)))

(defn run-here [m]
  (loop [m m]
    (let [res (run-cont m :here)]
      (cond
        (timeout-status? res)
        (let [cont (timeout-status-continuation res)
              msec (timeout-status-timeout res)]
          (Thread/sleep msec)
          (recur (cont nil)))

        (exit-status? res)
        (exit-status-value res)
        ))))
