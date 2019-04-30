(ns chemicl.exec
  (:import [java.util.concurrent Executors Executor])
  (:require
   [active.clojure.record :as acr]
   [active.clojure.lens :as lens]
   [active.clojure.monad :as m]))


;; --- Helpers ---------

(defn print-tid! []
  (println "THREAD: "
           (pr-str
            (.getId (Thread/currentThread)))))




;; --- Task executor ---------

(defn- thread-pool-executor
  []
  (Executors/newScheduledThreadPool (max 8 (* 2 (.availableProcessors (Runtime/getRuntime))))))

(defonce ^:private executor (delay (thread-pool-executor)))

(def ^:private debug false)

(defn ^Runnable wrap [^Runnable r]
  (if debug
    (fn []
      (try (.run r)
           ;; TODO: put something equiv in conc ?
           (catch Throwable t
             (println "Uncaught exception in thread" t)
             (throw t))))
    r))

(defn run
  "Runs Runnable r in a thread pool thread"
  [^Runnable r]
  (.execute ^java.util.concurrent.ExecutorService @executor ^Runnable (wrap r)))

(defn run-after
  "Runs Runnable r in a thread pool thread after msec"
  [^long msec
   ^Runnable r]
  (.schedule ^java.util.concurrent.ScheduledExecutorService @executor ^Runnable (wrap r) msec java.util.concurrent.TimeUnit/MILLISECONDS))
