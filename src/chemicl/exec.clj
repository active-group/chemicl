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

(defn thread-pool-executor
  []
  (Executors/newScheduledThreadPool (max 8 (* 2 (.availableProcessors (Runtime/getRuntime))))))

(defonce executor (delay (thread-pool-executor)))

(defn run
  "Runs Runnable r in a thread pool thread"
  [^Runnable r]
  (.execute @executor r))

(defn run-after
  "Runs Runnable r in a thread pool thread after msec"
  [^long msec
   ^Runnable r]
  (.schedule @executor r msec java.util.concurrent.TimeUnit/MILLISECONDS))
