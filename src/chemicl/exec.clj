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
;; stolen from core.async

(defprotocol Xctr
  (exec [e runnable] "execute runnable asynchronously"))

(defn thread-pool-executor
  []
  (let [executor-svc (Executors/newFixedThreadPool 8)]
    (reify Xctr
      (exec [this r]
        (.execute executor-svc ^Runnable r)))))

(def executor (delay (thread-pool-executor)))

(defn run
  "Runs Runnable r in a thread pool thread"
  [^Runnable r]
  (exec @executor r))

(defn run-after
  "Runs Runnable r in a thread pool thread after msec"
  [^Long msec
   ^Runnable r]
  (let [timer (java.util.Timer.)
        task (proxy [java.util.TimerTask] []
               (run []
                 (run r)))]
    (.schedule timer task msec)))
