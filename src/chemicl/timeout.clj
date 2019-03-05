(ns chemicl.timeout
  (:require
   [chemicl.reagents :as rea]
   [chemicl.concurrency :as conc]
   [active.clojure.monad :as m]
   ))

(defn- now! []
  (new java.util.Date))

(defn- add-msec [date msec]
  (let [cal (java.util.Calendar/getInstance)]
    (.setTime cal date)
    (.add cal java.util.Calendar/MILLISECOND msec)
    (.getTime cal)))

(defn- before-now? [date]
  (> (compare
      (now!)
      date) 0))


;; --- API ---------

(defn timeout [msec]
  (rea/my
   (fn [a start-time rerun]
     (if start-time
       ;; been here before
       (if (before-now? (add-msec start-time msec))
         ;; continue
         (rea/my-return a)
         
         ;; else block
         (rea/my-block))

       ;; first time around
       (m/monadic
        ;; fork waker upper
        (conc/fork
         (m/monadic
          (conc/timeout msec)
          rerun))
        ;; block
        (rea/my-block (now!)))
       ))))
