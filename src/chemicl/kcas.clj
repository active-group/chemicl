(ns chemicl.kcas
  (:require
   [chemicl.monad :as cm :refer [defmonadic whenm]]
   [chemicl.concurrency :as conc]
   [chemicl.refs :as refs]
   [active.clojure.monad :as m]))

(defmonadic cas-all-to-sentinel [cs sentinel]
  ;; CAS all cs to a given sentinel value
  ;; return false on interference
  (conc/print "cas all to sent" (pr-str cs))
  (if (empty? cs)
    (m/return true)
    (m/monadic
     (let [[r ov _] (first cs)])
     [succ (refs/cas r ov sentinel)]
     (if succ
       (cas-all-to-sentinel (rest cs) sentinel)
       (m/return false)))))

(defmonadic rollback-cases-from-sentinel [cs sentinel]
  ;; This is too wasteful
  ;; We don't need n CASes but only n normal writes
  ;; The kcas-to-sentinel has shielded us from interference already
  (if (empty? cs)
    (m/return nil)
    (m/monadic
     (let [[r ov _] (first cs)])
     [succ (refs/cas r sentinel ov)]
     (if succ
       (rollback-cases-from-sentinel cs sentinel)
       ;; else done
       (m/return nil)))))

(defmonadic kcas-to-sentinel [cs sentinel]
  [succ (cas-all-to-sentinel cs sentinel)]
  (if succ
    (m/return true)
    ;; else rollback
    (rollback-cases-from-sentinel cs sentinel)))

(defmonadic kcas-from-sentinel [cs sentinel]
  ;; This is too wasteful
  ;; We don't need n CASes but only n normal writes
  ;; The kcas-to-sentinel has shielded us from interference already
  (if (empty? cs)
    (m/return true)
    (m/monadic
     (let [[r _ nv] (first cs)])
     [succ (refs/cas r sentinel nv)]
     (if succ
       (kcas-from-sentinel (rest cs) sentinel)
       (m/return false)))))

(defmonadic kcas [cs]
  (let [sentinel (rand)])
  [succ-1 (kcas-to-sentinel cs sentinel)]
  (if succ-1
    (kcas-from-sentinel cs sentinel)
    (m/return false)))
