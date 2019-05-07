(ns chemicl.kcas
  (:require
   [chemicl.monad :as cm :refer [defmonadic whenm]]
   [chemicl.concurrency :as conc]
   [active.clojure.monad :as m]
   [chemicl.backoff :as backoff])
  (:refer-clojure :exclude [read]))

(defonce ^:private sentinel-t ::sentinel)

(defn- make-sentinel [rn]
  [sentinel-t rn])

(defn- sentinel? [v]
  (and (vector? v)
       (identical? (first v) sentinel-t)))

(defmonadic cas-all-to-sentinel-counting [cs sentinel counter]
  (if (empty? cs)
    (m/return counter)
    (m/monadic
     (let [[r ov _] (first cs)])
     [succ (conc/cas r ov sentinel)]
     (if succ
       (cas-all-to-sentinel-counting
        (rest cs)
        sentinel
        (inc counter))
       ;; else
       (m/return counter)))))

(defmonadic cas-all-to-sentinel [cs sentinel]
  ;; CAS all cs to a given sentinel value
  ;; return the number of successful cases
  (cas-all-to-sentinel-counting cs sentinel 0))

(defmonadic rollback-cases-from-sentinel [cs sentinel until-idx]
  #_(conc/print "rolling back for sentinel" (pr-str sentinel))
  #_(conc/print (str "---> " until-idx " <---"))
  ;; This is too wasteful
  ;; We don't need n CASes but only n normal writes
  ;; The kcas-to-sentinel has shielded us from interference already
  (m/sequ_ (mapv (fn [[r ov _]]
                   (conc/reset r ov)) (take until-idx cs)))
  (m/return true))

(defmonadic kcas-to-sentinel [cs sentinel]
  [nsucc (cas-all-to-sentinel cs sentinel)]
  #_(conc/print (str ":::" nsucc " : " (count cs)))
  (if (= nsucc (count cs))
    (m/return true)
    ;; else rollback
    (m/monadic
     (rollback-cases-from-sentinel cs sentinel nsucc)
     (m/return false))))

(defmonadic kcas-from-sentinel [cs sentinel]
  ;; FIXME: This is possibly too wasteful
  ;; We don't need n atomic resets but only n normal writes
  ;; The kcas-to-sentinel has shielded us from interference already
  ;; We want something like vreset! on volatiles
  ;; which is not directly supported on atoms
  (m/sequ_ (mapv (fn [[r _ nv]]
                  (conc/reset r nv)) cs))
  (m/return true))

(defmonadic kcas [cs]
  ;; maybe rand is too expensive
  ;; we should at least precompute it for each lightweight thread
  (let [sentinel (make-sentinel (rand))])
  [succ-1 (kcas-to-sentinel cs sentinel)]
  (if succ-1
    (kcas-from-sentinel cs sentinel)
    (m/return false)))


;; --- Raw refs

(def new-ref conc/new-ref)

(def cas conc/cas)

(def reset conc/reset)

(defmonadic read [r]
  (backoff/with-exp-backoff
    [v (conc/read r)]
    (if (sentinel? v)
      ;; r is currently being kcased on
      (m/return (backoff/retry-backoff))
      ;; else success
      (m/return (backoff/done v)))))
