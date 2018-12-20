(ns chemicl.reactions
  (:require
   [chemicl.monad :as cm :refer [defmonadic whenm]]
   [chemicl.concurrency :as conc]
   [chemicl.refs :as refs]
   [active.clojure.record :as acr]
   [active.clojure.lens :as lens]
   [active.clojure.monad :as m]
   [clojure.core.match :refer [match]]))

(acr/define-record-type Reaction
  (make-reaction cases offers post-commits)
  reaction?
  [(cases reaction-cases reaction-cases-lens) ;; [[ref ov nv]]
   (offers reaction-offers reaction-offers-lens)
   (post-commits reaction-post-commits reaction-post-commits-lens)]) ;; [conc-monad-action]

(defn empty-reaction []
  (make-reaction [] [] []))

(defn reaction-cases-sorted [rx]
  (sort-by (fn [[r _ _]]
             (hash r))
           (reaction-cases rx)))

(defn has-cas? [rx]
  (not (empty? (reaction-cases rx))))

(defn add-cas [rx ref ov nv]
  (lens/overhaul
   rx reaction-cases-lens
   (fn [cs]
     (conj cs [ref ov nv]))))



;; Commit

(defmonadic cas-all-to-sentinel [cs sentinel]
  ;; CAS all cs to a given sentinel value
  ;; return false on interference
  (conc/print "cas all to sent" (pr-str cs))
  (if (empty? cs)
    (m/return true)
    (m/monadic
     (let [[r ov _] (first cs)])
     [succ (refs/cas-data r ov sentinel)]
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
     [succ (refs/cas-data r sentinel ov)]
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
     [succ (refs/cas-data r sentinel nv)]
     (if succ
       (kcas-from-sentinel (rest cs) sentinel)
       (m/return false)))))

(defmonadic kcas [cs]
  (let [sentinel (rand)])
  [succ-1 (kcas-to-sentinel cs sentinel)]
  (if succ-1
    (kcas-from-sentinel cs sentinel)
    (m/return false)))

(defmonadic try-commit [rx]
  ;; Perform the CASes
  (let [cases (reaction-cases-sorted rx)])
  [cas-succ
   (cond
     (empty? cases)
     (m/return true)

     (= 1 (count cases))
     (let [[r ov nv] (first cases)]
       (refs/cas-data r ov nv))

     :else
     (kcas cases))]

  (if cas-succ
    ;; Perform post commit actions
    (m/monadic
     (let [pcs (reaction-post-commits rx)])
     (m/sequ_ pcs)
     (m/return true))
    ;; else fail
    (m/return false)))
