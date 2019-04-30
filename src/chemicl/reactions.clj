(ns chemicl.reactions
  (:require
   [chemicl.monad :as cm :refer [defmonadic whenm]]
   [chemicl.concurrency :as conc]
   [chemicl.kcas :as kcas]
   [chemicl.reaction-data :as rx-data]
   [active.clojure.monad :as m]))


(defmonadic try-commit [rx]
  (whenm (rx-data/live? rx)
    (let [cases (rx-data/rx-cases rx)])
    [succ (cond
            (empty? cases)
            (m/return true)

            (empty? (rest cases))
            (let [[r ov nv] (first cases)]
              (conc/cas r ov nv))

            :else
            (kcas/kcas cases))]

    ;; perform post commits
    ;; TODO: maybe we want to associate
    ;; post-commit acctions with cases
    ;; and perform them in unison?
    (whenm succ
      (m/sequ_ (rx-data/rx-actions rx)))

    (m/return succ)))
