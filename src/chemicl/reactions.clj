(ns chemicl.reactions
  (:require
   [chemicl.monad :as cm :refer [defmonadic whenm]]
   [chemicl.concurrency :as conc]
   [chemicl.refs :as refs]
   [chemicl.kcas :as kcas]
   [active.clojure.record :as acr]
   [active.clojure.lens :as lens]
   [active.clojure.monad :as m]
   [clojure.core.match :refer [match]]))


(defn rx-union [rx-1 rx-2]
  {:tag ::reaction
   :cases (concat (:cases rx-1)
                  (:cases rx-2))})

(defn rx-cases [rx]
  (sort-by (fn [[r _ _]]
             (hash r))
           (:cases rx)))

(defn rx-from-cas [cas]
  {:tag ::reaction
   :cases [cas]})

(defn empty-rx []
  {:tag ::reaction
   :cases []})

(defn cas->str [[r ov nv]]
  (str ">>>" "\n"
       "\tref: " (pr-str r) "\n"
       "\tov: " (pr-str ov) "\n"
       "\tnv: " (pr-str nv) "\n"
       "<<<" "\n"
       ))

(defn rx->str [{:keys [cases]}]
  (clojure.string/join "\n\n" (map cas->str cases)))

(defmonadic rx-commit [rx]
  (conc/print "committing" (rx->str rx))
  (let [cases (rx-cases rx)])
  (cond
    (empty? cases)
    (m/return true)

    (= 1 (count cases))
    (let [[r ov nv] (first cases)]
      (refs/cas r ov nv))

    :else
    (m/monadic
     [succ (kcas/kcas cases)]
     (whenm (not succ)
       ;; backoff
       (rx-commit rx))))

  ;; wake all waiters
  (let [rs (map first cases)])
  (m/sequ_ (map refs/rescind-offers rs)))
