(ns chemicl.reaction-data)

(defn rx-union [& rxs]
  {:tag ::reaction
   :cases (apply concat (map :cases rxs))
   :actions (apply concat (map :actions rxs))
   :live? (every? :live? rxs)})

(defn rx-cases [rx]
  (sort-by (fn [[r _ _]]
             (hash r))
           (:cases rx)))

(defn rx-actions [rx]
  (:actions rx))

(defn live? [rx]
  (:live? rx))

(defn add-cas [rx cas]
  (update rx :cases conj cas))

(defn add-action
  "Add a monadic action to a reaction"
  [rx m]
  (update rx :actions conj m))

(defn empty-rx []
  {:tag ::reaction
   :cases []
   :actions []
   :live? true})

(defn failing-rx []
  (assoc (empty-rx) :live? false))


;; Stringify

(defn cas->str [[r ov nv]]
  (str ">>>" "\n"
       "\tref: " (pr-str r) "\n"
       "\tov: " (pr-str ov) "\n"
       "\tnv: " (pr-str nv) "\n"
       "<<<" "\n"
       ))

(defn rx->str [{:keys [cases actions]}]
  (str "actions: " (pr-str actions)
       (clojure.string/join "\n\n" (map cas->str cases))))
