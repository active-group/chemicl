(ns chemicl.reaction-data)

(defn rx-union [& rxs]
  (let [grxs (filter some? rxs)]
    {:tag ::reaction
     :cases (mapcat :cases grxs)
     :actions (mapcat :actions grxs)
     :live? (every? :live? grxs)}))

(defn- rx-cases-order [c]
  (hash (first c)))

(defn rx-cases [rx]
  (let [cases (:cases rx)]
    (cond
      (empty? cases) cases
      (empty? (rest cases)) cases
      :else (sort-by rx-cases-order cases))))

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

(defn add-cas+action [rx cas action]
  ;; = (add-action (add-cas rx cas) action)
  {:tag ::reaction
   :cases (conj (:cases rx) cas)
   :actions (conj (:actions rx) action)
   :live? (:live? rx)})

(def empty-rx
  {:tag ::reaction
   :cases []
   :actions []
   :live? true})

(def failing-rx
  (assoc empty-rx :live? false))

(defn cas [cas]
  ;; = (add-cas empty-rx cas)
  {:tag ::reaction
   :cases [cas]
   :actions []
   :live? true})

(defn cas+action [cas action]
  ;; = (add-action (add-cas empty-rx cas) action)
  {:tag ::reaction
   :cases [cas]
   :actions [action]
   :live? true})

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
