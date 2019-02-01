(ns chemicl.examples.elimination-stack
  (:require [chemicl.reagents :as rea]
            [active.clojure.monad :as m]
            [chemicl.monad :as cm :refer [defmonadic whenm]]
            [chemicl.concurrency :as conc]
            [chemicl.channels :as channels]
            [chemicl.refs :as refs]))

;; Treiber stack

(defn new-stack []
  (refs/new-ref []))

(defn stack-push [st]
  (rea/upd st (fn [[xs x]]
                [(concat [x] xs) nil])))

(defn stack-pop [st]
  (rea/upd st (fn [[xs _]]
                (when-not (empty? xs)
                  [(rest xs) (first xs)]))))

(defn push-imm [st el]
  (rea/react! (stack-push st) el))

(defn pop-imm [st]
  (rea/react! (stack-pop st) nil))


;; Elimination stack

(defmonadic new-elimination-stack []
  [ts (new-stack)]
  [[l r] (channels/new-channel)]
  (m/return {:stack ts
             :push-endpoint l
             :pop-endpoint r}))

(defn elim-push [{:keys [stack push-endpoint]}]
  (rea/choose
   (stack-push stack)
   (rea/swap push-endpoint)))

(defn elim-pop [{:keys [stack pop-endpoint]}]
  (rea/choose
   (stack-pop stack)
   (rea/swap pop-endpoint)))

(defn elim-push-imm [st el]
  (rea/react! (elim-push st) el))

(defn elim-pop-imm [st]
  (rea/react! (elim-pop st) nil))



(conc/run-many-to-many
 (m/monadic
  [stack (new-elimination-stack)]

  [tid (conc/fork
        (m/monadic
         (conc/print "parking")
         [res (elim-pop-imm stack)]
         (conc/print "child got" (pr-str res))))]

  (conc/fork
   (m/monadic
    (conc/timeout 2000)
    (elim-push-imm stack "hi")))
  ))
