(ns chemicl.message-queue
  (:require
   [chemicl.monad :as cm :refer [defmonadic whenm]]
   [chemicl.concurrency :as conc]
   [active.clojure.record :as acr]
   [active.clojure.monad :as m]
   ))

;; FIXME: replace with fine grained MSQueue or similar

(defmonadic new []
  (conc/new-ref []))

(defmonadic push [q e]
  (conc/swap q conj e))

(defmonadic clean [q predm]
  (conc/swapm
   q (fn [es]
       (m/monadic
        [new-es (cm/filterm predm es)]
        (m/return new-es)
        ))))

(defmonadic empty? [q]
  [es (conc/read q)]
  (m/return
   (clojure.core/empty? es)))

(defmonadic snapshot [q]
  (conc/read q))
