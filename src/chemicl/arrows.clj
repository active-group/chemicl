(ns chemicl.reagents
  (:require
   [active.clojure.record :as acr]
   [active.clojure.lens :as lens]
   [active.clojure.monad :as m]))

;; An arrow is a data structure that allows three ops
;; --------------------------------------------------

;; make function to arrow
;; : (b -> c) -> A b c
(acr/define-record-type Arr
  (make-arr f)
  arr?
  [f arr-f])

(defn arr [f] (make-arr f))

;; compose arrows
;; : A b c -> A c d -> A b d
(acr/define-record-type Composition
  (make-composition a1 a2)
  composition?
  [a1 composition-a1
   a2 composition-a2])

(defn >>> [a1 a2] (make-composition a1 a2))

;; save values across arrows
;; : A b c -> A (b, d) (c, d)
(acr/define-record-type First
  (make-first a)
  first?
  [a first-arrow])

(defn first [a] (make-first a))

;; Derived combinators

;; : A b c -> A (d, b) (d, c)
(defn second [a]
  (let [swap (fn [[x y]]
               [y x])]
    (>>> (arr swap)
         (>>> (first a)
              (arr swap)))))

;; : A b c -> A d e -> A (b, d) (c, e)
(defn *** [f g]
  (>>> (first f)
       (second g)))

;; : A b c -> A b d -> A b (c, d)
(defn &&& [f g]
  (>>> (arr (fn [x] [x x]))
       (*** f g)))

;; liftM2 for arrows
;; : (b -> c -> d) -> A e b -> A e c -> A e d
(defn liftA2 [op f g]
  (>>> (&&& f g)
       (arr (fn [[b c]] (op b c)))))
