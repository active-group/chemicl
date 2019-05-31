(ns chemicl.stack
  (:require [active.clojure.record :as acr]
            [active.clojure.monad :as m]
            [chemicl.monad :as cm :refer [defmonadic whenm]]
            [chemicl.concurrency :as conc]
            [chemicl.backoff :as backoff]
            [chemicl.concurrency-test-runner :as test-runner]
            [clojure.test :as t :refer [deftest testing is]]))

(acr/define-record-type Stack
  (make-stack top)
  stack?
  [top stack-top-ref])

(acr/define-record-type Node
  (make-node value next)
  node?
  [value node-value
   next node-next])

(defmonadic new-stack []
  [t (conc/new-ref nil)]
  (m/return (make-stack t)))

(defmonadic ^:private push [st e]
  (let [top (stack-top-ref st)])
  [top-node (conc/read top)]

  (if top-node
    ;; then
    (m/monadic
     (let [new-top-node (make-node e top-node)])
     [succ (conc/cas top top-node new-top-node)]
     (if succ
       (m/return :done)
       (push st e)))
    ;; else
    (m/monadic
     (let [new-top-node (make-node e nil)])
     [succ (conc/cas top top-node new-top-node)]
     (if succ
       (m/return :done)
       (push st e)))))

(defmonadic push-or-reuse [st e & [pred]]
  (let [pred (or pred #(m/return true))
        top (stack-top-ref st)])
  [top-node (conc/read top)]

  (if top-node
    (m/monadic
     [active? (pred (node-value top-node))]

     (if-not active?
       ;; reuse
       (m/monadic
        (let [new-top-node
              (make-node e (node-next top-node))])
        [succ (conc/cas top top-node new-top-node)]
        (if succ
          (m/return :done)
          (push-or-reuse st e pred)))
       ;; else standard push
       (push st e)
       ))
    ;; else standard push
    (push st e)
    ))

(defmonadic try-pop-valid [st & [pred]]
  (let [pred (or pred #(m/return true))
        top (stack-top-ref st)])
  [top-node (conc/read top)]

  (if top-node
    ;; then
    (m/monadic
     [active? (pred (node-value top-node))]
     [succ (conc/cas top
                     top-node
                     (node-next top-node))]
     (if (and succ active?)
       (m/return (node-value top-node))
       (try-pop-valid st pred)))
    ;; else
    (m/return nil)
    ))


;; satisfying the message queue interface

(defmonadic cursor [st]
  (conc/read (stack-top-ref st)))

(defn cursor-value [c]
  (when c
    (node-value c)))

(defn cursor-next [c]
  (m/return
   (node-next c)))

(defmonadic clean [st pred]
  (let [top (stack-top-ref st)])
  [top-node (conc/read top)]

  (if top-node
    ;; then
    (m/monadic
     [active? (pred (node-value top-node))]
     (if active?
       (m/return :done)
       ;; else
       (m/monadic
        [succ (conc/cas top
                        top-node
                        (node-next top-node))]
        (clean st pred))))
    ;; else
    (m/return :done)))
