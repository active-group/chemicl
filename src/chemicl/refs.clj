(ns chemicl.refs
  (:require
   [chemicl.concurrency :as conc]
   [active.clojure.record :as acr]
   [active.clojure.lens :as lens]
   [active.clojure.monad :as m]))

;; Waiters
;; A waiter holds an offer value and a waiter

(acr/define-record-type Waiter
  (make-waiter task offer)
  waiter?
  [task waiter-task
   offer waiter-offer])


(acr/define-record-type Ref
  (make-ref data-ref waiters-ref)
  ref?
  [data-ref ref-data
   waiters-ref ref-waiters])

;; module type S = sig
;;   type 'a ref
;;   type ('a,'b) reagent
;;   val mk_ref   : 'a -> 'a ref
;;   val read     : 'a ref -> (unit, 'a) reagent
;;   val read_imm : 'a ref -> 'a
;;   val cas      : 'a ref -> 'a -> 'a -> (unit, unit) reagent
;;   val cas_imm  : 'a ref -> 'a -> 'a -> bool
;;   val upd      : 'a ref -> ('a -> 'b -> ('a *'c) option) -> ('b,'c) reagent
;; end

(defn new-ref [init]
  (m/monadic
   (conc/print "making a new ref")
   [r1 (conc/new-ref init)]
   [r2 (conc/new-ref [])]
   (m/return
    (make-ref r1 r2))))

(defn read-data [r]
  (m/monadic
   (conc/print "Reading from ref")
   (conc/read 
    (ref-data r))))

(defn cas-data [r ov nv]
  (conc/cas 
   (ref-data r) ov nv))

;; TODO: make offers a fine-grained circular pool
(defn- put-waiter [ref task offer]
  (conc/swap (ref-waiters ref)
             (fn [ws]
               (conj ws (make-waiter task offer)))))

(defn put-offer [ref offer]
  (m/monadic
   [me (conc/get-current-task)]
   (put-waiter ref me offer)
   (m/return nil)))

(defn wake-all-waiters [ref]
  (m/monadic
   [ws (conc/read (ref-waiters ref))]
   (conc/print "ws: " (pr-str ws))
   (let [ts (map waiter-task ws)])
   (m/sequ_
    (map conc/unpark ts))))

(defn wake-all-waiters! [ref]
  (conc/run-many-to-many (wake-all-waiters ref)))

