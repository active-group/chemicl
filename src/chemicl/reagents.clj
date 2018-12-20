(ns chemicl.reagents
  (:require
   [chemicl.monad :as cm :refer [defmonadic whenm]]
   [chemicl.concurrency :as conc]
   [chemicl.reactions :as rx]
   [chemicl.refs :as refs]
   [active.clojure.record :as acr]
   [active.clojure.lens :as lens]
   [active.clojure.monad :as m]))


;; --- Internal Reagent Data Structures ---------

(acr/define-record-type Commit
  (make-commit)
  commit?
  [])

(acr/define-record-type Read
  (make-read ref k)
  read?
  [ref read-ref
   k read-continuation])

(acr/define-record-type CAS
  (make-cas ref ov nv k)
  cas?
  [ref cas-ref
   ov cas-old-value
   nv cas-new-value
   k cas-continuation])

;; 'a ref -> ('a -> 'b -> ('a * 'c) option) -> ('c,'r) reagent -> ('b,'r) reagent
(acr/define-record-type Update
  (make-update ref updf k)
  update?
  [ref update-ref
   updf update-function
   k update-continuation])

(acr/define-record-type Choice
  (make-choice r1 r2)
  choice?
  [r1 choice-left
   r2 choice-right])

(acr/define-record-type Computed
  (make-computed c)
  computed?
  [c computed-computation])

(acr/define-record-type Swap
  (make-swap ep k)
  swap?
  [ep swap-endpoint
   k swap-continuation])

;; CAS

(defn has-cas? [reagent]
  (cond
    (commit? reagent)
    false

    (cas? reagent)
    true

    (choice? reagent)
    ;; FIXME: loop recur
    (or (has-cas? (choice-left reagent))
        (has-cas? (choice-right reagent)))

    (swap? reagent)
    true

    (update? reagent)
    true

    :else
    false))

(defn can-cas-immediate? [k rx offer]
  (and (not (rx/has-cas? rx))
       (not (has-cas? k))
       (not offer)))




;; has cas predicate for reactions



;; --- Helpers ---------




;; --- Reacting ---------

#_(acr/define-record-type PostCommitCAS
  (make-post-commit-cas r ov nv k)
  post-commit-cas?
  [r pcc-ref
   ov pcc-old-value
   nv pcc-new-value
   k pcc-continuation])

(declare try-react)

(defn try-react-commit [reagent a rx offer]
  (println "--- committing ---")
  (println (pr-str reagent))
  (println (pr-str a))
  (println (pr-str rx))
  (println (pr-str offer))
  (cond
    offer
    (m/monadic
     [succ (rescind-offer offer)]
     (whenm (not succ)
       (get-answer offer)))

    (not offer)
    (m/monadic
     [succ (rx/try-commit rx)]
     (conc/print "succ in try-react-commit" (pr-str succ))
     (if succ
       (m/return a)
       ;; else retry
       (m/return :retry)))))

(defn try-react-read [reagent a rx offer]
  (let [r (read-ref reagent)
        k (read-continuation reagent)]
    (m/monadic
     (m/return
      (when offer
        (refs/put-offer r offer)))
     (let [v (refs/read-data r)])
     (try-react k v rx offer))))

(defn try-react-cas [reagent a rx offer]
  (let [ref (cas-ref reagent)
        ov (cas-old-value reagent)
        nv (cas-new-value reagent)
        k (cas-continuation reagent)]
    (if (and (not (rx/has-cas? rx))
             (not (has-cas? k)))
      ;; try cas immediately
      (m/monadic
       (let [succ (compare-and-set! ref ov nv)])
       (if succ
         (try-react k rx offer)
         :retry))
      ;; else integrate cas
      (try-react k nil (rx/add-cas rx ref ov nv) offer)
      )))

(defn try-react-update [reagent a rx offer]
  (println "try-react-update")
  (println (pr-str reagent))
  (println (pr-str a))
  (println (pr-str rx))
  (println (pr-str offer))
  (let [r (update-ref reagent)
        f (update-function reagent)
        k (update-continuation reagent)]
    (if (can-cas-immediate? k rx offer)
      ;; try the CAS right away
      (m/monadic
       (conc/print "can cas imm")
       [ov (refs/read-data r)]
       (conc/print "READ: " ov)
       (let [res (f [ov a])])
       (conc/print "RES: " (pr-str res))
       (if res
         (let [[nv retv] res]
           (m/monadic
            [succ (refs/cas-data r ov nv)]
            (conc/print "succ in update cas: " (pr-str succ))
            (if succ
              ;; wake waiters and continue
              (m/monadic
               (refs/wake-all-waiters r)
               (conc/print "woken all waiters")
               (conc/print "handing over to try-react impl for" (pr-str k))
               (try-react k retv rx offer))
              ;; else retry
              (m/return :retry))))
         ;; else block
         (m/return :block)
         ))

      ;; cannot cas immediately
      (m/monadic
       (whenm offer
         ;; wait on the ref
         (refs/put-offer r offer))

       [ov (refs/read-data r)]
       (let [res (f [ov a])])
       (if-not res
         (m/return :block)
         ;; else record cas for the commit phase
         (m/monadic
          (let [[nv retv] res])
          (try-react k retv (rx/add-cas rx r ov nv) offer))
         )))))

(defn try-react-choice [reagent a rx offer]
  (let [left (choice-left reagent)
        right (choice-right reagent)]
    (m/monadic
     [res-1 (try-react left)]
     (case res-1
       :retry
       (m/monadic
        [res-2 (try-react right)]
        (case res-2
          :block
          (m/return :retry)

          :retry
          (m/return :retry)

          (m/return res-2)))

       :block
       (try-react right)

       (m/return res-1)))))

(defn try-react [reagent a rx offer]
  (cond
    (commit? reagent)
    (try-react-commit reagent a rx offer)

    (read? reagent)
    (make-read (read-ref reagent)
               )

    (cas? reagent)
    (try-react-cas reagent a rx offer)

    (update? reagent)
    (try-react-update reagent a rx offer)

    (choice? reagent)
    (try-react-choice reagent a rx offer)
    

    ))



;; Helpers

(defn may-sync? [reagent]
  true
  )



;; Outer react protocol

(defmonadic p []
  [o (new-offer)]
  (conc/print "answering offer")
  (answer-offer o "hi na")
  (conc/print "done")
  )

(conc/run-cont (p) :task)

(defn with-offer [reagent a]
  (m/monadic
   [offer (new-offer)]
   [res (try-react reagent a (rx/empty-reaction) offer)]
   (case res
     :block
     (m/monadic
      (conc/park)
      [succ (rescind-offer offer)]
      (if succ
        (with-offer reagent a)
        (get-answer offer)))

     :retry
     (m/monadic
      [succ (rescind-offer offer)]
      (if succ
        (with-offer reagent a)
        (answer-offer offer)))

     (m/return res) 
     )))

(defn without-offer [reagent a]
  (m/monadic
   [res (try-react reagent a (rx/empty-reaction) nil)]
   (case res
     :block
     (with-offer reagent a)

     :retry
     (m/monadic
      (if (may-sync? reagent)
        (with-offer reagent a)
        (without-offer reagent a)))

     (m/return res)
     )))

;; Reagent A B => A -> B
;; aka run
(defn react [reagent a]
  (m/monadic
   (without-offer reagent a)))








;; --- Playground ---------

(defmonadic make-treiber []
  (refs/new-ref '()))

(defn treiber-push-k [ts k]
  (make-update ts
            (fn [[ov retv]]
              [(conj ov retv) nil])
            k))

(defn treiber-push [ts]
  (treiber-push-k ts (make-commit)))

(defn treiber-try-pop-k [ts k]
  (make-update ts
               (fn [[ov retv]]
                 (cond
                   (empty? ov)
                   nil ;; block

                   :else
                   [(rest ov) (first ov)]))
               k))

(defn treiber-try-pop [ts]
  (treiber-try-pop-k ts (make-commit)))

(defn pop-then-push [ts1 ts2]
  (treiber-try-pop-k ts1
                     (treiber-push ts2)))




;; A little test
(declare react)
(defmonadic a-little-test []
  (conc/print "making a ts")
  [ts (make-treiber)]
  (conc/print "pushing a value")
  (react (treiber-push ts) 99)
  (conc/print "popping a value")
  [res 
   (react (treiber-try-pop ts) nil)]
  (conc/print "got res " (pr-str res))
  )

(conc/run-cont (a-little-test) :task)

(defmonadic a-bigger-test []
  (conc/print "----------")
  (conc/print "making a ts")
  [ts1 (make-treiber)]

  (conc/print "and another")
  [ts2 (make-treiber)]

  (conc/print "pushing a value on ts1")
  (react (treiber-push ts1) 99)

  (conc/print "popping from ts1 and atomically pushing the result to ts2")
  (react (pop-then-push ts1 ts2) 99999)

  (conc/print "yippi")

  (conc/print "ts1: " (pr-str ts1))
  (conc/print "ts2: " (pr-str ts2))
  )

(conc/run-cont (a-bigger-test) :task)
