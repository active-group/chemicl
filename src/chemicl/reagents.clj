(ns chemicl.reagents
  (:require
   [chemicl.monad :as cm :refer [defmonadic whenm]]
   [chemicl.concurrency :as conc]
   [chemicl.kcas :as kcas]
   [chemicl.reactions :as rx]
   [chemicl.refs :as refs]
   [chemicl.offers :as offers]
   [chemicl.channels :as ch]
   [active.clojure.record :as acr]
   [active.clojure.lens :as lens]
   [active.clojure.monad :as m]))

(defmacro >>> [& rs]
  (if (= 1 (count rs))
    `(fn [k#]
       (~@rs k#))
    ;; else
    (let [[[& a1] & rest] rs]
      `(fn [k#]
         ((~@a1) ((>>> ~@rest) k#))))))

#_(macroexpand-1 '(>>> (read :ref)
                     (read :rof)))



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

(defmacro read [r]
  `(fn [k#]
     (make-read ~r k#)))

(acr/define-record-type CAS
  (make-cas ref ov nv k)
  cas?
  [ref cas-ref
   ov cas-old-value
   nv cas-new-value
   k cas-continuation])

(defmacro cas [r ov nv]
  `(fn [k#]
     (make-cas ~r ~ov ~nv k#)))

;; 'a ref -> ('a -> 'b -> ('a * 'c) option) -> ('c,'r) reagent -> ('b,'r) reagent
(acr/define-record-type Update
  (make-update ref updf k)
  update?
  [ref update-ref
   updf update-function
   k update-continuation])

(defmacro upd [r f]
  `(fn [k#]
     (make-update ~r ~f k#)))

(acr/define-record-type Choice
  (make-choice r1 r2)
  choice?
  [r1 choice-left
   r2 choice-right])

(defmacro choice [r1 r2]
  `(fn [k#]
     (make-choice (~r1 k#)
                  (~r2 k#))))

(acr/define-record-type Computed
  (make-computed c)
  computed?
  [c computed-computation])

(defmacro computed [c]
  `(make-computed ~c))

(acr/define-record-type Swap
  (make-swap ep k)
  swap?
  [ep swap-endpoint
   k swap-continuation])

(defmacro swap [ep]
  `(fn [k#]
     (make-swap ~ep k#)))

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




;; --- Reacting ---------

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
     [succ (offers/rescind-offer offer)]
     (whenm (not succ)
       (offers/get-answer offer)))

    (not offer)
    (m/monadic
     [succ (rx/try-commit rx)]
     (conc/print "succ in try-react-commit" (pr-str succ))
     (if succ
       (m/return a)
       ;; else retry
       (m/return :retry)))))

#_(defn try-react-read [reagent a rx offer]
  (let [r (read-ref reagent)
        k (read-continuation reagent)]
    (m/monadic
     (m/return
      (when offer
        (refs/put-offer r offer)))
     (let [v (refs/read-data r)])
     (try-react k v rx offer))))

#_(defn try-react-cas [reagent a rx offer]
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
  (println "<try-react-update>")
  (println (pr-str reagent))
  (println (pr-str a))
  (println (pr-str rx))
  (println (pr-str offer))
  (println "</try-react-update>")
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
         (do (println "yes res")
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
                  (m/return :retry)))))
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
          (try-react k retv
                     (rx/add-cas+post-commit rx r ov nv (refs/wake-all-waiters r))
                     offer))
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

#_(defn message-exchange
  "Return a reagent computation that performs the message exchange"
  [msg k]
  (let [p (ch/message-payload msg)
        srx (ch/message-sender-reaction msg)
        sk (ch/message-sender-continuation msg)
        so (ch/message-sender-offer msg)]

    ))

#_(defmonadic try-react-swap-from [k a rx o msgs fail-mode]
  (if (empty? msgs)
    (m/return fail-mode)
    ;; else
    (m/monadic
     (let [[msg & rest] msgs])
     [res (try-react (message-exchange msg k) a rx o)]
     (case res
       :retry
       (try-react-swap-from k a rx o rest :retry)

       :block
       (try-react-swap-from k a rx o rest fail-mode)

       ;; else
       (m/return res)))))

#_(defn try-react-swap [reagent a rx o]
  (let [ep (swap-endpoint reagent)
        k (swap-continuation reagent)]
    (m/monadic
     (whenm o
       ;; post the offer to the endpoint
       (m/monadic
        (let [msg (ch/make-message a rx k o)])
        (ch/endpoint-put ep msg)))

     ;; Take a snapshot of the dual messages
     [msgs (ch/endpoint-messages (ch/endpoint-dual ep))]
     ;; fold over msgs
     (try-react-swap-from k a rx o msgs :retry))
    ))

(defn try-react [reagent a rx offer]
  (cond
    (commit? reagent)
    (try-react-commit reagent a rx offer)

    #_(read? reagent)
    #_(try-react-read reagent a rx offer)

    #_(cas? reagent)
    #_(try-react-cas reagent a rx offer)

    (update? reagent)
    (try-react-update reagent a rx offer)
(choice? reagent)
    (try-react-choice reagent a rx offer)

    #_(swap? reagent)
    #_(try-react-swap reagent a rx offer)))



;; Helpers

(defn may-sync? [reagent]
  true)



;; Outer react protocol

(defn with-offer [reagent a]
  (m/monadic
   (conc/print "with-offer")
   [offer (offers/new-offer)]
   [res (try-react reagent a (rx/empty-reaction) offer)]
   (case res
     :block
     (m/monadic
      (conc/park)
      [succ (offers/rescind-offer offer)]
      (conc/print "succ after block: " (pr-str succ))
      (if succ
        (with-offer reagent a)
        (offers/get-answer offer)))

     :retry
     (m/monadic
      [succ (offers/rescind-offer offer)]
      (if succ
        (with-offer reagent a)
        (offers/answer-offer offer)))

     (m/return res) 
     )))

(defn without-offer [reagent a]
  (m/monadic
   (conc/print "without-offer")
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

;; Reagent A B x A -> B
;; aka run
;; reagent is a fn k -> reagent data structure
(defn react [reagent a]
  ;; final continuation is a commit
  (let [rea (reagent (make-commit))]
    (without-offer rea a)))


;; -----------------------------------------------
;; Another try

(acr/define-record-type SequentialComposition
  (>>>> l r)
  >>>>?
  [l >>>>-l
   r >>>>-r])

(acr/define-record-type Update
  (upd r f)
  upd?
  [r upd-ref
   f upd-fn])

;; reactions

(defn rx-union [rx-1 rx-2]
  {:tag ::reaction
   :cases (concat (:cases rx-1)
                  (:cases rx-2))})

(defn rx-cases [rx]
  (sort-by (fn [[r _ _]]
             (hash r))
           (:cases rx)))

(defmonadic rx-commit [rx]
  (conc/print "committing" (pr-str rx))
  (let [cases (rx-cases rx)])
  (cond
    (empty? cases)
    (m/return true)

    (= 1 (count cases))
    (let [[r ov nv] (first cases)]
      (refs/cas-data r ov nv))

    :else
    (m/monadic
     [succ (kcas/kcas cases)]
     (whenm (not succ)
       ;; backoff
       (rx-commit rx))))

  ;; wake all waiters
  (let [rs (map first cases)])
  (m/sequ_ (map refs/wake-waiters rs))
  )

(defn rx-from-cas [cas]
  {:tag ::reaction
   :cases [cas]})

;; reagent -> reaction

(defmonadic reagent->reaction-upd [rea a]
  (let [r (upd-ref rea)
        f (upd-fn rea)])
  [ov (refs/read-data r)]
  (let [res (f [ov a])])
  (if res
    ;; record cas
    (m/monadic
     (let [[nv retv] res])
     (m/return [(rx-from-cas [r ov nv])
                retv]))
    ;; else wait on ref and block
    (m/monadic
     [me (conc/get-current-task)]
     (refs/add-waiter r me)
     (m/return :block))))

(defmonadic reagent->reaction-seq-comp [rea a]
  (let [l (>>>>-l rea)
        r (>>>>-r rea)
        [lrx lres] (reagent->reaction l a)
        [rrx rres] (reagent->reaction r lres)])
  (m/return [(rx-union lrx rrx) rres]))

(defn reagent->reaction [rea a]
  (cond
    (>>>>? rea)
    (reagent->reaction-seq-comp rea a)

    (upd? rea)
    (reagent->reaction-upd rea a)))

(defmonadic do-react! [rea a]
  [res (reagent->reaction rea a)]
  (cond
    (= :block res)
    (m/monadic
     (conc/print "Parking: " (pr-str rea))
     (conc/park)
     (do-react! rea a))

    :else
    (m/return res)))

(defmonadic react! [reagent a]
  ;; phase 1 (may block)
  [[rx res] (do-react! reagent a)]
  ;; phase 2 (may retry)
  (rx-commit rx)
  (m/return res))


;; playground

(defn treiber-push [ts]
  (upd ts
       (fn [[ov retv]]
         [(conj ov retv) nil])))

(defn treiber-pop [ts]
  (upd ts
       (fn [[ov retv]]
         (cond
           (empty? ov)
           nil ;; block

           :else
           [(rest ov) (first ov)]))))


(def dataref-1 (atom '()))
(def waitersref-1 (atom []))
(def ts-1 (refs/make-ref dataref-1 waitersref-1))

(def dataref-2 (atom '()))
(def waitersref-2 (atom []))
(def ts-2 (refs/make-ref dataref-2 waitersref-2))

(defmonadic poperoonies [ts]
  (conc/print "---- begin")
  [res (react! (treiber-pop ts) :whatever)]
  (conc/print "popper got: " (pr-str res)))

(defmonadic pusheroonies [ts]
  (conc/print "---- begin")
  [res (react! (treiber-push ts) 1337)]
  (conc/print "pushher got: " (pr-str res)))

(defmonadic moveroonies [ts-1 ts-2]
  (conc/print "--- begin")
  [res (react! (>>>> (treiber-pop ts-1)
                     (treiber-push ts-2)) :whatever)]
  (conc/print "mover got: " (pr-str res)))

(conc/run-many-to-many (pusheroonies ts-2))
(conc/run-many-to-many (poperoonies ts-2))
(conc/run-many-to-many (moveroonies ts-1 ts-2))

(printref ts-1)
(printref ts-2)








;; --- Playground ---------

(defmonadic make-treiber []
  (refs/new-ref '()))

(defn treiber-push [ts]
  (update ts
          (fn [[ov retv]]
              [(conj ov retv) nil])))

(defn treiber-try-pop [ts]
  (update ts
          (fn [[ov retv]]
                 (cond
                   (empty? ov)
                   nil ;; block

                   :else
                   [(rest ov) (first ov)]))))

(defn pop-then-push [ts1 ts2]
  (>>> (treiber-try-pop ts1)
       (treiber-push ts2)))




;; A little test
(def dataref-1 (atom '()))
(def waitersref-1 (atom []))
(def ts-1 (refs/make-ref dataref-1 waitersref-1))

(def dataref-2 (atom '()))
(def waitersref-2 (atom []))
(def ts-2 (refs/make-ref dataref-2 waitersref-2))

(declare react)




(defmonadic pusheroonies [ts]
  (conc/print "---- begin")
  [res (react (treiber-push ts) :whatever)]
  (conc/print "pushher got: " (pr-str res)))

(defmonadic poperoonies [ts]
  (conc/print "---- begin")
  [res (react (treiber-try-pop ts) :whatever)]
  (conc/print "popper got: " (pr-str res)))

(defmonadic moveroonies []
  (conc/print "--- begin")
  [res (react (>>> (treiber-try-pop ts-1)
                   (treiber-push ts-2)) :whatever)]
  (conc/print "got: " (pr-str res))
  )

(defn printref [r]
  (println (pr-str @(refs/ref-data r)))
  (println (pr-str @(refs/ref-waiters r))))

(printref ts-1)
(printref ts-2)

(conc/run-many-to-many (pusheroonies ts-2))
(conc/run-many-to-many (poperoonies ts-2))
#_(conc/run-many-to-many (moveroonies))


