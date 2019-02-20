(ns chemicl.reagents
  (:require
   [active.clojure.monad :as m]
   [active.clojure.record :as acr]
   [chemicl.message-queue :as mq]
   [chemicl.channels :as ch]
   [chemicl.post-commit :as pc]
   [chemicl.offers :as offers]
   [chemicl.kcas :as kcas]
   [chemicl.refs :as refs]
   [chemicl.reactions :as rx]
   [chemicl.reaction-data :as rx-data]
   [chemicl.backoff :as backoff]
   [chemicl.monad :as cm :refer [defmonadic whenm]]
   [chemicl.maybe :as maybe :refer [maybe-case just nothing]]))


;; -----------------------------------------------
;; Internal building blocks

(acr/define-record-type Update
  (make-upd r f k)
  upd?
  [r upd-ref
   f upd-fn
   k upd-k])

(acr/define-record-type Read
  (make-read r k)
  read?
  [r read-ref
   k read-k])

(acr/define-record-type Swap
  (make-swap ep k)
  swap?
  [ep swap-endpoint
   k swap-k])

(acr/define-record-type Choose
  (make-choose l r)
  choose?
  [l choose-l
   r choose-r])

(acr/define-record-type PostCommit
  (make-post-commit f k)
  post-commit?
  [f post-commit-f
   k post-commit-k])

(acr/define-record-type Commit
  (make-commit)
  commit?
  [])

(acr/define-record-type Return
  (make-return v k)
  return?
  [v return-value
   k return-k])

(acr/define-record-type Computed
  (make-computed f k)
  computed?
  [f computed-function
   k computed-k])

(acr/define-record-type Lift
  (make-lift f k)
  lift?
  [f lift-function
   k lift-k])

(acr/define-record-type CompleteOffer
  (make-complete-offer o k)
  complete-offer?
  [o complete-offer-offer
   k complete-offer-k])

;; -----------------------------------------------
;; API

(defn >>> [l r]
  (fn [k]
    (l (r k))))

(defn upd [r f]
  (fn [k]
    (make-upd r f k)))

(defn cas [r ov nv]
  (upd r (fn [[current _]]
           (if (= current ov)
             [nv nil]
             nil))))

(defn read [r]
  (fn [k]
    (make-read r k)))

(defn swap [ep]
  (fn [k]
    (make-swap ep k)))

(defn choose [l r]
  (fn [k]
    (make-choose (l k) (r k))))

(defn post-commit [f]
  (fn [k]
    (make-post-commit f k)))

;; return : a -> Reagent () a
(defn return [v]
  (fn [k]
    (make-return v k)))

;; computed : (a -> R () b) -> R a b
(defn computed [f]
  (fn [k]
    (make-computed f k)))

;; lift : (a -> b) -> Reagent a b
(defn lift [f]
  (fn [k]
    (make-lift f k)))

;; -----------------------------------------------
;; Helpers

(defn- compose [rea r]
  (cond
    (upd? rea)
    (make-upd
     (upd-ref rea)
     (upd-fn rea)
     (compose (upd-k rea) r))

    (swap? rea)
    (make-swap
     (swap-endpoint rea)
     (compose (swap-k rea) r))

    (commit? rea)
    r

    (post-commit? rea)
    (make-post-commit
     (post-commit-f rea)
     (compose (post-commit-k rea) r))

    (read? rea)
    (make-read
     (read-ref rea)
     (compose (read-k rea) r))

    (choose? rea)
    (make-choose
     (compose (choose-l rea) r)
     (compose (choose-r rea) r))

    (complete-offer? rea)
    (make-complete-offer
     (complete-offer-offer rea)
     (compose (complete-offer-k rea) r))

    (return? rea)
    (make-return
     (return-value rea)
     (compose (return-k rea) r))

    (computed? rea)
    (make-computed
     (computed-function rea)
     (compose (computed-k rea) r))

    (lift? rea)
    (make-lift
     (lift-function rea)
     (compose (lift-k rea) r))

    ))

(defn compose-n [rea & reas]
  (reduce compose rea reas))


;; -----------------------------------------------

(declare try-react)

;; --- Message passing

(acr/define-record-type Message
  (make-message v rx k o)
  message?
  [v message-value
   rx message-sender-rx
   k message-sender-k
   o message-sender-offer])

(defmonadic message-is-active? [m]
  (let [oref (message-sender-offer m)])
  [o (kcas/read oref)]
  (m/return
   (offers/active? o)))

(defmonadic try-react-swap-from [k a rx oref cursor retry?]
  (let [msg (and cursor (mq/cursor-value cursor))])
  (if-not msg
    (m/return
     (if retry? :retry :block))
    ;; else not empty
    ;; TODO: check that it is not our own message (compare oref with sender offer)
    ;; ha, caught one
    (m/monadic
     (let [sender-a (message-value msg)
           sender-rx (message-sender-rx msg)
           sender-k (message-sender-k msg)
           sender-oref (message-sender-offer msg)])
     
     (let [new-rx (rx-data/rx-union rx sender-rx)
           new-rea (compose-n sender-k
                              (make-complete-offer sender-oref (make-commit))
                              (make-return sender-a (make-commit))
                              k)])

     ;; run combined reagent
     [res (try-react new-rea a new-rx oref)]

     ;; handle result
     (cond
       (= :block res)
       (m/monadic
        [next-cursor (mq/cursor-next cursor)]
        (try-react-swap-from k a rx oref next-cursor retry?))

       (= :retry res)
       (m/monadic
        [next-cursor (mq/cursor-next cursor)]
        (try-react-swap-from k a rx oref next-cursor true))

       :else
       (m/return res))
     )))

(defmonadic try-react-swap [rea a rx oref]
  (let [ep (swap-endpoint rea)
        k (swap-k rea)
        ;; our read end:
        in (ch/endpoint-incoming ep)
        ;; our write end:
        out (ch/endpoint-outgoing ep)])

  ;; push offer (if any)
  (whenm oref
    (mq/push out (make-message a rx k oref)))

  ;; clean the in queue
  (mq/clean in message-is-active?)

  ;; finally: search for matching messages
  [cursor (mq/cursor in)]
  (if cursor
    (try-react-swap-from k a rx oref cursor false)
    ;; else block
    (m/return :block)))

;; --- Shared memory

(defmonadic try-react-read [rea a rx oref]
  (let [r (read-ref rea)
        k (read-k rea)])
  [v (refs/read r)]
  (try-react k v rx oref))

(defmonadic try-react-upd [rea a rx oref]
  (let [r (upd-ref rea)
        f (upd-fn rea)
        k (upd-k rea)])

  ;; post offer first in order to avoid lost wakeups
  (refs/add-offer r oref)

  ;; read current value
  [ov (refs/read r)]
  (let [res (f [ov a])])

  ;; res might be a monadic program
  [resres (cm/maybe-unwrap-monadic res)]

  (if resres
    ;; record cas
    (m/monadic
     (let [[nv retv] resres])
     (try-react k retv (-> rx
                           (rx-data/add-cas
                            [(refs/ref-data-ref r) ov nv])
                           (rx-data/add-action
                            (refs/rescind-offers r))) oref))
    ;; else block
    (m/return :block)))

(defmonadic try-react-post-commit [rea a rx oref]
  (let [f (post-commit-f rea)
        k (post-commit-k rea)])
  (try-react k a (rx-data/add-action rx (f a)) oref))

(defmonadic try-react-choose [rea a rx oref]
  (let [l (choose-l rea)
        r (choose-r rea)])
  ;; try left
  [lres (try-react l a rx oref)]
  (if (= :block lres)
    ;; try right
    (try-react r a rx oref)
    ;; else return res
    (m/return lres)))

;; --- Misc

(defmonadic try-react-return [rea a rx oref]
  (let [v (return-value rea)
        k (return-k rea)])
  (try-react k v rx oref))

(defmonadic try-react-computed [rea a rx oref]
  (let [f (computed-function rea)
        k (computed-k rea)])
  (let [res (f a)])

  ;; res might be a monadic program
  [resres (cm/maybe-unwrap-monadic res)]

  ;; resres is a function k -> reagent
  (try-react (resres k) nil rx oref))

(defmonadic try-react-lift [rea a rx oref]
  (let [f (lift-function rea)
        k (lift-k rea)])
  (try-react k (f a) rx oref))

(defmonadic commit-reaction [rx a]
  [succ (rx/try-commit rx)]
  (if succ
    (m/return a)
    (m/return :retry)))

(defmonadic try-react-commit [rea a rx oref]
  (if oref
    (m/monadic
     [ores (offers/rescind oref)]
     (maybe-case ores
       (just res)
       (m/return res)

       (nothing)
       (commit-reaction rx a)))
    ;; else
    (commit-reaction rx a)))

(defmonadic try-react-complete-offer [rea a rx my-oref]
  (let [other-oref (complete-offer-offer rea)
        k (complete-offer-k rea)])
  [offer-rx (offers/complete other-oref a)]
  (try-react k a (rx-data/rx-union rx offer-rx) my-oref))

(defn try-react [rea a rx oref]
  (cond
    (read? rea)
    (try-react-read rea a rx oref)

    (upd? rea)
    (try-react-upd rea a rx oref)

    (swap? rea)
    (try-react-swap rea a rx oref)

    (post-commit? rea)
    (try-react-post-commit rea a rx oref)

    (choose? rea)
    (try-react-choose rea a rx oref)

    (return? rea)
    (try-react-return rea a rx oref)

    (computed? rea)
    (try-react-computed rea a rx oref)

    (lift? rea)
    (try-react-lift rea a rx oref)

    ;; final case
    (commit? rea)
    (try-react-commit rea a rx oref)

    ;; special case
    (complete-offer? rea)
    (try-react-complete-offer rea a rx oref)
    ))

(declare with-offer)

(defmonadic with-offer-continue [reagent a oref backoff-counter]
  [ores (offers/rescind oref)]
  (maybe-case ores
    ;; got an answer to return
    (just res)
    (m/return res)

    ;; else retry
    (nothing)
    (with-offer reagent a backoff-counter)))

(defmonadic with-offer [reagent a backoff-counter]
  [oref (offers/new-offer)]
  [res (try-react reagent a (rx-data/empty-rx) oref)]
  (cond
    (= :block res)
    (m/monadic
     (offers/wait oref)
     ;; when continued:
     (with-offer-continue reagent a oref backoff-counter))

    (= :retry res)
    ;; backoff.once
    (m/monadic
     (backoff/timeout-with-counter backoff-counter)
     (with-offer-continue reagent a oref (inc backoff-counter)))

    :else
    (m/return res)))

(defmonadic without-offer [reagent a backoff-counter]
  [res (try-react reagent a (rx-data/empty-rx) nil)]
  (cond
    (= :block res)
    (m/monadic
     (with-offer reagent a backoff-counter))

    (= :retry res)
    (m/monadic
     (backoff/timeout-with-counter backoff-counter)
     (without-offer reagent a (inc backoff-counter)))

    :else
    (m/return res)))

(defmonadic react! [reagent-fn a]
  ;; add commit continuation
  (let [reagent (reagent-fn (make-commit))])
  (without-offer reagent a 0))
