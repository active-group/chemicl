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
   [chemicl.concurrency :as conc]
   [chemicl.monad :as cm :refer [defmonadic whenm]]
   [chemicl.maybe :as maybe :refer [maybe-case just nothing]])
  (:refer-clojure :exclude [read send nth first]))


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

(def commit (make-commit))

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

(acr/define-record-type Nth
  (make-nth a n k)
  nth?
  [a nth-arrow
   n nth-n
   k nth-k])

(acr/define-record-type CompleteOffer
  (make-complete-offer o k)
  complete-offer?
  [o complete-offer-offer
   k complete-offer-k])

(acr/define-record-type MyReagent
  (make-my-reagent try-react k)
  my-reagent?
  [try-react my-reagent-try-react
   k my-reagent-k])

;; -----------------------------------------------
;; API

(defn- m-lift1 [f]
  (fn [v]
    (m/return (f v))))

(defn upd-m [r f]
  (make-upd r f commit))

(defn upd [r f]
  (make-upd r (m-lift1 f) commit))

(defn cas [r ov nv]
  (upd r (fn [[current _]]
           (if (= current ov)
             [nv nil]
             nil))))

(defn read [r]
  (make-read r commit))

(defn swap [ep]
  (make-swap ep commit))

;; if swap is too confusing, try send and receive

(defn send [[i o]]
  (swap i))

(defn receive [[i o]]
  (swap o))

(def ^{:doc "A reagent that just blocks forever."} never
  ;; TODO: Markus probably knows a beter way to define this:
  (upd (conc/run-many-to-many! (refs/new-ref nil))
       (fn [_] nil)))

(defn choose
  ([] never)
  ([r] r)
  ([r & rs]
   (reduce make-choose r rs)))

(defn post-commit [f]
  (make-post-commit f commit))

;; return : a -> Reagent () a
(defn return [v]
  (make-return v commit))

;; computed : (a -> Monad (R () b)) -> R a b
(defn computed-m [f]
  (make-computed f commit))

;; computed : (a -> R () b) -> R a b
(defn computed [f]
  (make-computed (m-lift1 f) commit))

;; lift : (a -> Monad b) -> Reagent a b
(defn lift-m [f]
  (make-lift f commit))

;; lift : (a -> b) -> Reagent a b
(defn lift [f]
  (make-lift (m-lift1 f) commit))

(defn nth [a n]
  (make-nth a n commit))

(defn first [a]
  (nth a 0))

(declare >>>)
(declare >>)

;; *** : a b c -> a b' c' -> a (b,b') (c,c')
(defn ***
  "split"
  [& arrs]
  (apply >>>
         (map nth arrs (range))))

;; &&& : a b c -> a b c' -> a b (c,c')
(defn &&&
  "fanout"
  [& arrs]
  (>>
   (lift (fn [b] (repeat (count arrs) b)))
   (apply *** arrs)))

(def id
  (lift identity))

;; --- User-defined Reagents ---------

(defn my
  "Create a custom reagent. You must provide a function that takes three
  arguments: an input value, a context value, and a rerun thunk. The
  input value is the standard reagent input. The context is a place
  where you can store values that persists over mutliple invocations
  of your reagent function. The rerun thunk can be used to trigger a
  rerun of the reaction this reagent is involved in. The reaction
  should be in a blocked state when rerun is called (Therefore it only
  makes sense to run `rerun` on a different thread).

  The function you provide may return one of three types of
  values: :block, :retry or a map containing (some of) the
  keys :context, :result, :reaction."
  [tr]
  (make-my-reagent tr commit))

(defmacro defreagent [name args1 args2 body]
  `(defn ~name [~@args1]
     (my (fn [~@args2]
           (m/monadic
            ~body)))))

(defn my-return [& [res rx ctx]]
  (m/return
   {:type :continue
    :result res
    :reaction rx
    :context ctx}))

(defn my-block [& [ctx]]
  (m/return
   {:type :block
    :context ctx}))

(defn my-retry [& [ctx]]
  (m/return
   {:type :retry
    :context ctx}))

(declare react!)
(defn ->reagent
  "Turn a monadic action into a reagent"
  [action]
  (let [ch @(conc/run (ch/new-channel))]
    (conc/run
      [res action]
      (react! (send ch) res))

    (receive ch)))


;; ------------------------------
;; --- Sequential composition ---
;; ------------------------------

(defn- >> [rea r]
  (cond
    (upd? rea)
    (make-upd
     (upd-ref rea)
     (upd-fn rea)
     (>> (upd-k rea) r))

    (swap? rea)
    (make-swap
     (swap-endpoint rea)
     (>> (swap-k rea) r))

    (commit? rea)
    r

    (post-commit? rea)
    (make-post-commit
     (post-commit-f rea)
     (>> (post-commit-k rea) r))

    (read? rea)
    (make-read
     (read-ref rea)
     (>> (read-k rea) r))

    (choose? rea)
    (make-choose
     (>> (choose-l rea) r)
     (>> (choose-r rea) r))

    (complete-offer? rea)
    (make-complete-offer
     (complete-offer-offer rea)
     (>> (complete-offer-k rea) r))

    (return? rea)
    (make-return
     (return-value rea)
     (>> (return-k rea) r))

    (computed? rea)
    (make-computed
     (computed-function rea)
     (>> (computed-k rea) r))

    (lift? rea)
    (make-lift
     (lift-function rea)
     (>> (lift-k rea) r))

    (nth? rea)
    (make-nth
     (nth-arrow rea)
     (nth-n rea)
     (>> (nth-k rea) r))

    (my-reagent? rea)
    (make-my-reagent
     (my-reagent-try-react rea)
     (>> (my-reagent-k rea) r))

    ))

(defn >>> [& reas]
  (reduce >> reas))


;; -------------
;; --- React ---
;; -------------

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

(defmonadic try-react-swap-from [k a rx oref cursor retry? ctx]
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
           new-rea (>>> sender-k
                        (make-complete-offer sender-oref commit)
                        (make-return sender-a commit)
                        k)])

     ;; run combined reagent
     [res (try-react new-rea a new-rx oref ctx)]

     ;; handle result
     (cond
       (= :block res)
       (m/monadic
        [next-cursor (mq/cursor-next cursor)]
        (try-react-swap-from k a rx oref next-cursor retry? ctx))

       (= :retry res)
       (m/monadic
        [next-cursor (mq/cursor-next cursor)]
        (try-react-swap-from k a rx oref next-cursor true ctx))

       :else
       (m/return res))
     )))

(defmonadic try-react-swap [rea a rx oref ctx]
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
    (try-react-swap-from k a rx oref cursor false ctx)
    ;; else block
    (m/return :block)))

;; --- Shared memory

(defmonadic try-react-read [rea a rx oref ctx]
  (let [r (read-ref rea)
        k (read-k rea)])
  [v (refs/read r)]
  (try-react k v rx oref ctx))

(defmonadic try-react-upd [rea a rx oref ctx]
  (let [r (upd-ref rea)
        f (upd-fn rea)
        k (upd-k rea)])

  ;; post offer first in order to avoid lost wakeups
  (refs/add-offer r oref)

  ;; read current value
  [ov (refs/read r)]
  ;; f must be monadic:
  [res (f [ov a])]

  (if res
    ;; record cas
    (m/monadic
     (let [[nv retv] res])
     (try-react k retv (-> rx
                           (rx-data/add-cas+action [(refs/ref-data-ref r) ov nv]
                                                   (refs/rescind-offers r)))
                oref ctx))
    ;; else block
    (m/return :block)))

(defmonadic try-react-post-commit [rea a rx oref ctx]
  (let [f (post-commit-f rea)
        k (post-commit-k rea)
        act (try (f a)
                 (catch Exception e
                   (println "Exception in post-commit:" (pr-str e))
                   (throw e)))])
  (try-react k a (rx-data/add-action rx act) oref ctx))

(defmonadic try-react-choose [rea a rx oref ctx]
  (let [l (choose-l rea)
        r (choose-r rea)])
  ;; try left
  [lres (try-react l a rx oref ctx)]
  (if (= :block lres)
    ;; try right
    (try-react r a rx oref ctx)
    ;; else return res
    (m/return lres)))

;; --- Misc

(defmonadic try-react-return [rea a rx oref ctx]
  (let [v (return-value rea)
        k (return-k rea)])
  (try-react k v rx oref ctx))

(defmonadic try-react-computed [rea a rx oref ctx]
  (let [f (computed-function rea)
        k (computed-k rea)])

  ;; f must be monadic, and return a reagent
  [res (f a)]

  (try-react (>> res k) nil rx oref ctx))

(defmonadic try-react-lift [rea a rx oref ctx]
  (let [f (lift-function rea)
        k (lift-k rea)])

  ;; f must be monadic
  [res (f a)]

  (try-react k res rx oref ctx))

(defmonadic try-react-nth [rea a rx oref ctx]
  (let [arr (nth-arrow rea)
        n (nth-n rea)
        k (nth-k rea)])
  (try-react
   (>>> arr
        (lift
         (fn [c]
           (assoc (vec a) n c)))
        k)
   (clojure.core/nth a n) rx oref ctx))

(defmonadic commit-reaction [rx a]
  [succ (rx/try-commit rx)]
  (if succ
    (m/return a)
    (m/return :retry)))

(defmonadic try-react-commit [rea a rx oref ctx]
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

(defmonadic try-react-complete-offer [rea a rx my-oref ctx]
  (let [other-oref (complete-offer-offer rea)
        k (complete-offer-k rea)])
  [offer-rx (offers/complete other-oref a)]
  (try-react k a (rx-data/rx-union rx offer-rx) my-oref ctx))

(defmonadic try-react-my-reagent [rea a rx oref ctx]
  (let [tr (my-reagent-try-react rea)
        k (my-reagent-k rea)
        narrow-ctx (get @ctx rea)])

  ;; run user-defined try-react
  [{:keys [type result context reaction]}
   (tr a narrow-ctx (offers/rescind oref))]

  ;; set new context
  (let [_ (when context
            (swap! ctx assoc rea context))])

  (case type
    :block
    (m/return :block)

    :retry
    (m/return :retry)

    :continue
    (try-react k result
               (rx-data/rx-union rx reaction)
               oref ctx)))

(defn try-react [rea a rx oref ctx]
  (cond
    (read? rea)
    (try-react-read rea a rx oref ctx)

    (upd? rea)
    (try-react-upd rea a rx oref ctx)

    (swap? rea)
    (try-react-swap rea a rx oref ctx)

    (post-commit? rea)
    (try-react-post-commit rea a rx oref ctx)

    (choose? rea)
    (try-react-choose rea a rx oref ctx)

    (return? rea)
    (try-react-return rea a rx oref ctx)

    (computed? rea)
    (try-react-computed rea a rx oref ctx)

    (lift? rea)
    (try-react-lift rea a rx oref ctx)

    (nth? rea)
    (try-react-nth rea a rx oref ctx)

    (my-reagent? rea)
    (try-react-my-reagent rea a rx oref ctx)

    ;; final case
    (commit? rea)
    (try-react-commit rea a rx oref ctx)

    ;; special case
    (complete-offer? rea)
    (try-react-complete-offer rea a rx oref ctx)
    ))

(declare with-offer)

(defmonadic with-offer-continue [reagent a oref backoff-counter ctx]
  [ores (offers/rescind oref)]
  (maybe-case ores
    ;; got an answer to return
    (just res)
    (m/return res)

    ;; else retry
    (nothing)
    (with-offer reagent a backoff-counter ctx)))

(defmonadic with-offer [reagent a backoff-counter ctx]
  [oref (offers/new-offer)]
  [res (try-react reagent a rx-data/empty-rx oref ctx)]
  (cond
    (= :block res)
    (m/monadic
     (offers/wait oref)
     ;; when continued:
     (with-offer-continue reagent a oref backoff-counter ctx))

    (= :retry res)
    ;; backoff.once
    (m/monadic
     (backoff/timeout-with-counter backoff-counter)
     (with-offer-continue reagent a oref (inc backoff-counter) ctx))

    :else
    (m/return res)))

#_(defmonadic without-offer [reagent a backoff-counter]
  [res (try-react reagent a rx-data/empty-rx nil)]
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

(defmonadic react! [reagent & [a]]
  (with-offer reagent a 0 (atom nil)))
