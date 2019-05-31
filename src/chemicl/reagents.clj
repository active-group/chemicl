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

(defprotocol IReagent
  (try-react [rea a rx oref ctx])
  (>> [rea r]))

(declare >>>)
(declare commit)
(declare return)
(declare lift)

(def BLOCK ::block)
(def RETRY ::retry)

;; -----------------------------------------------
;; Internal building blocks

(defrecord Update [r f k]
  IReagent
  (>> [rea r]
    (update rea :k >> r))
  (try-react [rea a rx oref ctx]
    (m/monadic
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
       (m/return BLOCK)))))

;; --- Shared memory

(defrecord Read [r k]
  IReagent
  (>> [rea r]
    (update rea :k >> r))
  (try-react [rea a rx oref ctx]
    (m/monadic
     [v (refs/read r)]
     (try-react k v rx oref ctx))))

;; --- Message passing

(defrecord CompleteOffer [offer k]
  IReagent
  (>> [rea r]
    (update rea :k >> r))
  (try-react [rea a rx my-oref ctx]
    (m/monadic
     (let [other-oref offer])
     [offer-rx (offers/complete other-oref a)]
     (try-react k a (rx-data/rx-union rx offer-rx) my-oref ctx))))

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
     (if retry? RETRY BLOCK))
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
                        (CompleteOffer. sender-oref commit)
                        (return sender-a)
                        k)])

     ;; run combined reagent
     [res (try-react new-rea a new-rx oref ctx)]

     ;; handle result
     (cond
       (= BLOCK res)
       (m/monadic
        [next-cursor (mq/cursor-next cursor)]
        (try-react-swap-from k a rx oref next-cursor retry? ctx))

       (= RETRY res)
       (m/monadic
        [next-cursor (mq/cursor-next cursor)]
        (try-react-swap-from k a rx oref next-cursor true ctx))

       :else
       (m/return res))
     )))

(defrecord Swap [ep k]
  IReagent
  (>> [rea r]
    (update rea :k >> r))
  (try-react [rea a rx oref ctx]
    (m/monadic 
     (let [;; our read end:
           in (ch/endpoint-incoming ep)
           ;; our write end:
           out (ch/endpoint-outgoing ep)])

     ;; push offer (if any)
     (whenm oref
       (mq/push out (make-message a rx k oref) message-is-active?))

     ;; clean the in queue
     (mq/clean in message-is-active?)

     ;; finally: search for matching messages
     [cursor (mq/cursor in)]
     (if cursor
       (try-react-swap-from k a rx oref cursor false ctx)
       ;; else block
       (m/return BLOCK)))))

(defrecord Choose [l r]
  IReagent
  (>> [rea more]
    (assoc rea
           :l (>> l more)
           :r (>> r more)))
  (try-react [rea a rx oref ctx]
    (m/monadic
     ;; try left
     [lres (try-react l a rx oref ctx)]
     (if (= BLOCK lres)
       ;; try right
       (try-react r a rx oref ctx)
       ;; else return res
       (m/return lres)))))

(defrecord PostCommit [f k]
  IReagent
  (>> [rea r]
    (update rea :k >> r))
  (try-react [rea a rx oref ctx]
    (m/monadic
     (let [act (try (f a)
                    (catch Exception e
                      (println "Exception in post-commit:" (pr-str e))
                      (throw e)))])
     (try-react k a (rx-data/add-action rx act) oref ctx))))

(defmonadic commit-reaction [rx a]
  [succ (rx/try-commit rx)]
  (if succ
    (m/return a)
    (m/return RETRY)))

(defrecord Commit []
  IReagent
  (>> [rea r]
    r)
  (try-react [rea a rx oref ctx]
    (if oref
      (m/monadic
       [ores (offers/rescind oref)]
       (maybe-case ores
                   (just res)
                   (m/return res)

                   (nothing)
                   (commit-reaction rx a)))
      ;; else
      (commit-reaction rx a))))

(def commit (Commit.))

;; --- Misc

(defrecord Return [v k]
  IReagent
  (>> [rea r]
    (update rea :k >> r))
  (try-react [rea a rx oref ctx]
    (try-react k v rx oref ctx)))

(defrecord Computed [f k]
  IReagent
  (>> [rea r]
    (update rea :k >> r))
  (try-react [rea a rx oref ctx]
    (m/monadic
     ;; f must be monadic, and return a reagent
     [res (f a)]

     (try-react (>> res k) nil rx oref ctx))))

(defrecord Lift [f k]
  IReagent
  (>> [rea r]
    (update rea :k >> r))
  (try-react [rea a rx oref ctx]
    (m/monadic
     ;; f must be monadic
     [res (f a)]

     (try-react k res rx oref ctx))))

(defrecord Nth [arr n k]
  IReagent
  (>> [rea r]
    (update rea :k >> r))
  (try-react [rea a rx oref ctx]
    (try-react
     (>>> arr
          (lift
           (fn [c]
             (assoc (vec a) n c)))
          k)
     (clojure.core/nth a n) rx oref ctx)))

(defrecord MyReagent [tr k]
  IReagent
  (>> [rea r]
    (update rea :k >> r))
  (try-react [rea a rx oref ctx]
    (m/monadic
     (let [narrow-ctx (get @ctx rea)])

     ;; run user-defined try-react
     [{:keys [type result context reaction]}
      (tr a narrow-ctx (offers/rescind oref))]

     ;; set new context
     (let [_ (when context
               (swap! ctx assoc rea context))])

     (case type
       :block
       (m/return BLOCK)

       :retry
       (m/return RETRY)

       :continue
       (try-react k result
                  (rx-data/rx-union rx reaction)
                  oref ctx)))))

;; -----------------------------------------------
;; API

(defn- m-lift1 [f]
  (fn [v]
    (m/return (f v))))

(defn upd-m [r f]
  (Update. r f commit))

(defn upd [r f]
  (upd-m r (m-lift1 f)))

(defn cas [r ov nv]
  (upd r (fn [[current _]]
           (if (= current ov)
             [nv nil]
             nil))))

(defn read [r]
  (Read. r commit))

(defn swap [ep]
  (Swap. ep commit))

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
   (reduce ->Choose r rs)))

(defn post-commit [f]
  (PostCommit. f commit))

;; return : a -> Reagent () a
(defn return [v]
  (Return. v commit))

;; computed : (a -> Monad (R () b)) -> R a b
(defn computed-m [f]
  (Computed. f commit))

;; computed : (a -> R () b) -> R a b
(defn computed [f]
  (computed-m (m-lift1 f)))

;; lift : (a -> Monad b) -> Reagent a b
(defn lift-m [f]
  (Lift. f commit))

;; lift : (a -> b) -> Reagent a b
(defn lift [f]
  (lift-m (m-lift1 f)))

(defn nth [a n]
  (Nth. a n commit))

(defn first [a]
  (nth a 0))

(declare >>>)

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
  (MyReagent. tr commit))

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

(defn >>> [& reas]
  (reduce >> reas))


;; -------------
;; --- React ---
;; -------------


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
    (= BLOCK res)
    (m/monadic
     (offers/wait oref)
     ;; when continued:
     (with-offer-continue reagent a oref backoff-counter ctx))

    (= RETRY res)
    ;; backoff.once
    (m/monadic
     (backoff/timeout-with-counter backoff-counter)
     (with-offer-continue reagent a oref (inc backoff-counter) ctx))

    :else
    (m/return res)))

(defmonadic without-offer [reagent a backoff-counter ctx]
  [res (try-react reagent a rx-data/empty-rx nil ctx)]
  (cond
    (= BLOCK res)
    (m/monadic
     (with-offer reagent a backoff-counter ctx))

    (= RETRY res)
    (m/monadic
     (backoff/timeout-with-counter backoff-counter)
     (without-offer reagent a (inc backoff-counter) ctx))

    :else
    (m/return res)))

(defmonadic react! [reagent & [a]]
  (without-offer reagent a 0 (atom nil)))
