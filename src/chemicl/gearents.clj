(ns chemicl.gearents
  (:require
   [active.clojure.monad :as m]
   [active.clojure.record :as acr]
   [chemicl.message-queue :as mq]
   [chemicl.channels :as ch]
   [chemicl.post-commit :as pc]
   [chemicl.concurrency :as conc]
   [chemicl.offers :as offers]
   [chemicl.kcas :as kcas]
   [chemicl.refs :as refs]
   [chemicl.reactions :as rx]
   [chemicl.reaction-data :as rx-data]
   [chemicl.monad :as cm :refer [defmonadic whenm]]))


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

(defn return [v]
  (fn [k]
    (make-return v k)))

;; -----------------------------------------------
;; Helpers

(defn may-sync? [rea]
  (cond
    (upd? rea)
    true

    (swap? rea)
    true

    (commit? rea)
    false

    (post-commit? rea)
    (may-sync? (post-commit-k rea))

    (read? rea)
    (may-sync? (read-k rea))

    (choose? rea)
    (or (may-sync? (choose-l rea))
        (may-sync? (choose-r rea)))))

;; FIXME: loop recur
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
  [o (conc/read oref)]
  (m/return
   (offers/active? o)))

(defmonadic try-react-swap-from [k a rx oref msgs retry?]
  (if (empty? msgs)
    (m/return
     (if retry? :retry :block))
    ;; else not empty
    ;; TODO: check that it is not our own message (compare oref with sender offer)
    ;; ha, caught one
    (m/monadic
     (let [msg (first msgs)

           sender-a (message-value msg)
           sender-rx (message-sender-rx msg)
           sender-k (message-sender-k msg)
           sender-oref (message-sender-offer msg)])
     
     (let [new-rx (rx-data/rx-union rx sender-rx)
           new-rea (compose-n sender-k
                              (make-complete-offer sender-oref (make-commit))
                              (make-return sender-a (make-commit))
                              k)])
     (try-react new-rea a new-rx oref))))

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
  [mqe (mq/empty? in)]
  (if-not mqe
    (m/monadic
     [msgs (mq/snapshot in)]
     (try-react-swap-from k a rx oref msgs false))
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
  [ov (refs/read r)]
  (let [ov-data (refs/ref-data ov)
        ov-offers (refs/ref-offers ov)
        res (f [ov-data a])])
  (if res
    ;; record cas
    (m/monadic
     (let [[nv-data retv] res
           nv (refs/make-ref nv-data ov-offers)])
     (try-react k retv (-> rx
                           (rx-data/add-cas [r ov nv])
                           (rx-data/add-action
                            (refs/rescind-offers r))) oref))
    ;; else wait on ref by placing offer there and block
    (m/monadic
     (refs/add-offer r oref)
     (m/return :block))))

(defmonadic try-react-post-commit [rea a rx oref]
  (let [f (post-commit-f)
        k (post-commit-k)])
  (try-react k a (rx-data/add-action rx (f a))))

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

(defmonadic try-react-return [rea a rx oref]
  (let [v (return-value rea)
        k (return-k rea)])
  (try-react k v rx oref))

(defmonadic commit-reaction [rx a]
  [succ (rx/try-commit rx)]
  (if succ
    (m/return a)
    (m/return :retry)))

(defmonadic try-react-commit [rea a rx oref]
  (if oref
    (m/monadic
     [ores (offers/rescind oref)]
     (if ores
       (m/return ores)
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

    ;; final case
    (commit? rea)
    (try-react-commit rea a rx oref)

    ;; special case
    (complete-offer? rea)
    (try-react-complete-offer rea a rx oref)
    ))

(declare with-offer)

(defmonadic with-offer-continue [reagent a oref]
  [ores (offers/rescind oref)]
  (if ores
    ;; got an answer to return
    (m/monadic
     (m/return ores))
    ;; else retry
    (m/monadic
     (with-offer reagent a))))

(defmonadic with-offer [reagent a]
  [oref (offers/new-offer)]
  [res (try-react reagent a (rx-data/empty-rx) oref)]
  (cond
    (= :block res)
    (m/monadic
     (offers/wait oref)
     ;; when continued:
     (with-offer-continue reagent a oref))

    (= :retry res)
    ;; backoff.once
    (with-offer-continue reagent a oref)

    :else
    (m/return res)))

(defmonadic without-offer [reagent a]
  [res (try-react reagent a (rx-data/empty-rx) nil)]
  (cond
    (= :block res)
    (m/monadic
     (with-offer reagent a))

    (= :retry res)
    (m/monadic
     (if (may-sync? reagent)
       (with-offer reagent a)
       (without-offer reagent a)))

    :else
    (m/return res)))

(defmonadic react! [reagent-fn a]
  ;; add commit continuation
  (let [reagent (reagent-fn (make-commit))])
  (without-offer reagent a))


;; -----------------------------------------------
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

(defn pop-either [ts-1 ts-2]
  (choose
   (treiber-pop ts-1)
   (treiber-pop ts-2)))


(def ts-1 (atom (refs/make-ref nil [])))
(def ts-2 (atom (refs/make-ref nil [])))

(defmonadic poperoonies [ts]
  (conc/print "---- begin")
  [res (react! (treiber-pop ts) :whatever)]
  (conc/print "popper got: " (pr-str res)))

(defmonadic pusheroonies [ts v]
  (conc/print "---- begin")
  [res (react! (treiber-push ts) v)]
  (conc/print "pushher got: " (pr-str res)))

(defmonadic moveroonies [ts-1 ts-2]
  (conc/print "--- begin")
  [res (react! (>>> (treiber-pop ts-1)
                    (treiber-push ts-2)) :whatever)]
  (conc/print "mover got: " (pr-str res)))

(defmonadic popeitheroonies [ts-1 ts-2]
  (conc/print "----")
  [res (react! (pop-either ts-1 ts-2) :yea)]
  (conc/print "either got: " (pr-str res)))

(defmonadic pusheitheroonies [ts-1 ts-2]
  (conc/print "----")
  [res (react! (choose
                (upd ts-1
                     (fn [[ov retv]]
                       (if ov
                         ;; full
                         nil ;; block
                         ;; empty
                         [retv nil])))
                (upd ts-2
                     (fn [[ov retv]]
                       (if ov
                         ;; full
                         nil ;; block
                         ;; empty
                         [retv nil])))) :yea)]
  (conc/print "push either got: " (pr-str res)))

(defmonadic takeeither [ts-1 ts-2]
  (conc/print "---- taking")
  [res (react! (choose
                (upd ts-1
                     (fn [[ov retv]]
                       (if ov
                         ;; full
                         [nil ov]
                         ;; empty
                         nil ;; block
                         )))
                (upd ts-2
                     (fn [[ov retv]]
                       (if ov
                         ;; full
                         [nil ov]
                         ;; empty
                         nil ;; block
                         )))) :whataever)]
  (conc/print "take either got: " (pr-str res)))

#_(conc/run-many-to-many (pusheroonies ts-1 123123))
#_(conc/run-many-to-many (pusheroonies ts-2 :yooo))
#_(conc/run-many-to-many (poperoonies ts-1))
#_(conc/run-many-to-many (moveroonies ts-1 ts-2))
#_(conc/run-many-to-many (popeitheroonies ts-1 ts-2))
#_(conc/run-many-to-many (pusheitheroonies ts-1 ts-2))
#_(conc/run-many-to-many (takeeither ts-1 ts-2))

(defmonadic return-test []
  (conc/print "---- begin")
  [res (react! (return 123) :whatever)]
  (conc/print "got: " (pr-str res)))

(conc/run-many-to-many (return-test))

(defn printref [r]
  (println (pr-str @r)))

(printref ts-1)
(printref ts-2)





