(ns chemicl.cml
  (:require [chemicl.reagents :as r]
            [chemicl.channels :as ch]
            [chemicl.refs :as refs]
            [chemicl.timeout :as tmo]
            [chemicl.concurrency :as c]
            [active.clojure.monad :as monad])
  (:refer-clojure :exclude [send sync resolve]))

(defprotocol Event
  (resolve [this] "Is called at 'synchronization time', and must
  return a tuple `[reagent abort]`, where abort is nil, or a function
  of no arguments, that is called when this event is not selected in
  a [[choose]], and must return an action in the concurrency
  monad. That action is scheduled on a new thread after the
  synchronization."))

(defn event?
  "Returns wheather v is an event."
  [v]
  (satisfies? Event v))

(defn- comp-abort [& aborts]
  (reduce (fn [res ab]
            (if (some? ab)
              (if (some? res)
                (fn []
                  (monad/sequ_ (list (res) (ab))))
                ab)
              res))
          nil
          aborts))

(defn- demonad [m] ;; TODO: this should not be needed.
  (c/run-many-to-many! m))

;; the primitives

(defrecord ^:private Never
  []
  Event (resolve [_]
          [r/never nil]))

(defrecord ^:private Always
  [v]
  Event (resolve [_]
          [(r/return v) nil]))

(defrecord ^:private Send
  [ch v]
  Event (resolve [_]
          [(r/>>> (r/return v) (r/send ch)) nil]))

(defrecord ^:private Receive
  [ch]
  Event (resolve [_]
          [(r/receive ch) nil]))

(defrecord ^:private Wrap
  [ev f args]
  Event (resolve [_]
          (let [[r abort] (resolve ev)]
            [(r/>>> r (r/lift (fn [v] (apply f v args))))
             abort])))

(defrecord ^:private WrapAbort
  [ev f args]
  Event (resolve [_]
          (let [[reagent a] (resolve ev)
                ab (fn [] ;; TODO fork should be this.
                     (c/fork (apply f args)))]
            [reagent (comp-abort a ab)])))

(defrecord ^:private Guard
  [f args]
  Event (resolve [_]
          (let [ev (apply f args)]
            (resolve ev))))

(defrecord ^:private Timeout
  [ms]
  Event (resolve [_]
          [(tmo/timeout ms) nil]))

;; the api

(def ^{:doc "An event that will never become available."}
  never
  (Never.))

(defn always
  "Returns an event that is available immediately, yielding the given
  value."
  [v]
  (Always. v))

(defn channel
  "Returns a new synchronous channel."
  []
  (demonad (ch/new-channel)))

(defn channel?
  "Return wheather v is a [[channel]]."
  [v]
  (ch/channel? v))

(defn send
  "Returns an event that will become available after the given value `v`
  could be sent over the given channel `ch`."
  [ch v]
  (assert (channel? ch) (pr-str ch))
  (Send. ch v))

(defn receive
  "Returns an event that will become available after a value could be
  received over the given channel `ch`, yielding that value."
  [ch]
  (assert (channel? ch) (pr-str ch))
  (Receive. ch))

(defn wrap
  "Returns an event equivalent to `ev`. After that is synchronized upon,
  `f` will be invoked on the resulting value, and it's result will
  become the value of the event."
  [ev f & args]
  (assert (event? ev) (pr-str ev))
  (assert (ifn? f) (pr-str f))
  (Wrap. ev f args))

(defn wrap-abort-m [ev f & args]
  (assert (event? ev) (pr-str ev))
  (assert (ifn? f) (pr-str f))
  (WrapAbort. ev f args))

(defn- monad-lift [f & args]
  (monad/free-bind (monad/return nil)
                   (fn [_] (monad/return (apply f args)))))

(defn wrap-abort [ev f & args]
  (assert (ifn? f) (pr-str f))
  (apply wrap-abort-m ev monad-lift f args))

(defn guard
  "Returns an event that when it's synchronized, `f` will be called which
  must return an event which is then synchronized upon instead."
  [f & args]
  (assert (ifn? f) (pr-str f))
  (Guard. f args))

(defn timeout
  "Returns an event that will become available the given amount of
  milliseconds after synchronization."
  [ms] ;; TODO: add time unit, or a nanosecond variant?
  (Timeout. ms))

;; With-nack could be defined in terms of with-abort and a channel, but it's a little more efficient with one more primitive: the ref even

(defrecord RefIs [ref predicate]
  Event (resolve [_]
          [(r/upd ref
                  (fn [[old input]]
                    (when (predicate old)
                      [old old])))
           nil]))

(defn ref-is
  "Returns an event that becomes available when the given reference
  cell is set to a value for which predicate returns truthy. The
  current value of the ref is then the result of the event."
  [ref predicate]
  (RefIs. ref predicate))

(defn- with-nack-internal [f args]
  (let [ref (demonad (refs/new-ref false))
        nack-ev (wrap (ref-is ref identity)
                      (constantly nil))
        nack (fn []
               (refs/reset ref true))]
    (wrap-abort-m (apply f nack-ev args)
                  nack)))

(defn with-nack
  "Returns an event, for which `(f nack-ev & args)` is called at
  synchronization time, which must return another event which is then
  synchronized on instead. The given `nack-ev` becomes available if
  this event is not the selected one in the synchronization of
  a [[choose]] event."
  [f & args]
  (guard with-nack-internal f args))

(defn- abort-others [aborts-map except-idx]
  (monad/sequ_ (reduce-kv (fn [r ai abort]
                            (if (or (= ai except-idx) (nil? abort))
                              r
                              (conj r (abort))))
                          []
                          aborts-map)))

(defrecord Choose [evs]
  Event (resolve [_]
          (let [reas-aborts (map resolve evs)
                aborts-map (into {} (map-indexed vector
                                                 (map second reas-aborts)))]
            [(apply r/choose (map-indexed (fn [idx rea]
                                            (r/>>> rea (r/post-commit (fn cho-post-abort [_]
                                                                        (abort-others aborts-map idx)))))
                                          (map first reas-aborts)))
             ;; abort commit => abort all:
             (when-not (empty? aborts-map)
               (fn cho-abort-all []
                 (abort-others aborts-map -1)))])))


(defn choose
  "Returns an event with will synchronize on one of the given events non-deterministically."
  [& evs]
  (assert (every? event? evs) (vec (remove event? evs)))
  (Choose. evs))

;; synchronization

(defn sync-m "Synchronize the given event as a command in the concurrency monad." [ev]
  (r/react! (first (resolve ev)) nil))

(defn sync-p "Synchronize the given event, returning a promise of the result." [ev]
  (c/run-many-to-many (sync-m ev)))

(defn sync
  "Synchronize the given event, blocking the current thread until it succeeds, resp. returning `timeout-val` after a timeout."
  ([ev]
   (c/run-many-to-many! (sync-m ev)))
  ([ev timeout-ms timeout-val]
   (c/run-many-to-many! (sync-m ev) timeout-ms timeout-val)))
