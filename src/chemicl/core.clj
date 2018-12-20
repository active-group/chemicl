(ns chemicl.core
  (:import [java.util.concurrent Executors Executor])
  (:require
   [clojure.stacktrace :as st]
   [clojure.core.async :as a]
   [active.clojure.record :as acr]
   [active.clojure.monad :as monad]))


(defn print-tid! []
  (println "THREAD: "
           (pr-str
            (.getId (Thread/currentThread)))))

(print-tid!)

(println "---")
(doseq [i (range 24)]
  (a/go
    (print-tid!)))



(def state-vec-atom (atom nil))

(clojure.core/let
    [c__11128__auto__
     (clojure.core.async/chan 1)

     captured-bindings__11129__auto__
     (clojure.lang.Var/getThreadBindingFrame)]

  (print-tid!)
  (clojure.core.async.impl.dispatch/run
    (fn* []
         #dbg (clojure.core/let
                  [f__11130__auto__
                   (clojure.core/fn state-machine__10917__auto__

                     ([]
                      (clojure.core.async.impl.ioc-macros/aset-all!
                       (java.util.concurrent.atomic.AtomicReferenceArray. 7)
                       0 state-machine__10917__auto__
                       1 1))

                     ([state_19069]
                      (clojure.core/let
                          [_ (print-tid!)
                           old-frame__10918__auto__
                           (clojure.lang.Var/getThreadBindingFrame)

                           ret-value__10919__auto__
                           (try
                             (clojure.lang.Var/resetThreadBindingFrame
                              (clojure.core.async.impl.ioc-macros/aget-object state_19069 3))
                             (clojure.core/loop []
                               (clojure.core/let
                                   [result__10920__auto__
                                    (clojure.core/case
                                        (clojure.core/int (clojure.core.async.impl.ioc-macros/aget-object state_19069 1))
                                      1 (clojure.core/let [_ (print-tid!) 
                                                           inst_19067 (println "hi")]
                                          (clojure.core.async.impl.ioc-macros/return-chan state_19069 inst_19067)))]
                                 (if (clojure.core/identical? result__10920__auto__ :recur)
                                   (recur)
                                   result__10920__auto__)))

                             (catch java.lang.Throwable ex__10921__auto__
                               (clojure.core.async.impl.ioc-macros/aset-all! state_19069 2 ex__10921__auto__)
                               (if (clojure.core/seq (clojure.core.async.impl.ioc-macros/aget-object state_19069 4))
                                 (clojure.core.async.impl.ioc-macros/aset-all!
                                  state_19069
                                  1 (clojure.core/first (clojure.core.async.impl.ioc-macros/aget-object state_19069 4))
                                  4 (clojure.core/rest (clojure.core.async.impl.ioc-macros/aget-object state_19069 4)))
                                 (throw ex__10921__auto__))
                               :recur)

                             (finally
                               (clojure.lang.Var/resetThreadBindingFrame old-frame__10918__auto__)))]

                        (if (clojure.core/identical? ret-value__10919__auto__ :recur)
                          (recur state_19069)
                          ret-value__10919__auto__))))

                   state__11131__auto__
                   (clojure.core/->
                    (f__11130__auto__)
                    (clojure.core.async.impl.ioc-macros/aset-all!
                     clojure.core.async.impl.ioc-macros/USER-START-IDX c__11128__auto__
                     clojure.core.async.impl.ioc-macros/BINDINGS-IDX captured-bindings__11129__auto__))]


                (print-tid!)
                (clojure.core.async.impl.ioc-macros/run-state-machine-wrapped state__11131__auto__)
                #_(reset! state-vec-atom state__11131__auto__))))
  c__11128__auto__)

(clojure.core.async.impl.ioc-macros/run-state-machine-wrapped state__11131__auto__)

(def sv @state-vec-atom)

((clojure.core.async.impl.ioc-macros/aget-object sv 0) sv)
;; ==
(clojure.core.async.impl.ioc-macros/run-state-machine-wrapped @state-vec-atom)

(a/go
  (print-tid!))












;; Monad results

(acr/define-record-type Parked
  (make-parked cont)
  parked?
  [cont parked-cont])

(acr/define-record-type Quit
  (make-quit)
  quit?
  [])

(acr/define-record-type Returned
  (make-returned v)
  returned?
  [v returned-value])

(acr/define-record-type Forked
  (make-forked cont m)
  forked?
  [cont forked-cont
   m forked-other-monad])

(acr/define-record-type Putting
  (make-putting c v cont)
  putting?
  [c putting-chan-ref
   v putting-value
   cont putting-cont])

(acr/define-record-type Taking
  (make-taking c cont)
  taking?
  [c taking-chan-ref
   cont taking-cont])

;; Monad commands

(acr/define-record-type Park
  (make-park)
  park?
  [])

(acr/define-record-type Fork
  (make-fork m)
  fork?
  [m fork-monad])

(acr/define-record-type Exit
  (make-exit)
  exit?
  [])

(acr/define-record-type Print
  (make-print l)
  print?
  [l print-line])

(acr/define-record-type Put
  (make-put c v)
  put?
  [c put-chan-ref
   v put-value])

(acr/define-record-type Take
  (make-take c)
  take?
  [c take-chan-ref])


;; Channel data structure
(acr/define-record-type Chan
  (make-chan p t)
  chan?
  [p chan-putter
   t chan-taker])

(defn new-chan []
  (make-chan (atom nil) (atom nil)))

(defn run-cont
  [m]
  (loop [m m]
    (cond
      (monad/free-return? m)
      (make-returned
       (monad/free-return-val m))

      (park? m)
      ;; parking in the last call is quite stupid
      (make-quit)

      (fork? m)
      ;; forking in the last call is continuing with the fork
      (recur (fork-monad m))

      (print? m)
      (do
        (println (print-line m))
        (make-returned nil))

      (put? m)
      (make-putting (put-chan-ref m)
                    (put-value m)
                    (fn [_] nil))

      (take? m)
      (make-taking (take-chan-ref m)
                   (fn [_] nil))

      (monad/free-bind? m)
      (let [m1 (monad/free-bind-monad m)
            c (monad/free-bind-cont m)]
        (cond
          (monad/free-return? m1)
          (recur (c (monad/free-return-val m1)))

          (park? m1)
          (make-parked c)

          (exit? m1)
          (make-quit)

          (fork? m1)
          (make-forked c (fork-monad m1))

          (print? m1)
          (do
            (println (print-line m1))
            (recur (c nil)))

          (put? m1)
          (make-putting (put-chan-ref m1)
                        (put-value m1)
                        c)

          (take? m1)
          (make-taking (take-chan-ref m1)
                       c)

          )
        ))))


(def p2 (monad/monadic
         (make-print "bim p2")))

(def p1 (monad/monadic
         (make-fork p2)
         (make-park)
         (make-print "bim p1 nach fork")
         (monad/return 123)))



(defn run-many-to-one [m]
  (loop [ms [m]]
    #_(println "another round: " (pr-str ms))
    (when-let [[em & ems] ms]
      (let [res (run-cont em)]
        (cond

          (parked? res)
          (let [cont (parked-cont res)]
            (recur (concat ems [(cont nil)])))

          (quit? res)
          (recur ems)

          (returned? res)
          (recur ems)

          (forked? res)
          (let [c (forked-cont res)
                om (forked-other-monad res)
                forked-tid 1337]
            (recur (concat [(c 1337) om] ems))))))))



;; stolen from core.async

(defprotocol Xctr
  (exec [e runnable] "execute runnable asynchronously"))

(defn thread-pool-executor
  []
  (let [executor-svc (Executors/newFixedThreadPool 8)]
    (reify Xctr
      (exec [this r]
        (.execute executor-svc ^Runnable r)))))

(defonce executor (delay (thread-pool-executor)))

(defn run
  "Runs Runnable r in a thread pool thread"
  [^Runnable r]
  (exec @executor r))

(run (fn []
       (print-tid!)
       (println "whatever")
       "hiahahah"
       ))

(def parked-continuations (atom []))


(defn run-many-to-many [m]
  (run (fn []
         (let [res (run-cont m)]
           (cond

             (parked? res)
             (let [cont (parked-cont res)]
               (swap! parked-continuations (fn [cs]
                                             (conj cs (cont nil)))))

             (quit? res)
             :ok

             (returned? res)
             :ok

             (forked? res)
             (let [c (forked-cont res)
                   om (forked-other-monad res)
                   forked-tid 1337]
               (run-many-to-many (c 1337))
               (run-many-to-many om))

             (putting? res)
             (let [ch-ref (putting-chan-ref res)
                   v (putting-value res)
                   cont (putting-cont res)]
               (loop []
                 (let [ch @ch-ref]
                   (when-let [taker (chan-taker ch)]
                     (let [taker-m (taker v)
                           putter-m (cont nil)]
                       ;; Both continuations can run
                       ;; First we need to claim that we are the ones setting them off
                       (if (compare-and-set! ch-ref ch (new-chan))
                         (do
                           (run-many-to-many taker-m)
                           (run-many-to-many putter-m))
                         ;; else retry
                         (recur))
                       )))))

             (taking? res)
             (let [ch-ref (taking-chan-ref res)
                   cont (taking-cont res)]
               (loop []
                 (let [ch @ch-ref]
                   (when-let [putter (chan-putter ch)]
                     (let [taker-m (cont )])
                     ))))

             ))))) 



@parked-continuations
(reset! parked-continuations [])

(run-many-to-one p1)
(run-many-to-many p1)






(acr/define-record-type Print
  (make-print l)
  print?
  [l print-line])

(acr/define-record-type Park
  (make-park)
  park?
  [])

(acr/define-record-type Fork
  (make-fork m)
  fork?
  [m fork-monad])


(defn run-park
  [m]
  (loop [m m
         parked-continuations []]
    (cond
      (monad/free-return? m)
      (if-let [[n & r] parked-continuations]
        (do
          (println "continuing")
          (recur n r))
        (do
          (println "done")
          (monad/free-return-val m)
          ))

      (print? m)
      (do
        (println (print-line m))
        (if-let [[n & r] parked-continuations]
          (do
            (println "continuing from print")
            (recur n r))
          (do
            (println "done from print")
            nil
            ))
        )

      (monad/free-bind? m)
      (let [m1 (monad/free-bind-monad m)
            c (monad/free-bind-cont m)]
        (cond
          (monad/free-return? m1)
          (do (println "return inside bind")
            (recur (c (monad/free-return-val m1))
                   parked-continuations))

          (print? m1)
          (do
            (println (print-line m1))
            (println "parked cont in print: " (pr-str parked-continuations))
            (recur (c nil) parked-continuations))

          (park? m1)
          (let [next
                (if-not (empty? parked-continuations)
                  (first parked-continuations)
                  (c nil))

                rest
                (if-not (empty? parked-continuations)
                  (rest parked-continuations)
                  [])]
            (println "parking" (pr-str parked-continuations))
            (recur next (concat rest [(c nil)])))

          (fork? m1)
          (let [other-m (fork-monad m1)
                new-parked (concat parked-continuations [other-m])]
            (println "new parked: " (pr-str new-parked))
            (recur (c nil)
                   new-parked))


          )))))

(def print-sth-naughty
  (monad/monadic
   (make-print "shite")))

(def print-sth-nice
  (monad/monadic
   (make-print "NEIS")))

(run-park
 (monad/monadic
  (make-fork print-sth-naughty)
  (make-park)
  (make-print "jetzt ich")
  (monad/return nil)))



(run-park
 (monad/monadic
  (make-fork print-sth-naughty)
  (make-fork print-sth-nice)
  (make-print "hi")
  (make-print "sup")
  (make-park)
  (make-park)
  (make-park)
  (monad/return 123)
  ))



(acr/define-record-type Reaction)

(acr/define-record-type EmptyReaction
  (make-empty-reaction)
  empty-reaction?
  [])

(defn try-react
  [reagent a reaction]
  )

(defn !-reagent
  [reagent a]
  (monad/monadic
   [res (try-react reagent a (make-empty-reaction))]
   (case res
     :block (make-park)
     :retry (make-park)
     (monad/return
      res))))










(run-state (monad/monadic
            (make-set 99)
            [foo (make-get)]
            (monad/return (+ 1 foo))))

(def count-down 
  (monad/monadic
   [v (make-get)]
   (make-print (str "another round?" v))
   (if (= v 0)
     (monad/return "noice")
     (monad/monadic
       (make-set (- v 1))
       count-down))))

(run-state
 (monad/monadic
  (make-set 50000)
  count-down))


































(defn return [a]
  (println "return")
  (fn [k] (k a)))

(defn bind [c f]
  (println "----------")
  (println "bind")
  (fn [k]
    (c (fn [a]
         ((f a) k)))))

(def mop-1
  (return 123))

(def mop-2
  (bind (return 123)
        (fn [v] (fn [k] (k (+ 1 v))))))

(def mop-3
  (bind (return 123)
        (fn [v] (return (+ 1 v)))))

(defn mop-4 [n]
  (bind (return n)
       (fn [v] (if (= n 0)
                 (return :yep)
                 (mop-4 (- n 1))))))

(mop-1 identity)
(mop-2 identity)
(mop-3 identity)
((mop-4 9999) identity)
