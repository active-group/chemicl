(ns chemicl.channels
  (:require
   [chemicl.concurrency :as conc]
   [active.clojure.record :as acr]
   [active.clojure.lens :as lens]
   [active.clojure.monad :as m]))

;; Helpers

(defn append [e l]
  (concat [e] l))


;; Channels

(acr/define-record-type Channel
  (make-channel state)
  channel?
  [state channel-state]) 

(acr/define-record-type ChannelState
  (make-channel-state r w)
  channel-state?
  [(r channel-state-readers channel-state-readers-lens)  ;; : atom [task]
   (w channel-state-writers channel-state-writers-lens)]) ;; : atom [[v task]]

(defn new-chan []
  (m/return
   (make-channel
    (atom
     (make-channel-state [] [])))))

(defn <! [chan]
  (let [sref (channel-state chan)
        s @sref
        ws (channel-state-writers s)
        rs (channel-state-readers s)
        [v task] (first ws)]
    (if task
      ;; unpark writer and deliver value
      (m/monadic
       (let [succ (compare-and-set!
                   sref s
                   (lens/shove
                    s channel-state-writers-lens
                    (rest ws)))])
       (if succ
         (m/monadic
          (conc/unpark task nil)
          (m/return v))
         ;; else
         (<! chan)
         ))

      ;; else register as a reader and block
      (m/monadic
       [self (conc/get-current-task)]
       (let [succ (compare-and-set!
                   sref s
                   (lens/overhaul
                    s channel-state-readers-lens
                    (partial append self)))])
       (if succ
         (conc/park)
         (<! chan))))))

(defn >! [chan value]
  (let [sref (channel-state chan)
        s @sref
        ws (channel-state-writers s)
        rs (channel-state-readers s)
        task (first rs)
        ]
    (if task
      ;; unpark reader by delivering value
      (m/monadic
       (let [succ (compare-and-set!
                   sref s
                   (lens/shove
                    s channel-state-readers-lens
                    (rest rs)))])
       (if succ
         (conc/unpark task value)
         (>! chan value)))

      ;; else register as a writer and block
      (m/monadic
       [self (conc/get-current-task)]
       (let [succ (compare-and-set!
                   sref s
                   (lens/overhaul
                    s channel-state-writers-lens
                    (partial append [value self])))])
       (if succ
         (conc/park)
         (>! chan value))
       ))))





;; sand casten

(defn konsumer [ch]
  (m/monadic
   (conc/print "consuming")
   [v (<! ch)]
   (conc/print (str "got: " (pr-str v)))
   ))

(defn all-consumer [ch]
  (m/monadic
   (conc/print "---")
   [v (<! ch)]
   (conc/print (pr-str v))
   (all-consumer ch)
   ))

(defn simple-producer [ch v]
  (m/monadic
   (>! ch v)))

(conc/run-many-to-many (all-consumer ch-1))
(conc/run-many-to-many (simple-producer ch-1 99))



(defn produker [ch]
  (m/monadic
   (conc/print "waiting")
   (conc/timeout 3000)
   (conc/print "producing")
   (>! ch "hello")
   (conc/print "produced nicely")
   (conc/exit)))

(def p1
  (m/monadic
   (conc/print "----")
   [ch (new-chan)]
   (conc/fork (konsumer ch))
   (conc/fork (produker ch))
   (conc/print "done")
   ))

(conc/run-many-to-many p1)

(def ch-1 (make-channel
           (atom
            (make-channel-state [] []))))

ch-1

(conc/run-many-to-many (produker ch-1))


(conc/run-many-to-many (konsumer ch-1))


(conc/run-cont (konsumer (make-channel
                                  (atom
                                   (make-channel-state [] []))))
               (conc/new-task!))




ch-1
((m/free-bind-cont (<! ch-1))
 (conc/new-task!))
