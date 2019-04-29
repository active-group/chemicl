(ns chemicl.io
  (:require
   [active.clojure.record :as acr]
   [chemicl.concurrency :as conc]
   [chemicl.monad :as cm :refer [defmonadic whenm]]
   [chemicl.kcas :as kcas]
   [chemicl.reagents :as rea])
  (:import [java.nio.channels AsynchronousFileChannel]
           [java.nio.file StandardOpenOption OpenOption]))


;; --- BufferedReader Reagent ---------

(rea/defreagent read-buffered-reader [br]
  [a ctx rerun]
  (if-not (.ready br)
    ;; block
    (rea/my-block)

    ;; else read a line
    (let [l (.readLine br)]
      (if l
        (rea/my-return l)
        (rea/my-block)))))


;; --- Asynchronous File Channel Reagent ---------

;; Byte buffers

(defn- new-byte-buffer [size]
  (java.nio.ByteBuffer/allocate size))

(defn- byte-buffer-limit [buf]
  (.limit buf))

(defn- concat-byte-buffers [& bufs]
  (let [new-size (apply + (map byte-buffer-limit bufs))
        new-buf (new-byte-buffer new-size)]
    (doseq [b bufs]
      (.rewind b)
      (.put new-buf b))
    (.rewind new-buf)
    new-buf))

;; File channels

(defn- open-file-channel [p & opts]
  (let [path (java.nio.file.Paths/get
              (java.net.URI. p))]
    (java.nio.channels.AsynchronousFileChannel/open
     path
     (into-array OpenOption
                 opts))))

;; reagents

(rea/defreagent read-channel [ch]
  [a ctx rerun]
  (if ctx
    ;; been here before
    (let [context @ctx]
      (if (:done? context)
        (let [buf (:buffer context)
              s (.toString (.decode (java.nio.charset.Charset/forName "UTF-8") buf))]
          (rea/my-return s))
        ;; else block
        (rea/my-block)))

    ;; first time visit
    (let [buf (new-byte-buffer 1024)
          new-ctx (atom {:done? false
                         :buffer buf})]
      ;; kick off reader
      (.read ch buf 0 nil
             (reify java.nio.channels.CompletionHandler
               (completed [_ result _]
                 (.flip buf)
                 (swap! new-ctx assoc :done? true)
                 ;; restart reaction
                 (conc/run rerun))

               (failed [this ex attachment]
                 (println "failed"))))
      ;; store context
      (rea/my-block new-ctx)
      )))

(defn read-file [f]
  (read-channel (open-file-channel f java.nio.file.StandardOpenOption/READ)))


(defn readch [ch]
  (let [buf (new-byte-buffer 1024)
        [ep-1 ep-2] @(conc/run (chemicl.channels/new-channel))]
    ;; kick off reader
    (.read ch buf 0 nil
           (reify java.nio.channels.CompletionHandler
             (completed [_ result _]
               (.flip buf)
               (conc/run
                 (rea/react! (rea/swap ep-1) buf)))

             (failed [this ex attachment]
               (println "failed"))))

    (rea/swap ep-2)
    ))

(defn readf [f]
  (readch (open-file-channel f java.nio.file.StandardOpenOption/READ)))
