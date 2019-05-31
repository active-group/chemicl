(ns chemicl.message-queue
  (:require
   [chemicl.monad :as cm :refer [defmonadic whenm]]
   [chemicl.concurrency :as conc]
   [active.clojure.record :as acr]
   [active.clojure.monad :as m]
   [chemicl.michael-scott-queue :as msq]
   [chemicl.stack :as st]))

(def new-message-queue st/new-stack)
(def push st/push-or-reuse)
(def clean st/clean)
(def cursor st/cursor)
(def cursor-value st/cursor-value)
(def cursor-next st/cursor-next)
