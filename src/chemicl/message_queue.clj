(ns chemicl.message-queue
  (:require
   [chemicl.monad :as cm :refer [defmonadic whenm]]
   [chemicl.concurrency :as conc]
   [active.clojure.record :as acr]
   [active.clojure.monad :as m]
   [chemicl.michael-scott-queue :as msq]))

(def new-message-queue msq/create)
(def push msq/push)
(def clean msq/clean-until)
(def cursor msq/cursor)
(def cursor-value msq/cursor-value)
(def cursor-next msq/cursor-next)
