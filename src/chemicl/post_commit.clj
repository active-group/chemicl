(ns chemicl.post-commit
  (:require [active.clojure.record :as acr]
            [active.clojure.monad :as m]))

;; -----------------------------------------------
;; Internal representation

;; A post commit action is either
;; 1. PostCommitCAS
;; 2. PostCommitReturn

;; (acr/define-record-type PostCommitCASAction
;;   (make-post-commit-cas-action r ov nv m)
;;   post-commit-cas-action?
;;   [r post-commit-cas-action-ref
;;    ov post-commit-cas-action-old
;;    nv post-commit-cas-action-new
;;    m post-commit-cas-action-action])

;; (acr/define-record-type PostCommitReturn
;;   (make-post-commit-return v m)
;;   post-commit-return?
;;   [v post-commit-return-value
;;    m post-commit-return-action])


;; ;; -----------------------------------------------
;; ;; Public API

;; (defn make-post-commit-cas+action [r ov nv m]
;;   (make-post-commit-cas-action r ov nv m))

;; (defn make-post-commit-cas [r ov nv]
;;   (make-post-commit-cas-action r ov nv (m/return nil)))

;; (defn make-post-commit-action [m]
;;   (make-post-commit-return nil m))
