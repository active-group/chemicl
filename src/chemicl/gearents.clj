(ns chemicl.gearents
  (:require
   [active.clojure.monad :as m]
   [active.clojure.record :as acr]
   [chemicl.concurrency :as conc]
   [chemicl.offers :as offers]
   [chemicl.kcas :as kcas]
   [chemicl.refs :as refs]
   [chemicl.reactions :as rx]
   [chemicl.monad :as cm :refer [defmonadic whenm]]))


;; -----------------------------------------------
;; Another try

(acr/define-record-type SequentialComposition
  (>>>> l r)
  >>>>?
  [l >>>>-l
   r >>>>-r])

(acr/define-record-type Update
  (upd r f)
  upd?
  [r upd-ref
   f upd-fn])

(acr/define-record-type CAS
  (cas r ov nv)
  cas?
  [r cas-ref
   ov cas-old
   nv cas-new])

(acr/define-record-type Read
  (read r)
  read?
  [r read-ref])

(acr/define-record-type Choose
  (choose l r)
  choose?
  [l choose-l
   r choose-r])


;; -----------------------------------------------
;; reagent -> reaction

(declare try-react)

(defmonadic try-react-cas [rea a oref]
  (let [r (cas-ref rea)
        ov (cas-old rea)
        nv (cas-new rea)])
  (m/return [(rx/rx-from-cas [r ov nv]) nil]))

(defmonadic try-react-read [rea a oref]
  (let [r (read-ref rea)])
  [v (refs/read r)]
  (m/return [(rx/empty-rx) v]))

(defmonadic try-react-upd [rea a oref]
  (let [r (upd-ref rea)
        f (upd-fn rea)])
  [ov (refs/read r)]
  (let [ov-data (refs/ref-data ov)
        ov-offers (refs/ref-offers ov)
        res (f [ov-data a])])
  (if res
    ;; record cas
    (m/monadic
     (let [[nv-data retv] res
           nv (refs/make-ref nv-data ov-offers)])
     (m/return [(rx/rx-from-cas [r ov nv])
                retv]))
    ;; else wait on ref by placing offer there and block
    (m/monadic
     (refs/add-offer r oref)
     (m/return :block))))

(defmonadic try-react-seq-comp [rea a oref]
  (let [l (>>>>-l rea)
        r (>>>>-r rea)])
  [[lrx lres] (try-react l a oref)]
  (conc/print "left result: " (pr-str lres))
  [[rrx rres] (try-react r lres oref)]
  (conc/print "right result: " (pr-str rres))
  (conc/print "")
  (m/return [(rx/rx-union lrx rrx) rres]))

(defmonadic try-react-choose [rea a oref]
  (let [l (choose-l rea)
        r (choose-r rea)])
  ;; try left
  [lres (try-react l a oref)]
  (if (= :block lres)
    ;; try right
    (try-react r a oref)
    ;; else return res
    (m/return lres)))

(defn try-react [rea a oref]
  (cond
    (>>>>? rea)
    (try-react-seq-comp rea a oref)

    (cas? rea)
    (try-react-cas rea a oref)

    (upd? rea)
    (try-react-upd rea a oref)

    (choose? rea)
    (try-react-choose rea a oref)))

(defmonadic with-offer [reagent a]
  [oref (offers/new-offer)]
  [res (try-react reagent a oref)]
  (cond
    (= :block res)
    (m/monadic
     (conc/print "with-offer blocks -> wait on offer")
     (offers/wait oref)
     ;; when continued:
     [ores (offers/rescind oref)]
     (if ores
       ;; got an answer to return
       (m/monadic
        (conc/print "huh, got an answer?")
        (m/return ores))
       ;; else retry
       (m/monadic
        (conc/print "retrying")
        (with-offer reagent a))))

    :else
    (m/return res)))

(defmonadic without-offer [reagent a]
  [res (try-react reagent a nil)]
  (cond
    (= :block res)
    (m/monadic
     (conc/print "without-offer blocks -> try with-offer")
     (with-offer reagent a))

    :else
    (m/return res)))

(defmonadic react! [reagent a]
  ;; phase 1 (may block)
  [[rx res] (without-offer reagent a)]
  ;; phase 2 (may retry)
  (rx/rx-commit rx)
  (m/return res))


;; missing:
;; choose
;; computed
;; message passing


;; now message passing can be implemented in terms of update and >>>>


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
  [res (react! (>>>> (treiber-pop ts-1)
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

(conc/run-many-to-many (pusheroonies ts-1 :nather))
(conc/run-many-to-many (pusheroonies ts-2 :yooo))
#_(conc/run-many-to-many (poperoonies ts-1))
(conc/run-many-to-many (moveroonies ts-1 ts-2))
(conc/run-many-to-many (popeitheroonies ts-1 ts-2))
(conc/run-many-to-many (pusheitheroonies ts-1 ts-2))
(conc/run-many-to-many (takeeither ts-1 ts-2))

(defn printref [r]
  (println (pr-str @r)))

(printref ts-1)
(printref ts-2)
