(ns chemicl.maybe)

;; --- Maybe ---------

(defn just [x]
  {::tag ::just
   ::value x})

(defn just? [x]
  (= ::just
     (::tag x)))

(defn just-value [x]
  (::value x))

(def nothing
  ::nothing)

(defn nothing? [x]
  (= ::nothing x))

(defn maybe? [x]
  (or (just? x)
      (nothing? x)))

(defn- maybe-case-1 [probe just-var just-body nothing-body]
  `(cond
     (just? ~probe)
     (let [~just-var (just-value ~probe)]
       ~just-body)

     (nothing? ~probe)
     ~nothing-body))

(defmacro maybe-case
  {:style/indent :defn}
  [probe pred-1 body-1 pred-2 body-2]
  (if (or (= 'just (first pred-1))
          (= `just (first pred-1)))
    (maybe-case-1 probe (second pred-1) body-1 body-2)
    (maybe-case-1 probe (second pred-2) body-2 body-1)))
