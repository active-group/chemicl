<img src="https://raw.githubusercontent.com/active-group/chemicl/master/logo.png" width="180">

> « Sortie de secours – casser la vitre en cas de danger »

A Clojure library for composable concurrency based on Reagents.

## Usage

## API

Reagents are reified concurrent programs, which can be combined to form larger concurrent programs. Reagents communicate and coordinate via shared memory or message-passing.

### Monadic Substrate

Reagents run on a monadic substrate that implements lightweight threading. Other than with `core.async`, we don't try to transform your ordinary functions into monadic (CPS) form. You have to write monadic programs yourself. `chemicl.concurrency` provides you with the monadic commands that you can combine with the usual `active.clojure.monad` facilities.

You can fork a new lightweight thread using `fork`, which returns a handle for the forked thread. There are commands to `park` the current thread and `unpark` a thread via its handle. In addition, we provide `new-ref`, `cas`, `read`, `reset`, and `timeout`. You can run a program with `run-many-to-many`, which runs your lightweight threads on a fixed-size thread pool.

```clojure
(defn prog []
  (m/monadic
    ;; fork a lightweight thread
    [t1 (fork
         (m/monadic
          ...
          (park)
          (print "sup")))]

     ;; print something
     (print "hi")

     ;; unpark child
     (unpark t1 nil)))

(run-many-to-many (prog)) ;; prints "hi\nsup"
```

The monad runner `run-many-to-many` always returns `nil`, so you cannot break out of the monad. It is up for debate whether we want this behaviour.

## Reagents

A reagent is a reified concurrent program. You can think of any reagent as a box with one input and one output.

Given a reagent, you can run it with `chemicl.reagents/react!`. This returns a monadic program that you can run inside the substrate mentioned above.

### Shared-memory primitives

You can create memory references (think Clojure atoms) with `chemicl.refs/new-ref`.

Reagents can communicate via shared memory by means of the `upd` reagent. `upd` takes a memory reference and an update function. The update function must accept a tuple of the old value stored in the memory cell and the input value to the reagent. It must either return `nil`, which signals that the reagent must block, or a tuple of an updated value for the memory cell and the output value of the reagent. A call to `upd` returns a reagent that you can combine with other reagents or run with `react!`.

```clojure
;; A reagent that increments a counter in a memory cell
(defn inc-reagent [cell]
  (upd cell (fn [[old-counter _]]
              [(inc old-counter) nil])))

;; A reagent that adds its input to the value in a memory cell
;; In addition it returns whether the value was even?
(defn add-reagent [cell]
  (upd cell (fn [[old input]]
              [(+ old input) (even? old])))

;; A reagent that increments odd values and blocks on even values
(defn inc-odd-or-block-reagent [cell]
  (upd cell (fn [[old _]]
              (when (even? old)
                [(inc old) nil]))))
```

A blocked `upd` reagent will re-run when someone else updates that same memory cell.


### Message-passing primitives

You can create a channel (a pair of endpoints) with `chemicl.channels/new-channel`.

Reagents can communicate and coordinate via message-passing by means of the `swap` reagent. `swap` takes an endpoint and tries to exchange messages with someone `swap`ping on the dual endpoint. If there is currently no other swapper to be found, the reagent blocks. Other than with `upd`, two reagents involving a `swap` on the same channel always commit in a single transaction. The usual duality between shared-memory and message-passing concurrency therefore doesn't hold for reagents.

```clojure
(run-many-to-many
  (m/monadic
    [[ep1 ep2] (new-channel)]
    (fork (m/monadic
           [r1 (react! (swap ep1) :hi-from-1)]
           (print "1 got" r1)))
    (fork (m/monadic
           [r2 (react! (swap ep2) :hi-from-2)]
           (print "2 got" r2)))))
;; prints the following two lines
;; 1 got :hi-from-2
;; 2 got :hi-from-1
```


### Sequential composition

You can think of any reagent as a box with one input and one output. The simplest composition is to route the output of one reagent to the input of the other. You can compose reagents sequentially with `chemicl.reagents/>>>`.

```clojure
(def seq-rea
  (>>> (upd cell-1 ...)
       (upd cell-2 ...)))
```

### Choice

You build a reagents that runs either of two options with `chemicl.reagents/choose`.

```clojure
(def alt-rea
  (choose (upd cell-1 ...)
          (upd cell-2 ...)))
```

### Computed

You can build a reagent from a function that returns a reagent. This bridges the gap between the very static arrow interface discussed above to a dynamic world more akin to monads.

```clojure
(defn depends-on-rea [cell-1 cell-2]
  (computed
   (fn [a]
     (if (even? a)
       ;; when the input to this reagent is even
       ;; we want to update cell-1
       (upd cell-1 ...)
       ;; when the input to this reagent is not even
       ;; we want to update cell-2
       (upd cell-2 ...)))))
```

## License

Copyright © 2018 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
