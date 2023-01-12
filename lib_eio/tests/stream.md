```ocaml
# #require "eio";;
```
```ocaml
module T = Eio__Stream_state
let show t = Fmt.pr "%a@." T.dump t

let add t v =
  match T.add t v with
  | None ->
    Fmt.pr "Added %s@." v;
    None
  | Some request ->
    Fmt.pr "Waiting for space to add %s@." v;
    T.add2 request (fun () -> Fmt.pr "Added %s@." v);
    Some request

let take t label =
  match T.take t with
  | Ok v -> Fmt.pr "%s: Took %s@." label v; None
  | Error request ->
    Fmt.pr "%s: Waiting for value@." label;
    T.take2 request (fun v -> Fmt.pr "%s: Took %s@." label v);
    Some request
```

Initially the queue is empty and there are no waiters.

```ocaml
# let t : string T.t = T.create 1;;
val t : string T.t =
  {T.count = <abstr>; capacity = 1; queue = <abstr>; producers = <abstr>}
# show t;;
Stream (items=0,borrows=0)
  Queue:
    Segment 0 (prev=None, pointers=2, cancelled=0):
      Empty (suspend) (resume)
      Empty
      Empty
      Empty
    End
  Producers:
    Segment 0 (prev=None, pointers=2, cancelled=0):
      In_transition (suspend) (resume)
      In_transition
      In_transition
      In_transition
    End
- : unit = ()
```

Adding one waiter makes the item count go negative:

```ocaml
# take t "cons1";;
cons1: Waiting for value
- : string T.take_request option =
Some
 ({T.count = <abstr>; capacity = 1; queue = <abstr>; producers = <abstr>},
  <abstr>, <abstr>)

# show t;;
Stream (items=-1,borrows=0)
  Queue:
    Segment 0 (prev=None, pointers=2, cancelled=0):
      Consumer (resume)
      Empty (suspend)
      Empty
      Empty
    End
  Producers:
    Segment 0 (prev=None, pointers=2, cancelled=0):
      In_transition (suspend) (resume)
      In_transition
      In_transition
      In_transition
    End
- : unit = ()
```

Adding one value wakes it:

```ocaml
# add t "A";;
cons1: Took A
Added A
- : string T.add_request option = None

# show t;;
Stream (items=0,borrows=0)
  Queue:
    Segment 0 (prev=None, pointers=2, cancelled=0):
      Finished
      Empty (suspend) (resume)
      Empty
      Empty
    End
  Producers:
    Segment 0 (prev=None, pointers=2, cancelled=0):
      In_transition (suspend) (resume)
      In_transition
      In_transition
      In_transition
    End
- : unit = ()
```

Adding a second value enqueues it and sets the item count to 1:

```ocaml
# add t "B";;
Added B
- : string T.add_request option = None

# show t;;
Stream (items=1,borrows=0)
  Queue:
    Segment 0 (prev=None, pointers=2, cancelled=0):
      Finished
      Value (suspend)
      Empty (resume)
      Empty
    End
  Producers:
    Segment 0 (prev=None, pointers=2, cancelled=0):
      In_transition (suspend) (resume)
      In_transition
      In_transition
      In_transition
    End
- : unit = ()
```

Adding a third value must wait (the item count now exceeds capacity):
```ocaml
# add t "C";;
Waiting for space to add C
- : string T.add_request option =
Some
 ({T.count = <abstr>; capacity = 1; queue = <abstr>; producers = <abstr>},
  <abstr>, <abstr>, "C")

# show t;;
Stream (items=2,borrows=0)
  Queue:
    Segment 0 (prev=None, pointers=2, cancelled=0):
      Finished
      Value (suspend)
      Empty (resume)
      Empty
    End
  Producers:
    Segment 0 (prev=None, pointers=2, cancelled=0):
      Request (resume)
      In_transition (suspend)
      In_transition
      In_transition
    End
- : unit = ()
```

The next consumer reads the enqueued value and wakes the producer:
```ocaml
# take t "cons2";;
Added C
cons2: Took B
- : string T.take_request option = None

# show t;;
Stream (items=1,borrows=0)
  Queue:
    Segment 0 (prev=None, pointers=2, cancelled=0):
      Finished
      Finished
      Value (suspend)
      Empty (resume)
    End
  Producers:
    Segment 0 (prev=None, pointers=2, cancelled=0):
      Finished
      In_transition (suspend) (resume)
      In_transition
      In_transition
    End
- : unit = ()
```

Finally, we collect the last value:
```ocaml
# take t "cons3";;
cons3: Took C
- : string T.take_request option = None
```

## Cancellation

Cancelling a consumer restores the item count:
```ocaml
# let t : string T.t = T.create 1;;
val t : string T.t =
  {T.count = <abstr>; capacity = 1; queue = <abstr>; producers = <abstr>}
# let request = take t "cons1" |> Option.get;;
cons1: Waiting for value
val request : string T.take_request =
  ({T.count = <abstr>; capacity = 1; queue = <abstr>; producers = <abstr>},
   <abstr>, <abstr>)

# show t;;
Stream (items=-1,borrows=0)
  Queue:
    Segment 0 (prev=None, pointers=2, cancelled=0):
      Consumer (resume)
      Empty (suspend)
      Empty
      Empty
    End
  Producers:
    Segment 0 (prev=None, pointers=2, cancelled=0):
      In_transition (suspend) (resume)
      In_transition
      In_transition
      In_transition
    End
- : unit = ()
```

```ocaml
# T.cancel_take request;;
- : bool = true

# show t;;
Stream (items=0,borrows=0)
  Queue:
    Segment 0 (prev=None, pointers=2, cancelled=1):
      Cancelled (resume)
      Empty (suspend)
      Empty
      Empty
    End
  Producers:
    Segment 0 (prev=None, pointers=2, cancelled=0):
      In_transition (suspend) (resume)
      In_transition
      In_transition
      In_transition
    End
- : unit = ()
```

Cancelling a producer restores the item count:

```ocaml
# let t : string T.t = T.create 1;;
val t : string T.t =
  {T.count = <abstr>; capacity = 1; queue = <abstr>; producers = <abstr>}
# add t "A";;
Added A
- : string T.add_request option = None
# let request = add t "B" |> Option.get;;
Waiting for space to add B
val request : string T.add_request =
  ({T.count = <abstr>; capacity = 1; queue = <abstr>; producers = <abstr>},
   <abstr>, <abstr>, "B")

# show t;;
Stream (items=2,borrows=0)
  Queue:
    Segment 0 (prev=None, pointers=2, cancelled=0):
      Value (suspend)
      Empty (resume)
      Empty
      Empty
    End
  Producers:
    Segment 0 (prev=None, pointers=2, cancelled=0):
      Request (resume)
      In_transition (suspend)
      In_transition
      In_transition
    End
- : unit = ()

# T.cancel_add request;;
- : bool = true

# show t;;
Stream (items=1,borrows=0)
  Queue:
    Segment 0 (prev=None, pointers=2, cancelled=0):
      Value (suspend)
      Empty (resume)
      Empty
      Empty
    End
  Producers:
    Segment 0 (prev=None, pointers=2, cancelled=1):
      Finished (resume)
      In_transition (suspend)
      In_transition
      In_transition
    End
- : unit = ()
```
