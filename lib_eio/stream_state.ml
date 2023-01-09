(* A lock-free bounded stream with cancellation, using Cells.

   Step 1: an unbounded queue without cancellation

   There is a sequence of cells ([main]) and a count ([items]).
   To add an item, a producer increments the counter and adds the value to main.
   To take an item, a consumer decrements the counter and removes the value.

   If there are more consumers than producers then [items] will go negative.
   The consumer will place a callback in the cell and when a producer later
   arrives it will pass the value to it.

   XXX: non-blocking operations

   Step 2: a bounded queue

   For a bounded queue, producers may need to wait for space. Since Cells
   only allows one party to cancel, we need a second queue [overflow] for
   this.

   When a producer increments [items] to be greater than [capacity], it
   instead adds itself to [overflow] to wait for space.

   When a consumer decrements [items] from being over [capacity], it wakes
   one producer.

   Invariants:

   - items = items_in_stream + waiting_producers - waiting_consumers
   - items_in_stream in 0..capacity

   Note that we support 0-capacity streams too.

   [waiting_producers] is the number of producers that have incremented [items]
   and not yet written their value to [main]. [waiting_consumers] is the number
   of consumers that have decremented [items] and not yet removed the value from
   [main].

   The system is at rest when no further actions can be taken without the
   user of the stream doing something. When the system is at rest:

   - waiting_consumers > 0 -> items_in_stream = 0
   - waiting_consumers > 0 -> waiting_producers = 0
   - waiting_producers > 0 -> waiting_consumers = 0
   - waiting_producers > 0 -> items_in_stream = capacity

   Step 3: consumer cancellation

   To cancel, a consumer increments [items] and marks its cell as cancelled.
   This is as if a producer appeared and added a cancelled token to the stream,
   which the consumer then read, and we consider [waiting_producers] to be updated
   accordingly during the operation.

   It is possible for the consumer to be resumed after incrementing [items] and
   before managing to cancel its cell. In this case the cancellation operation
   fails and the change to [items] is reverted. Conceptually, this is done by
   adding a fake consumer to consume the cancel token from the fake producer,
   and since neither fake party actually allocates a cell, there is no risk of
   them getting resumed by someone else.

   We have to be careful with cancellation to ensure that [items_in_stream]
   cannot exceed [capacity]. Consider a stream with capacity 0:

   1. A producer increments [items] to 1 and prepares to add itself to [overflow].
   2. A consumer decrements [items] to 0, writes a wakeup-token to [overflow],
      and adds itself to [main].
   3. The consumer decides to cancel. It increments [items] back to 1 and
      marks its cell in [main] as cancelled.
   4. The producer finds the wake-up token and adds its value to [main].
   5. Now we have no producers, no consumers, and one item in a 0-capacity stream.

   To avoid this, we add a rule:

   - A consumer can only cancel by incrementing [items] to a non-positive value.
     (actually, any value no greater than [capacity] would be OK)

   This corresponds to saying that the producer that is conceptually inserting
   the cancel token can only do so if it doesn't need to suspend itself in [overflow].
   If that happened then the consumer wouldn't be able to get the cancelled token.

   Not being able to cancel in this case isn't a problem for the consumer because
   if [items >= 0] then it's about to be resumed anyway
   (since then [items_in_stream + waiting_producers >= waiting_consumers]).
   The only way for [items] to become smaller is if new consumers join,
   but they will be added to [main] after the consumer that tried to cancel,
   so they won't stop it from being resumed.

   Step 4: producer cancellation

   To cancel, a producer decrements [items] and marks its cell as cancelled.
   This is as if a consumer appeared and took this producer's value, and we
   consider [waiting_consumers] to be updated accordingly during the operation.

   As with consumers, the producer may get resumed while it is doing this.
   In that case, the cancel operation fails and [items] is reverted as usual.

   Producer cancellation provides another way for a client that had its cancel
   operation refused to fail to get a value.
   For example (for a 0-capacity stream):

   1. A producer increments [items] to 1 and is about to add itself to [overflow].
   2. Two consumers join, decrementing [items] to -1.
      The first therefore writes a wake-up token to [overflow].
   3. Both consumers try to cancel.
      The first increments [items] to [0] and is about to mark itself as cancelled.
      The second has its cancellation refused because there are enough items for it.
   4. The producer finally adds itself to [overflow], finds the token, and resumes
      the first consumer.
   5. The first consumer tries to set its cell to Cancelled, but finds the
      value instead. It aborts cancelling and takes the value.
   6. The second consumer is left waiting, even though it was refused cancellation.

   To avoid that we have a similar rule for producers:

   - A producer cannot cancel by decrementing [items] to a negative value.

   A producer will not be inconvenienced by this because if [items <= 0] then
   it must be in the process of being resumed. *)

module Main = struct
  module Cell = struct
    type 'a t =
      | Empty
      | Request of ('a -> unit)         (* A consumer waiting for a value. *)
      | Value of 'a                     (* A value waiting for a consumer *)
      | Cancelling
      | Cancelled
      | Finished

    let init = Empty

    let segment_order = 2

    let dump f = function
      | Empty -> Fmt.string f "Empty"
      | Request _ -> Fmt.string f "Request"
      | Value _ -> Fmt.string f "Value"
      | Cancelling -> Fmt.string f "Cancelling"
      | Cancelled -> Fmt.string f "Cancelled"
      | Finished -> Fmt.string f "Finished"
  end

  module Cells = Cells.Make(Cell)

  let rec add t v =
    let cell = Cells.next_resume t in
    let rec aux () =
      match (Atomic.get cell : _ Cell.t) with
      | Empty ->
        (* No one was waiting. Add the value to the stream. *)
        if not (Atomic.compare_and_set cell Empty (Value v)) then aux ()
      | Request r ->
        (* There was a waiter for the value - pass it the value. *)
        Atomic.set cell Finished;
        r v
      | Cancelling ->
        if not (Atomic.compare_and_set cell Cancelling (Value v)) then aux ()
        else (
          (* The consumer will notice we got there first and the cancel fails. *)
        )
      | Cancelled ->
        (* The waiter has finished cancelling. Ignore it and resume the next one. *)
        add t v
      | Finished | Value _ -> assert false
    in
    aux ()
end

module Overflow = struct
  module Cell = struct
    type _ t =
      | In_transition                     (* The suspender will try to CAS this soon. *)
      | Request of (unit -> unit)         (* Waiting to be resumed. *)
      | Finished                          (* Resumed (or cancelled). *)

    let init = In_transition
    (* We only resume when we know another thread is suspended or in the process of suspending. *)

    let segment_order = 2

    let dump f = function
      | Request _ -> Fmt.string f "Request"
      | Finished -> Fmt.string f "Finished"
      | In_transition -> Fmt.string f "In_transition"
  end

  module Cells = Cells.Make(Cell)

  type cell = unit Cell.t

  let suspend t =
    Cells.next_suspend t
end

type 'a t = {
  items : int Atomic.t;
  (* Number of items in the queue if all producers finished adding and all consumers finished taking.
     If <0 then there are consumer waiting.
     If >capacity then there are producers waiter.
     Consumers can't cancel when this is >capacity; a value is coming! *)

  capacity : int;
  queue : 'a Main.Cells.t;                   (* Values waiting to be consumed or consumers waiting for values *)
  producers : unit Overflow.Cells.t;         (* Producers waiting for space *)
}

type 'a take_request = 'a t * 'a Main.Cells.segment * 'a Main.Cell.t Atomic.t
type 'a add_request = 'a t * unit Overflow.Cells.segment * unit Overflow.Cell.t Atomic.t * 'a

let add2 (request : _ add_request) k =
  let (t, _segment, cell, v) = request in
  let k () = Main.add t.queue v; k () in
  if Atomic.compare_and_set cell In_transition (Request k) then ()
  else k ()     (* Already resumed *)

let add t v : _ add_request option =
  let old_items = Atomic.fetch_and_add t.items 1 in
  if old_items < t.capacity then (
    Main.add t.queue v;
    None
  ) else (
    (* We must wait *)
    let segment, cell = Overflow.suspend t.producers in
    Some (t, segment, cell, v)
  )

let take2 request k =
  let (_, _, cell) = (request : _ take_request) in
  if not (Atomic.compare_and_set cell Empty (Request k)) then (
    match Atomic.get cell with
    | Value v -> k v
    | _ -> assert false
  )

let rec wake_producer (t:_ t) =
  let cell = Overflow.Cells.next_resume t.producers in
  match Atomic.exchange cell Finished with
  | Request k -> k ()
  | Finished -> wake_producer t         (* Cancelled, wake next producer *)
  | In_transition -> ()

let take t : ('a, 'a take_request) result =
  let prev_items = Atomic.fetch_and_add t.items (-1) in
  if prev_items > t.capacity then wake_producer t;
  let (segment, cell) = Main.Cells.next_suspend t.queue in
  match Atomic.get cell with
    | Empty -> Error (t, segment, cell)
    | Value v -> Atomic.set cell Finished; Ok v
    | Request _ | Cancelled | Cancelling | Finished ->
      (* These are unreachable from the previously-observed non-Empty state
         without us taking some action first *)
      assert false

let cancel_take (t, segment, cell) =
  match (Atomic.get cell : _ Main.Cell.t) with
  | Request _ as old ->
    (* Conceptually, cancelling is as if a producer showed up and resumed us with a special
       Cancelled value. *)
    let rec aux () =
      (* Simulate a producer adding a cancel token to the stream: *)
      let items = Atomic.get t.items in
      if items >= 0 then (
        (* There's no point cancelling because there are enough items for us to continue,
           and cancelling now might cause complications. *)
        false
      ) else if Atomic.compare_and_set t.items items (items + 1) then (
        (* Our fake provider is now committed to adding an item to the stream.
           We'll try to place it in our cell: *)
        if Atomic.compare_and_set cell old Cancelled then (
          (* The usual case. We received the fake token and have successfully cancelled. *)
          Main.Cells.cancel_cell segment;
          true
        ) else (
          (* Another producer arrived at the same time and resumed us instead!
             That's fine (we'll abort the cancel and use the value), but our
             fake provider still needs to put the fake token in the stream.
             To deal with that, we'll introduce a fake consumer...
             (same initial steps as in [take]) *)
          let prev_items = Atomic.fetch_and_add t.items (-1) in
          if prev_items > t.capacity then wake_producer t;
          (* At this point our fake producer needs to allocate a cell and put the
             cancel token in it, and our fake consumer needs to allocate a cell and
             read a token. We'll optimise this by doing nothing. *)
          false
        )
      )
      else aux ()
    in
    aux ()
  | Value _ | Finished -> false     (* We got resumed first *)
  | Cancelling | Cancelled -> invalid_arg "Already cancelled!"
  | Empty -> assert false

(* XXX: can't cancelling an add cause a consumer to miss a value it was relying on? *)
let cancel_add (request : _ add_request) =
  let (t, segment, cell, _v) = request in
  match (Atomic.get cell : Overflow.cell) with
  | Request _ as old ->
    let rec aux () =
      (* Create a fake consumer to read our value *)
      let items = Atomic.get t.items in
      if items <= 0 then (
        (* There's no point cancelling because there are enough consumers for us to continue,
           and cancelling now might cause complications. *)
        false
      ) else (
        if Atomic.compare_and_set t.items items (items - 1) then (
          (* Fake consumer ready to create a new cell. We'll transfer our value to it... *)
          if Atomic.compare_and_set cell old Finished then (
            (* Success. The fake consumer read our value and we can resume.
               (no need to create the cell for real) *)
            Overflow.Cells.cancel_cell segment;
            true
          ) else (
            (* Another consumer woke us first. Now we have a fake consumer we need to satisfy.
               Add fake producer and have it send a fake token to the fake consumer. *)
            Atomic.incr t.items;
            (* It doesn't matter whether we're now over capacity or not. We consume the token either way. *)
            false
          )
        ) else aux ()
      )
    in
    aux ()
  | Finished -> false     (* We got resumed first *)
  | In_transition -> invalid_arg "Already cancelling!"

let dump f t =
  Fmt.pf f "@[<v2>Stream (items=%d)@,@[<v2>Queue:@,%a@]@,@[<v2>Producers:@,%a@]@]"
    (Atomic.get t.items)
    Main.Cells.dump t.queue
    Overflow.Cells.dump t.producers

let create capacity =
  if capacity < 0 then raise (Invalid_argument "capacity < 0");
  {
    queue = Main.Cells.make ();
    producers = Overflow.Cells.make ();
    items = Atomic.make 0;
    capacity;
  }

let items t = Atomic.get t.items
