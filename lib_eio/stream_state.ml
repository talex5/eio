(* A lock-free bounded stream with cancellation, using Cells.

   Step 1: an unbounded queue without cancellation

   There is a sequence of cells ([main]) and a count ([items]).
   To add an item, a producer increments the counter and adds the value to main.
   To take an item, a consumer decrements the counter and removes the value.

   If there are more consumers than producers then [items] will go negative.
   The consumer will place a callback in the cell and when a producer later
   arrives it will pass the value to it.

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

   Conceptually, there are initially [capacity] slots. Adding a consumer
   creates a new slot, so there are always [waiting_consumers + capacity] slots
   in the system in total.

   When [items <= capacity], [items] owns [capacity - items] slots.
   When above [capacity], it owns no slots. Each value stored in [main]
   occupies a slot. This ensures that we never go over capacity.

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
   5. Now we have no producers, no consumers, and one item in a 0-capacity stream!

   To avoid this, we add a rule:

   - A consumer can only cancel by incrementing [items] to a non-positive value.
     (actually, any value no greater than [capacity] would be OK)

   This ensures that when the consumer increments [items] it always gets a slot,
   which it gives to the producer being resumed.

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

   - A producer cannot cancel by decrementing [items] to less than [capacity].

   A producer will not be inconvenienced by this because if [items <= capacity] then
   it must be in the process of being resumed.

   The above has not been formally verified (exercise for reader!). *)

module Main = struct
  module Cell = struct
    type 'a t =
      | Empty
      | Consumer of ('a -> unit)        (* A consumer waiting for a value. Removing this decrements [waiting_consumers]. *)
      | Value of 'a                     (* A value waiting for a consumer, in a slot. *)
      | Cancelled
      | Finished

    let init = Empty

    let segment_order = 2

    let dump f = function
      | Empty -> Fmt.string f "Empty"
      | Consumer _ -> Fmt.string f "Consumer"
      | Value _ -> Fmt.string f "Value"
      | Cancelled -> Fmt.string f "Cancelled"
      | Finished -> Fmt.string f "Finished"
  end

  module Cells = Cells.Make(Cell)

  (* The caller gives us a value and (conceptually) a slot representing space for it. *)
  let rec add t v =
    let cell = Cells.next_resume t in
    let rec aux () =
      match (Atomic.get cell : _ Cell.t) with
      | Empty ->
        (* No one was waiting. Add the value to the stream. *)
        if not (Atomic.compare_and_set cell Empty (Value v)) then aux ()
      | Consumer r ->
        (* There was a consumer waiting already.
           Remove it from the cell to allow [r] to be GC'd.
           This also decrements [waiting_consumers], destroying our slot. *)
        Atomic.set cell Finished;
        r v
      | Cancelled ->
        (* The waiter has finished cancelling. Ignore it and resume the next one. *)
        add t v
      | Finished | Value _ ->
        (* Unreachable because we haven't set the value yet. *)
        assert false
    in
    aux ()
end

module Overflow = struct
  (* This is essentially the same as the state model in {!Sem_state}. *)

  module Cell = struct
    type _ t =
      | In_transition                     (* The suspender will try to CAS this soon. *)
      | Request of (unit -> unit)         (* Waiting to be resumed (takes a slot). *)
      | Finished                          (* Resumed (or cancelled). *)
    (* If [Finished] is set by the consumer then it includes a slot until the producer
       sees it and takes it. Conceptually, this is a separate [Resume_with_slot] state,
       but we don't actually need to store that. *)

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

module Count = struct
  (* Owned slots: max 0 (t.capacity - borrowed_slots - items)
     Each consumer gives us a slot by decrementing items, but it may temporarily
     borrow it back (while cancelling). *)
  type t = int  (* XXX int63 *)

  let zero = 0

  let one_item = 1 lsl 16
  let items t = t asr 16
  let borrowed_slots t = t land 0xffff

  let pred_items t = t - one_item

  let succ_borrowed = succ

  let fetch_and_incr_items a =
    Atomic.fetch_and_add a one_item

  let fetch_and_decr_items a =
    Atomic.fetch_and_add a (-one_item)

  let fetch_and_decr_borrowed a =
    Atomic.fetch_and_add a (-1)

  let incr_items_and_decr_borrowed a =
    ignore (Atomic.fetch_and_add a (one_item - 1) : int)

  let has_overflow t ~capacity =
    items t > capacity - borrowed_slots t

  let has_space t ~capacity =
    items t < capacity - borrowed_slots t

  let pp f t =
    Fmt.pf f "items=%d,borrows=%d" (items t) (borrowed_slots t)
end

type 'a t = {
  count : Count.t Atomic.t;
  capacity : int;
  queue : 'a Main.Cells.t;                   (* Values waiting to be consumed or consumers waiting for values *)
  producers : unit Overflow.Cells.t;         (* Producers waiting for space *)
}

type 'a take_request = 'a t * 'a Main.Cells.segment * 'a Main.Cell.t Atomic.t
type 'a add_request = 'a t * unit Overflow.Cells.segment * Overflow.cell Atomic.t * 'a

let incr_items t =
  let old = Count.fetch_and_incr_items t.count in
  if Count.has_space old ~capacity:t.capacity then `Got_slot
  else `Full

(* Takes a slot, since it may need to create free space in [main]. *)
let decr_items t =
  let old = Count.fetch_and_decr_items t.count in
  if Count.has_overflow old ~capacity:t.capacity then `Slot_refused
  else `Slot_accepted

(* Like [decr_items], but just returns [false] if it would need a slot. *)
let rec decr_items_if_overflow t =
  let cur = Atomic.get t.count in
  if Count.has_overflow cur ~capacity:t.capacity then (
    if Atomic.compare_and_set t.count cur (Count.pred_items cur) then true
    else decr_items_if_overflow t
  ) else false

(* Used by the consumer when cancelling.
   Effectively, we are reducing [capacity].
   If we need a slot, the operation just fails. *)
let rec borrow_slot t =
  let cur = Atomic.get t.count in
  (* We could use [Count.has_space] here and allow cancelling in more places,
     but it isn't necessary and slows down the stress tests. Instead, we ensure
     there are still enough certainly-not-cancelling consumers to consume all
     values. *)
  if Count.items cur + Count.borrowed_slots cur < 0 then (
    if Atomic.compare_and_set t.count cur (Count.succ_borrowed cur) then true
     else borrow_slot t
  ) else false

let unborrow_slot t =
  let old = Count.fetch_and_decr_borrowed t.count in
  if Count.has_overflow old ~capacity:t.capacity then `Slot_refused
  else `Slot_accepted

(* Like [incr_items], but since we're unborrowing at the same time this doesn't
   change the overall number of slots held by [t]. *)
let unborrow_slot_incr_items t =
  Count.incr_items_and_decr_borrowed t.count

(* Need to decrement [waiting_producers] before resuming. *)
let add2 (request : _ add_request) k =
  let (t, _segment, cell, v) = request in
  let k () =
    (* We increment [items_in_stream] and decrement [waiting_producers].
       Our caller passed us a slot, which we use to write the value. *)
    Main.add t.queue v;
    k ()
  in
  if Atomic.compare_and_set cell In_transition (Request k) then ()
  else (
    (* The slot must have contained Resume_with_slot (but we represent that as Finished,
       so no need to do anything). We have therefore collected a slot from a
       consumer which decided to resume us. *)
    k ()
  )

let add t v : _ add_request option =
  match incr_items t with
  | `Got_slot ->
    (* We incremented [items] and [items_in_stream].
       We took a slot from [items], which we'll use to store the value: *)
    Main.add t.queue v;
    None
  | `Full ->
    (* We must wait (no slot available). We incremented [items] and [waiting_producers]. *)
    let segment, cell = Overflow.suspend t.producers in
    Some (t, segment, cell, v)  (* Must call [add2] *)

(* We have previously incremented [waiting_consumers].
   We need to reverse that when resuming. *)
let take2 request k =
  let (_, _, cell) = (request : _ take_request) in
  if not (Atomic.compare_and_set cell Empty (Consumer k)) then (
    match Atomic.exchange cell Finished with
    | Value v ->
      (* We decrement [waiting_consumers] and [items_in_stream].
         We recovered a free slot and destroyed it by resuming. *)
      k v
    | _ -> assert false
  )

(* We have a slot to pass on to a producer. *)
let rec wake_producer (t:_ t) =
  let cell = Overflow.Cells.next_resume t.producers in
  match Atomic.exchange cell Finished with
  | Request k -> k ()                   (* Pass the slot to [k] *)
  | Finished -> wake_producer t         (* Cancelled, wake next producer *)
  | In_transition -> ()                 (* The slot is with the Finished (Resume_with_slot) value *)

let take t : ('a, 'a take_request) result =
  (* We decrement [items] and increment [waiting_consumers], producing a new slot: *)
  begin match decr_items t with
    | `Slot_accepted -> ()
    | `Slot_refused ->
      (* If the stream was above capacity, pass the new slot to the next producer.
         Otherwise, [t.items] took the new slot. *)
      wake_producer t;
  end;
  let (segment, cell) = Main.Cells.next_suspend t.queue in
  match Atomic.get cell with
  | Empty -> Error (t, segment, cell) (* Must call [take2] later *)
  | Value v ->
    (* We decrement [items_in_stream] and [waiting_producers]. *)
    Atomic.set cell Finished;
    (* We recovered a slot by removing the value and destroyed it by
       decrementing [waiting_consumers]. *)
    Ok v
  | Consumer _ | Cancelled | Finished ->
    (* These are unreachable from the previously-observed non-Empty state
       without us taking some action first *)
    assert false

let take_nonblocking t : 'a option =
  (* We decrement [items] and increment [waiting_consumers], producing a new slot: *)
  begin match decr_items t with
    | `Slot_accepted -> ()
    | `Slot_refused ->
      (* If the stream was above capacity, pass the new slot to the next producer.
         Otherwise, [t.items] took the new slot. *)
      wake_producer t;
  end;
  let has_value (cell : _ Main.Cell.t Atomic.t) =
    match Atomic.get cell with
    | Empty -> false    (* Reject *)
    | Value _ -> true
    | Consumer _ | Cancelled | Finished ->
      (* Another consumer got here first. It will be rejected on the CAS. *)
      true
  in
  match Main.Cells.next_suspend_if t.queue has_value with
  | None -> None
  | Some (_segment, cell) ->
    match Atomic.get cell with
    | Value v ->
      Atomic.set cell Finished;
      Some v
    | Empty | Consumer _ | Cancelled | Finished ->
      (* These are unreachable from the previously-observed non-Empty state
         without us taking some action first *)
      assert false

let cancel_take (t, segment, cell) =
  match (Atomic.get cell : _ Main.Cell.t) with
  | Consumer _ as old ->
    if borrow_slot t then (
      (* The stream's capacity has been reduced. We can use the borrowed slot to cancel. *)
      if Atomic.compare_and_set cell old Cancelled then (
        (* The usual case - we cancelled successfully. *)
        unborrow_slot_incr_items t;
        Main.Cells.cancel_cell segment;
        true
      ) else (
        (* We got resumed first. Return the slot and abort the cancel. *)
        begin match unborrow_slot t with
          | `Slot_accepted -> ()
          | `Slot_refused -> wake_producer t
        end;
        false
      );
    ) else (
      (* There is no free space in the queue, so we can't cancel.
         We're being resumed at the moment anyway. *)
      false
    )
  | Value _ | Finished -> false     (* We got resumed first *)
  | Cancelled -> invalid_arg "Already cancelled!"
  | Empty -> assert false

(* Cancelling a producer is a bit easier. If we get resumed while cancelling we just pass the slot to another
   producer and continue cancelling. Consumers can't do that because the values might get out of order, but
   we don't promise anything about the order of waiting producers. *)
let cancel_add (request : _ add_request) =
  let (t, segment, cell, _v) = request in
  match (Atomic.get cell : Overflow.cell) with
  | Request r as old ->
    if Atomic.compare_and_set cell old In_transition then (
      if decr_items_if_overflow t then (
        (* At this point we have cancelled and will not deliver our value to anyone. *)
        if Atomic.compare_and_set cell In_transition Finished then (
          Overflow.Cells.cancel_cell segment
        ) else (
          (* Someone tried to resume us after we were cancelled but before we updated the cell.
             Pass the slot on to another producer. *)
          wake_producer t
        );
        true
      ) else (
        (* There's no point cancelling because there are enough consumers for us to continue,
           and cancelling now might cause complications. *)
        if not (Atomic.compare_and_set cell In_transition old) then (
          (* Someone tried to resume us while we were busy. Do it now. *)
          r ()
        );
        false
      )
    ) else false          (* We got resumed first *)
  | Finished -> false     (* We got resumed first *)
  | In_transition -> assert false

let dump f t =
  Fmt.pf f "@[<v2>Stream (%a)@,@[<v2>Queue:@,%a@]@,@[<v2>Producers:@,%a@]@]"
    Count.pp (Atomic.get t.count)
    Main.Cells.dump t.queue
    Overflow.Cells.dump t.producers

let create capacity =
  if capacity < 0 then raise (Invalid_argument "capacity < 0");
  {
    queue = Main.Cells.make ();
    producers = Overflow.Cells.make ();
    count = Atomic.make Count.zero;
    capacity;
  }

let items t = Count.items (Atomic.get t.count)
