(* A lock-free bounded stream with cancellation, using Cells.

   Step 1: an unbounded queue without cancellation

   There is a sequence of cells ([main]) and a count ([items]).
   To add an item, a producer increments the counter and adds the value to main.
   To take an item, a consumer decrements the counter and removes the value.

   If there are more consumers than producers then [items] will go negative.
   The consumer will place a callback in the cell and when a producer later
   arrives it will pass the value to it.

   As usual with Cells, the producer and consumer may get to the cell in
   either order (and not necessarily the same order in which they updated
   [items]). This is not a problem, they just have to handle both cases.

   Step 2: a bounded queue

   For a bounded queue, producers may need to wait for space. Since Cells
   only allows one party to cancel, we need a second queue [overflow] for
   this.

   Conceptually, there are "trays" and "tokens" (the same number of each).
   Initially there are [capacity] trays (and token).

   Each producer takes a token and then places its item in the next tray, along
   with the token.

   Each consumer arrives with its own additional tray and token and adds both
   to the queue. It then takes the next free tray in the queue and, once it's
   full, takes the tray away, along with the tray's item and token.

   Having consumers bring their own trays is necessary to support 0-capacity
   queues (where otherwise there would be no trays at all). It should also be
   slightly faster in general. For example, if we have a queue with capacity
   10 and 5 consumers start waiting, we can allow 15 producers to write values
   without waiting.

   The number of free tokens in the queue can be calculated from [items], and
   is therefore updated atomically with that:

     free_tokens = max 0 (capacity - items)

   When a producer increments [items] to be greater than [capacity], it
   therefore does not get a token and instead adds itself to [overflow] to wait
   for space.

   When a consumer decrements [items] from being over [capacity], it is
   therefore unable to add its token to the queue and instead uses it to wake
   one producer.

   Consumers and producers never suspend while holding a token. Therefore, when
   the system is idle the number of free tokens is the total number of tokens
   minus the number of filled trays. The total number of tokens is the capacity
   plus the number of waiting consumers.

   Thus, when idle:

   - capacity + waiting_consumers - filled_trays = max 0 (capacity - items)

   and so:

   - waiting_consumers + capacity = filled_trays     if items >  capacity
   - waiting_consumers + items    = filled_trays     if items <= capacity

   Since filling a tray wakes a consumer if there is one, either
   waiting_consumers or filled_trays is zero when idle, and so:

   - filled_trays = capacity              if items >  capacity
   - items = filled_trays                 if items <= capacity and waiting_consumers = 0
   - waiting_consumers = -items           if items <= capacity and waiting_consumers > 0

   In particular, it is therefore not possible to have unfilled trays when items > capacity
   and the system is idle.

   Step 3: consumer cancellation

   To cancel, a consumer removes one empty tray and one free token and leaves
   with them, reversing the effect of it joining the stream. If there are no
   free tokens then all trays are in the process of being resumed (either by
   getting an item or by becoming cancelled). Therefore the consumer is about
   to be resumed anyway and can simply ignore the cancellation request.

   The tricky part is doing this atomically. Removing a tray means setting its cell
   to Cancelled, while taking a token means incrementing items. Here is an example
   of how this could go wrong:

   1. A producer increments [items] to 1 and prepares to add itself to [overflow].
   2. A consumer decrements [items] to 0, writes a wakeup-token to [overflow],
      and adds itself to [main].
   3. The consumer decides to cancel. It increments [items] back to 1 and
      marks its cell in [main] as cancelled.
   4. The producer finds the wake-up token and adds its value to [main].
   5. Now we have no producers, no consumers, and one item in a 0-capacity stream!

   On the other hand, having a cancelled tray while the token is still
   available means a provider might add its item to [main] and take the queue
   over capacity. We can't re-add the item after the producer has resumed
   because then the item will arrive out of order. e.g the producer might see
   that it wrote 1 then 2 to the stream, but a consumer might read 2 and then 1
   because a second consumer got the 1 while cancelling and then re-added it.

   So we first take a free token *without* incrementing items. This requires
   adjusting free_tokens to allow briefly reserving a token:

     free_tokens = max 0 (capacity - items - reserved_tokens)

   We can't have more [reserved_tokens] than cancelling domains, so we can pack
   [reserved_tokens] and [items] together in an int63, allowing it to be
   updated atomically.

   To cancel then, a consumer:

   1. Marks its tray as Cancelling.
   2. Takes a token by atomically incrementing [items] (if possible).
   3. Marks its tray as Cancelled.

   (1) will fail if a provider filled our tray first. In that case we just
   ignore the cancellation request and take the item.

   (2) will fail if there are no free tokens. We know that for every token
   taken, exactly one tray will be either filled or cancelled. Therefore, if
   all tokens have been taken then all trays will be filled or cancelled. Since
   no one else can cancel our tray, it is about to be filled with an item. We
   revert the change to the cell's state (if possible) and abort cancellation.
   A provider might have wanted to fill our tray while this was happening.
   Since we didn't decide to cancel in the end, we accept the item and allow
   the provider to resume.

   (3) will fail if a provider wanted to fill our tray but wasn't sure if we
   were had committed to cancelling or not. We are now committed, and so
   tell the provider to use its permit on another tray.

   Some of these edge cases could perhaps be optimised a bit by spinning for a
   bit and hoping the cell changes. This might help avoid producers losing their
   place in the overflow queue (we don't actually promise anything about the
   order there anyway, but it's nice to be fair when we can).

   Step 4: producer cancellation

   Cancelling a producer is very similar to cancelling a consumer.
   Again we need an extra "undecided" step first, because we need to mark
   the provider cell as cancelled and decrement [items], and we can't do that
   atomically.

   We can't decide to cancel before decrementing [items] because consumers
   use that to know whether it's time to send a token to the overflow queue.
   We can't mark our cell as cancelled before deciding to cancel either,
   because then consumers would just skip it.

   Therefore, a producer waiting in the overflow queue cancels as follows:

   1. Mark its cell as In_transition (cancelling).
   1. Decrement [items] (if this would not release a token).
   2. Mark its cell as Finished (cancelled).

   (1) will fail if we got passed a token first. In that case we can certainly
   write our value to [main], so we do that instead and abort cancelling.

   (2) will fail if the [main] queue is at or under capacity, in which case
   we're not supposed to be on the [overflow] queue anyway and are in the
   process of being resumed. In that case, the provider just aborts the
   cancellation. It will write its value to a waiting consumer soon.
   If we decide not to cancel we CAS back to the waiting state. If that
   fails because we got a token, we use it to resume as usual for a
   non-cancelled provider.

   (3) will fail if a consumer wrote a token there first. Since we're already
   committed to cancelling at this point, we pass the token to the next
   producer. We know there is one because we and the consumer both successfully
   decremented [items] while it was overflowed.

   Step 5: non-blocking add

   This is quite simple:

   1. Increment [items] if that would get us a token.
   2. Use the token to write to the [main] queue.

   If (1) fails then the queue is full and the operation fails, as desired.

   If (2) fails then the consumer may be in the Cancelling state. If so,
   we CAS it to Cancelled (it wanted to cancel anyway) and try the next consumer.
   Conceptually, we successfully put a null value in the tray and the
   consumer successfully received it. Only a cancelling consumer can get a null
   value this way, and it doesn't care since it wants to cancel anyway and it
   can just report to the application that cancellation succeeded.

   Step 6: non-blocking take

   This is tricky. Consider this case:

   1. We have a stream with capacity=2.
   2. Two producers arrive, allocating two cells in [main].
   3. The second producer writes [2] to the second cell and resumes itself.
   4. A consumer does a non-bocking read. It is assigned the first cell.

   At this point the second producer is sure it wrote an item (2) to the queue,
   and might even mention this to the (single) consumer. What should happen if
   the consumer now does a non-blocking read? The consumer may get [None], even
   though it knows the stream contains a 2!

   Instead, the consumer will write Cancelled into the cell (which it knows is
   about to be filled). This will cause the producer to try another cell.
   But the consumer needs a spare token for this.

   To deal with this, we have two different paths depending on whether the queue
   is 0-capacity or not:

   If capacity > 0:

   1. The non-blocking consumer decrements [items] if positive, borrowing a token at the same time.
   2. It takes the value in the cell, if available, and returns the borrowed token.
   3. If not, it marks its cell as Cancelled.
   4. It increments [items], returning the borrowed token.

   If capacity = 0, a non-blocking take must always wake a producer, so we only look at [overflow]:

   1. The non-blocking consumer decrements [items] if positive, therefore keeping its token.
   2. It uses the token to wake one producer, resuming one cell in [overflow].
   3. It takes the value from the producer's cell, if available.

   (1) will fail if [items <= 0]. In that case there is nothing to take and it returns [None].

   (3) will fail if the producer hasn't yet written its value. In that case,
   the consumer marks the cell as Reject. The producer will see this when it
   writes is value and pick another cell. Conceptually, the client accepted the
   value and the producer will add another one to the stream.

   In practise, we can optimise things a bit by spinning a few times when the cell is in the
   process of being written. However, rejecting the cell is necessary

   The above has not been formally verified (exercise for reader). *)

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
