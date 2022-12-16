(* A lock-free queue of waiting fibers that should all be resumed at once.

   Based on "A formally-verified framework for fair synchronization in kotlin coroutines".
   See https://arxiv.org/pdf/2111.12682.pdf for more information.

   This implements the "simple" cancellation mode, which is suitable for
   broadcast operations where we just ignore waiters that cancel.

   The queue is an infinite sequence of cells with two pointers into it:
   "suspend" where the next waiter will wait and "resume" pointing at the first
   waiter to be resumed (if any).

   Each new waiter atomically increments the "suspend" pointer and writes
   a callback there. The waking fiber removes all the callbacks and calls them.
   In this version, "resume" never gets ahead of "suspend" (broadcasting just
   brings it up-to-date with the "suspend" pointer).

   Notes:

   - When the resume fiber runs, some of the cells reserved for callbacks might
     not yet have been filled. In this case, the resuming fiber just marks them
     as needing to be resumed. When the suspending fiber continues, it will
     notice this and continue immediately.

   - A suspended fiber can be cancelled, and we need to avoid leaking memory
     even if large numbers of fibers do this.
*)

module Cell = struct
  (* For any given cell, there are two actors running in parallel: the
     suspender and the resumer.

     The resumer only performs a single operation: resuming the cell's
     suspended fiber.

     The consumer waits to be resumed and, optionally, cancels.

     This means we only have three cases to think about:

     1. Consumer adds request (Empty -> Request).
        1a. Provider fulfills it (Request -> Resumed).
        1b. Consumer cancels it (Request -> Cancelled).
     2. Provider gets to cell first (Empty -> Resumed).

     The Resumed state should never been seen. It exists only to allow the
     request to be GC'd promptly. We could replace it with Empty, but having
     separate states is clearer for debugging. *)

  type _ t =
    | Request of (unit -> unit)
    | Cancelled
    | Resumed
    | Empty

  let init = Empty

  let segment_order = 2

  let dump f = function
    | Request _ -> Format.pp_print_string f "Request"
    | Empty -> Format.pp_print_string f "Empty"
    | Resumed -> Format.pp_print_string f "Resumed"
    | Cancelled -> Format.pp_print_string f "Cancelled"
end

module Cells = Cells.Make(Cell)

type t = unit Cells.t

type request = unit Cells.Segment.t * int

let resume t =
  let segment, offset = Cells.next_resume t in
  let cell = Cells.Segment.get segment offset in
  let rec retry () =
    let cur = Atomic.get cell in
    match cur with
    | Request r as cur ->
      (* The common case: we have a waiter for the value *)
      if Atomic.compare_and_set cell cur Resumed then r ()
      (* else it was cancelled at the same time; ignore *)
    | Empty ->
      (* The consumer has reserved this cell but not yet stored the request.
         We place a Value there and it wil handle it soon. *)
      if Atomic.compare_and_set cell Empty Resumed then
        ()              (* The consumer will deal with it *)
      else
        retry ()        (* The Request was added concurrently; use it *)
    | Cancelled -> ()
    | Resumed ->
      (* This states is unreachable because we (the provider) haven't added the value yet *)
      assert false
  in
  retry ()

let resume_all (t:t) =
  (* The resumer position needs to catch up to the suspend position. *)
  let suspend = Cells.Position.index t.suspend in
  (* XXX: not safe against concurrent resume_all! *)
  let rec aux () =
    if Cells.Position.index t.resume < suspend then (
      resume t; aux ()
    )
  in
  aux ()

let cancel (segment, offset) =
  let cell = Cells.Segment.get segment offset in
  match Atomic.get cell with
  | Request _ as old ->
    if Atomic.compare_and_set cell old Cancelled then (
      Cells.Segment.on_cancelled_cell segment;
      true
    ) else false          (* We got resumed first *)
  | Resumed -> false    (* We got resumed first *)
  | Cancelled -> invalid_arg "Already cancelled!"
  | Empty ->
    (* To call [cancel] the user needs a [request] value,
       which they only get once we've reached the [Request] state.
       These states are unreachable from [Request]. *)
    assert false

let suspend t k =
  let segment, offset = Cells.next_suspend t in
  let cell = Cells.Segment.get segment offset in
  match Atomic.get cell with
  | Empty ->
    if Atomic.compare_and_set cell Empty (Request k) then Some (segment, offset)
    else (
      match Atomic.get cell with
      | Resumed -> k (); None
      | _ -> assert false
    )
  | Resumed ->                      (* Resumed before we could add the waiter *)
    k ();
    None
  (* These are unreachable because they require us to have made a transition first: *)
  | Cancelled -> assert false
  | Request _ -> assert false        (* Only we can put a waiter at [i]! *)

let create () = Cells.make ()

let dump f t = Cells.dump f t
