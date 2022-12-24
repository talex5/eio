module Make(Cell : S.CELL) = struct
  let cells_per_segment = 1 lsl Cell.segment_order
  let segment_mask = cells_per_segment - 1

  (* An index identifies a cell. It is a pair of the segment ID and the offset
     within the segment, packed into a single integer so we can increment it
     atomically. *)
  module Index : sig
    type t
    type segment_id = int

    val of_segment : segment_id -> t
    (** [of_segment x] is the index of the first cell in segment [x]. *)

    val segment : t -> segment_id
    val offset : t -> int

    val zero : t
    val succ : t -> t

    val next : t Atomic.t -> t
  end = struct
    type t = int
    type segment_id = int

    let segment t = t lsr Cell.segment_order
    let of_segment id = id lsl Cell.segment_order

    let offset t = t land segment_mask

    let zero = 0
    let succ = succ

    let next t_atomic =
      Atomic.fetch_and_add t_atomic (+1)
  end

  (** A pair with counts for the number of cancelled cells in a segment and the
      number of pointers to it, packed as an integer so it can be adjusted atomically. *)
  module Count : sig
    type t = private int

    val zero : t        (* 0 pointers *)
    val init : t        (* 2 pointers *)
    val removed : t     (* All cancelled and no pointers *)

    val pointers : t -> int
    val cancelled : t -> int

    val incr_cancelled : t Atomic.t -> bool
    (* Increment the count of cancelled cells. Returns [true] if all are now cancelled
       and there are no pointers to this segment. *)

    val try_inc_pointers : t Atomic.t -> bool
    (* Atomically increment the pointers count if the value isn't [removed].
       Returns [true] on success, or [false] if the segment was removed first. *)

    val dec_pointers : t Atomic.t -> bool
    (* Decrement the pointers count. Returns [true] if the value is now [removed]. *)

    val dump : t Fmt.t
  end = struct
    type t = int

    let make ~pointers ~cancelled = (pointers lsl 16) lor cancelled
    let zero = make ~pointers:0 ~cancelled:0
    let init = make ~pointers:2 ~cancelled:0

    let pointers t = t lsr 16
    let cancelled t = t land 0xffff

    let dump f t =
      Fmt.pf f "pointers=%d, cancelled=%d" (pointers t) (cancelled t)

    let removed = make ~pointers:0 ~cancelled:cells_per_segment

    let incr_cancelled t_atomic =
      Atomic.fetch_and_add t_atomic 1 = cells_per_segment - 1

    let rec try_inc_pointers t_atomic =
      let cur = Atomic.get t_atomic in
      if cur = removed then false
      else (
        if Atomic.compare_and_set t_atomic cur (cur + (1 lsl 16)) then true
        else try_inc_pointers t_atomic
      )

    let dec_pointers t_atomic =
      Atomic.fetch_and_add t_atomic (-1 lsl 16) = removed + (1 lsl 16)
  end

  module Segment : sig
    type 'a t

    val id : _ t -> int

    val get : 'a t -> int -> 'a Cell.t Atomic.t

    val try_inc_pointers : _ t -> bool
    (* Atomically increment the pointers count if the segment isn't removed.
       Returns [true] on success, or [false] if the segment was removed first. *)

    val dec_pointers : _ t -> unit
    (* Decrement the pointers count, removing the segment if it is no longer
       needed. *)

    val find : 'a t -> int -> 'a t

    val clear_prev : _ t -> unit

    val make_init : unit -> 'a t

    val validate : 'a t -> suspend:'a t -> resume:'a t -> unit

    val dump_list : label:Index.t Fmt.t -> 'a t Fmt.t

    val on_cancelled_cell : _ t -> unit
  end = struct
    type 'a t = {
      id : Index.segment_id;
      count : Count.t Atomic.t;
      cells : 'a Cell.t Atomic.t array;
      prev : 'a t option Atomic.t;      (* None if first, or [prev] is no longer needed *)
      next : 'a t option Atomic.t;      (* None if not yet created *)
    }

    let id t = t.id

    let get t i = Array.get t.cells i

    let pp_id f t = Fmt.int f t.id

    let dump_cells ~label f t =
      let idx = ref (Index.of_segment t.id) in
      for i = 0 to Array.length t.cells - 1 do
        Fmt.pf f "@,%a" Cell.dump (Atomic.get t.cells.(i));
        label f !idx;
        idx := Index.succ !idx
      done

    let rec dump_list ~label f t =
      ignore label;
      Fmt.pf f "@,@[<v2>Segment %d (prev=%a, %a):%a@]"
        t.id
        (Fmt.Dump.option pp_id) (Atomic.get t.prev)
        Count.dump (Atomic.get t.count)
        (dump_cells ~label) t;
      match Atomic.get t.next with
      | Some next -> dump_list ~label f next
      | None ->
        Fmt.pf f "@,End";
        label f (Index.of_segment (t.id + 1))

    let next t =
      match Atomic.get t.next with
      | Some s -> s
      | None ->
        let next = {
          id = t.id + 1;
          count = Atomic.make Count.zero;
          cells = Array.init cells_per_segment (fun (_ : int) -> Atomic.make Cell.init);
          next = Atomic.make None;
          prev = Atomic.make (Some t);
        } in
        if Atomic.compare_and_set t.next None (Some next) then next
        else Atomic.get t.next |> Option.get

    let removed t =
      Atomic.get t.count = Count.removed

    (* Get the previous segment, if any. *)
    let rec alive_prev t =
      match Atomic.get t.prev with
      | Some prev when removed prev -> alive_prev prev
      | x -> x

    (* Get the next segment, if any. *)
    let alive_next t =
      let next = Atomic.get t.next |> Option.get in
      let rec live x =
        if removed x then (
          match Atomic.get x.next with
          | Some next -> live next
          | None -> x
        ) else x
      in
      live next

    let rec remove t =
      if Atomic.get t.next = None then () (* Can't remove tail *)
      else (
        let prev = alive_prev t
        and next = alive_next t in
        Atomic.set next.prev prev;
        Option.iter (fun prev -> Atomic.set prev.next (Some next)) prev;
        if removed next && Atomic.get next.next <> None then remove t
        else (
          match prev with
          | None -> ()
          | Some prev ->
            if removed prev then remove t
            else ()
        )
      )

    let try_inc_pointers t =
      Count.try_inc_pointers t.count

    let dec_pointers t =
      if Count.dec_pointers t.count then remove t

    let on_cancelled_cell t =
      if Count.incr_cancelled t.count then remove t

    (* Get segment [id], searching from [start], or a later one
       if it is completely cancelled. *)
    let rec find start id =
      if start.id >= id && not (removed start) then start
      else find (next start) id

    let make_init () =
      {
        id = 0;
        count = Atomic.make Count.init;
        cells = Array.init cells_per_segment (fun (_ : int) -> Atomic.make Cell.init);
        next = Atomic.make None;
        prev = Atomic.make None;
      }

    (* Note: this assumes the system is at rest (no operations in progress). *)
    let rec validate t ~suspend ~resume ~seen_pointers =
      let expected_pointers =
        (if t == suspend then 1 else 0) +
        (if t == resume then 1 else 0)
      in
      let count = Atomic.get t.count in
      let seen_pointers = seen_pointers + Count.pointers count in
      if Count.pointers count <> expected_pointers then
        Fmt.failwith "Bad pointer count!";
      assert (Count.cancelled count >= 0 && Count.cancelled count <= cells_per_segment);
      if Count.cancelled count = cells_per_segment then assert (Count.pointers count > 0);
      match Atomic.get t.next with
      | None -> assert (seen_pointers = 2);
      | Some next ->
        begin match Atomic.get next.prev with
          | None -> assert (resume.id >= next.id)
          | Some t2 -> assert (resume.id < next.id && t == t2)
        end;
        validate next ~suspend ~resume ~seen_pointers

    let validate = validate ~seen_pointers:0

    let clear_prev t =
      Atomic.set t.prev None
  end

  module Position : sig
    type 'a t

    val of_segment : 'a Segment.t -> 'a t

    val next : clear_prev:bool -> 'a t -> 'a Segment.t * int
    (* [next t] returns the segment and offset of [t], and atomically increments it.
       If [t]'s segment is all cancelled and no longer exists it will skip it and retry. *)

    val index : _ t -> Index.t

    (* For debugging: *)
    val segment : 'a t -> 'a Segment.t
  end = struct
    type 'a t = {
      segment : 'a Segment.t Atomic.t;
      idx : Index.t Atomic.t;
    }

    let segment t = Atomic.get t.segment
    let index t = Atomic.get t.idx

    let of_segment segment =
      {
        segment = Atomic.make segment;
        idx = Atomic.make Index.zero;
      }

    (* Set [t.segment] to [target] if it's ahead of us.
       Returns [false] if [target] gets removed first. *)
    let rec move_forward t (target : _ Segment.t) =
      let cur = Atomic.get t.segment in
      if Segment.id cur >= Segment.id target then true          (* XXX: wrapping? Use Int63? *)
      else (
        if not (Segment.try_inc_pointers target) then false     (* target already removed *)
        else (
          if Atomic.compare_and_set t.segment cur target then (
            Segment.dec_pointers cur;
            true
          ) else (
            (* Concurrent update of [t]. Undo ref-count changes and retry. *)
            Segment.dec_pointers target;
            move_forward t target
          )
        )
      )

    let rec find_and_move_forward t start id =
      let target = Segment.find start id in
      if move_forward t target then target
      else find_and_move_forward t start id

    let rec next ~clear_prev t =
      let r = Atomic.get t.segment in
      let i = Index.next t.idx in
      let id = Index.segment i in
      let s = find_and_move_forward t r id in
      if clear_prev then Segment.clear_prev s;
      if Segment.id s = id then (
        (s, Index.offset i)
      ) else (
        (* The segment we wanted contains only cancelled cells.
           Update the index to jump over those cells. *)
        let s_index = Index.of_segment (Segment.id s) in
        ignore (Atomic.compare_and_set t.idx (Index.succ i) s_index : bool);
        next ~clear_prev t
      )
  end

  type 'a t = {
    resume : 'a Position.t;
    suspend : 'a Position.t;
  }

  let next_suspend t = Position.next t.suspend ~clear_prev:false
  let next_resume t = Position.next t.resume ~clear_prev:true

  let make () =
    let start = Segment.make_init () in
    {
      resume = Position.of_segment start;
      suspend = Position.of_segment start;
    }

  let validate t =
    let suspend = Position.segment t.suspend in
    let resume = Position.segment t.resume in
    let start =
      if Segment.id suspend< Segment.id resume then suspend
      else resume
    in
    Segment.validate start ~suspend ~resume

  let dump f t =
    let suspend = Position.index t.suspend in
    let resume = Position.index t.resume in
    let start =
      if suspend < resume then t.suspend
      else t.resume
    in
    let label f i =
      if i = suspend then Format.pp_print_string f " (suspend)";
      if i = resume then Format.pp_print_string f " (resume)";
    in
    Format.fprintf f "@[<v2>cqs:%a@]" (Segment.dump_list ~label) (Position.segment start)
end
