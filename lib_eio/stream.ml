[@@@warning "-27"]

module State = Stream_state

type 'a t = {
  id : Ctf.id;
  state : 'a State.t;
  capacity : int;
}

let create capacity =
  assert (capacity >= 0);
  let id = Ctf.mint_id () in
  Ctf.note_created id Ctf.Stream;
  {
    id;
    state = State.create capacity;
    capacity;
  }

let add t item =
  match State.add t.state item with
  | None -> ()          (* Added without waiting *)
  | Some request ->
    Suspend.enter_unchecked (fun ctx enqueue ->
        State.add2 request (fun () -> enqueue (Ok ()));
        match Fiber_context.get_error ctx with
        | Some ex ->
          if State.cancel_add request then enqueue (Error ex);
          (* else already resumed *)
        | None ->
          Fiber_context.set_cancel_fn ctx (fun ex ->
              if State.cancel_add request then enqueue (Error ex)
              (* else already resumed *)
            )
      )

let take t =
  match State.take t.state with
  | Ok v -> v
  | Error request ->
    let v = Suspend.enter_unchecked (fun ctx enqueue ->
        State.take2 request (fun v -> enqueue (Ok v));
        match Fiber_context.get_error ctx with
        | Some ex ->
          if State.cancel_take request then enqueue (Error ex);
          (* else already resumed *)
        | None ->
          Fiber_context.set_cancel_fn ctx (fun ex ->
              if State.cancel_take request then enqueue (Error ex)
              (* else already resumed *)
            )
      ) in
    Ctf.note_read t.id;
    v

(* TODO: proper version *)
let take_nonblocking t =
  let v =
    match State.take t.state with
    | Ok v -> Some v
    | Error request ->
      let r = ref None in
      State.take2 request (fun v -> r := Some v);
      ignore (State.cancel_take request : bool);
      !r
  in
  Ctf.note_read t.id;
  v

let length t =
  assert false
(*   t.capacity - Sem_state.get_value t.state.free *)

let is_empty t = (length t = 0)

let dump f t = State.dump f t.state
