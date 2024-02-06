module Lf_stack = struct
  type item = {
    owner : t;
    fn : (unit -> unit) Atomic.t;
  }

  and state =
    | Active of { head : item list; items : int }
    | Closed
  
  and t = {
    state : state Atomic.t;
    cancelled : int Atomic.t;
  }

  let close t =
    match Atomic.exchange t.state Closed with
    | Closed -> []
    | Active x -> x.head

  let rec push t fn =
    match Atomic.get t.state with
    | Closed -> None
    | Active state as prev ->
      let item = { owner = t; fn = Atomic.make fn } in
      let next = Active { head = item :: state.head; items = state.items + 1 } in
      if Atomic.compare_and_set t.state prev next then Some item
      else push t fn

  let cancelled () = failwith "Cancelled hook called!"

  let incr_cancelled t =
    match Atomic.get t.state with
    | Closed -> ()
    | Active state as prev ->
      let n_cancelled = Atomic.fetch_and_add t.cancelled 1 in
      if 2 * n_cancelled > state.items then (
        let rec aux () =
          let items = ref 0 in
          let cleaned = ref 0 in
          let head =
            state.head |> List.filter (fun item ->
                if Atomic.get item.fn == cancelled then (incr cleaned; false)
                else (incr items; true)
              )
          in
          let next = Active { head; items = !items } in
          if Atomic.compare_and_set t.state prev next then (
            ignore (Atomic.fetch_and_add t.cancelled (- !cleaned) : int);
          ) else aux ()
        in
        aux ()
      )

  let cancel item =
    let fn = Atomic.exchange item.fn cancelled in
    if fn == cancelled then ignore
    else (
      incr_cancelled item.owner;
      fn
    )

  let create () = {
    state = Atomic.make (Active { head = []; items = 0 });
    cancelled = Atomic.make 0;
  }

  let closed = {
    state = Atomic.make Closed;
    cancelled = Atomic.make 0;
  }
end

type t = {
  mutable fibers : int;         (* Total, including daemon_fibers and the main function *)
  mutable daemon_fibers : int;
  mutable exs : (exn * Printexc.raw_backtrace) option;
  on_release : Lf_stack.t;
  waiter : unit Single_waiter.t;              (* The main [top]/[sub] function may wait here for fibers to finish. *)
  cancel : Cancel.t;
}

type hook = Lf_stack.item
let null_hook : hook = { owner = Lf_stack.closed; fn = Atomic.make Lf_stack.cancelled }

let remove_hook item =
  ignore (Lf_stack.cancel item : unit -> unit)

let dump f t =
  Fmt.pf f "@[<v2>Switch %d (%d extra fibers):@,%a@]"
    (t.cancel.id :> int)
    t.fibers
    Cancel.dump t.cancel

let is_finished t = Cancel.is_finished t.cancel

(* Check switch belongs to this domain (and isn't finished). It's OK if it's cancelling. *)
let check_our_domain t =
  if is_finished t then invalid_arg "Switch finished!";
  if Domain.self () <> t.cancel.domain then invalid_arg "Switch accessed from wrong domain!"

(* Check isn't cancelled (or finished). *)
let check t =
  if is_finished t then invalid_arg "Switch finished!";
  Cancel.check t.cancel

let get_error t =
  Cancel.get_error t.cancel

let combine_exn ex = function
  | None -> ex
  | Some ex1 -> Exn.combine ex1 ex

(* Note: raises if [t] is finished or called from wrong domain. *)
let fail ?(bt=Printexc.get_callstack 0) t ex =
  check_our_domain t;
  t.exs <- Some (combine_exn (ex, bt) t.exs);
  try
    Cancel.cancel t.cancel ex
  with ex ->
    let bt = Printexc.get_raw_backtrace () in
    t.exs <- Some (combine_exn (ex, bt) t.exs)

let inc_fibers t =
  check t;
  t.fibers <- t.fibers + 1

let dec_fibers t =
  t.fibers <- t.fibers - 1;
  if t.daemon_fibers > 0 && t.fibers = t.daemon_fibers then
    Cancel.cancel t.cancel Exit;
  if t.fibers = 0 then
    Single_waiter.wake t.waiter (Ok ())

let with_op t fn =
  inc_fibers t;
  Fun.protect fn
    ~finally:(fun () -> dec_fibers t)

let with_daemon t fn =
  inc_fibers t;
  t.daemon_fibers <- t.daemon_fibers + 1;
  Fun.protect fn
    ~finally:(fun () ->
        t.daemon_fibers <- t.daemon_fibers - 1;
        dec_fibers t
      )

let or_raise = function
  | Ok x -> x
  | Error ex -> raise ex

let rec await_idle t =
  (* Wait for fibers to finish: *)
  while t.fibers > 0 do
    Trace.try_get t.cancel.id;
    Single_waiter.await_protect t.waiter "Switch.await_idle" t.cancel.id
  done;
  (* Call on_release handlers: *)
  let queue = Lf_stack.close t.on_release in
  List.iter (fun item -> try Cancel.protect (Lf_stack.cancel item) with ex -> fail t ex ) queue;
  if t.fibers > 0 then await_idle t

let maybe_raise_exs t =
  match t.exs with
  | None -> ()
  | Some (ex, bt) -> Printexc.raise_with_backtrace ex bt

let create cancel =
  {
    fibers = 1;         (* The main function counts as a fiber *)
    daemon_fibers = 0;
    exs = None;
    waiter = Single_waiter.create ();
    on_release = Lf_stack.create ();
    cancel;
  }

let run_internal t fn =
  match fn t with
  | v ->
    dec_fibers t;
    await_idle t;
    Trace.get t.cancel.id;
    maybe_raise_exs t;        (* Check for failure while finishing *)
    (* Success. *)
    v
  | exception ex ->
    let bt = Printexc.get_raw_backtrace () in
    (* Main function failed.
       Turn the switch off to cancel any running fibers, if it's not off already. *)
    dec_fibers t;
    fail ~bt t ex;
    await_idle t;
    Trace.get t.cancel.id;
    maybe_raise_exs t;
    assert false

let run ?name fn = Cancel.sub_checked ?name Switch (fun cc -> run_internal (create cc) fn)

let run_protected ?name fn =
  let ctx = Effect.perform Cancel.Get_context in
  Cancel.with_cc ~ctx ~parent:ctx.cancel_context ~protected:true Switch @@ fun cancel ->
  Option.iter (Trace.name cancel.id) name;
  run_internal (create cancel) fn

(* Run [fn ()] in [t]'s cancellation context.
   This prevents [t] from finishing until [fn] is done,
   and means that cancelling [t] will cancel [fn]. *)
let run_in t fn =
  with_op t @@ fun () ->
  let ctx = Effect.perform Cancel.Get_context in
  let old_cc = ctx.cancel_context in
  Cancel.move_fiber_to t.cancel ctx;
  match fn () with
  | ()           -> Cancel.move_fiber_to old_cc ctx;
  | exception ex -> Cancel.move_fiber_to old_cc ctx; raise ex

exception Release_error of string * exn

let () =
  Printexc.register_printer (function
      | Release_error (msg, ex) -> Some (Fmt.str "@[<v2>%s@,while handling %a@]" msg Exn.pp ex)
      | _ -> None
    )

let on_release_cancellable t fn =
  if Domain.self () = t.cancel.domain then (
    match Lf_stack.push t.on_release fn with
    | Some node -> node
    | None ->
      match Cancel.protect fn with
      | () -> invalid_arg "Switch finished!"
      | exception ex ->
        let bt = Printexc.get_raw_backtrace () in
        Printexc.raise_with_backtrace (Release_error ("Switch finished!", ex)) bt
  ) else (
    match Cancel.protect fn with
    | () -> invalid_arg "Switch accessed from wrong domain!"
    | exception ex ->
      let bt = Printexc.get_raw_backtrace () in
      Printexc.raise_with_backtrace (Release_error ("Switch accessed from wrong domain!", ex)) bt
  )

let on_release t fn =
  ignore (on_release_cancellable t fn : Lf_stack.item)
