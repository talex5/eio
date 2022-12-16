type t = Broadcast.t

let create () = Broadcast.create ()

let await_generic ?mutex t =
  let need_lock = ref false in
  match
    Suspend.enter_unchecked (fun ctx enqueue ->
        match Fiber_context.get_error ctx with
        | Some ex ->
          enqueue (Error ex)
        | None ->
          match Broadcast.suspend t (fun () -> enqueue (Ok ())) with
          | None -> ()
          | Some request ->
            Option.iter (fun mutex -> Eio_mutex.unlock mutex; need_lock := true) mutex;
            Fiber_context.set_cancel_fn ctx (fun ex ->
                if Broadcast.cancel request then enqueue (Error ex)
                (* else already succeeded *)
              )
      )
  with
  | () -> if !need_lock then Eio_mutex.lock (Option.get mutex)
  | exception ex ->
    let bt = Printexc.get_raw_backtrace () in
    if !need_lock then Eio_mutex.lock (Option.get mutex);
    Printexc.raise_with_backtrace ex bt

let await t mutex = await_generic ~mutex t
let await_no_mutex t = await_generic t

let broadcast t =
  Broadcast.resume_all t
