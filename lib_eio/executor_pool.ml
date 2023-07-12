type job = Pack : (unit -> 'a) * ('a, exn) Result.t Promise.u -> job

(* Worker: 1 domain/thread
   m jobs per worker, n domains per executor_pool *)

type t = {
  stream: job Stream.t;
  cancelled: bool Atomic.t;
}

(* This function is the core of executor_pool.ml.
   Each worker recursively calls [loop ()] until the [terminating]
   promise is resolved. Workers pull one job at a time from the Stream. *)
let start_worker ~limit stream =
  Switch.run @@ fun sw ->
  let capacity = Semaphore.make limit in
  let run_job job w =
    Fiber.fork ~sw (fun () ->
        Promise.resolve w
          (try Ok (job ()) with
           | exn -> Error exn);
        Semaphore.release capacity )
  in
  (* The main worker loop. *)
  let rec loop () =
    Semaphore.acquire capacity;
    let actions = Stream.take stream in
    match actions with
    | Pack (job, w) ->
      (* Give a chance to other domains to start waiting on the Stream before the current thread blocks on [Stream.take] again. *)
      Fiber.yield ();
      run_job job w;
      (loop [@tailcall]) ()
  in
  loop ()

(* Start a new domain. The worker will need a switch, then we start the worker. *)
let start_domain ~sw ~domain_mgr ~limit stream =
  let go () =
    Domain_manager.run domain_mgr (fun () -> start_worker ~limit stream )
  in
  (* Executor_pools run as daemons to not hold the user's switch from completing.
     It's up to the user to hold the switch open (and thus, the executor_pool)
     by blocking on the jobs issued to the executor_pool. *)
  Fiber.fork_daemon ~sw (fun () ->
      let _ = go () in
      `Stop_daemon )

let create ~sw ~domain_count ~domain_concurrency domain_mgr =
  let stream = Stream.create 0 in
  let instance = { stream; cancelled = Atomic.make false } in
  Switch.on_release sw (fun () -> Atomic.set instance.cancelled true);
  for _ = 1 to domain_count do
    start_domain ~sw ~domain_mgr ~limit:domain_concurrency stream
  done;
  instance

let enqueue { stream; cancelled } f =
  if Atomic.get cancelled then raise (Failure "Executor_pool: Switch cancelled");
  let p, w = Promise.create () in
  Stream.add stream (Pack (f, w));
  p

let submit_fork ~sw instance f =
  Fiber.fork_promise ~sw (fun () ->
    enqueue instance f
    |> Promise.await_exn )

let submit instance f =
  enqueue instance f
  |> Promise.await

let submit_exn instance f =
  match submit instance f with
  | Ok x -> x
  | Error exn -> raise exn
