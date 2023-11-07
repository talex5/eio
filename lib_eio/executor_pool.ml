type job = Pack : (unit -> 'a) * ('a, exn) Result.t Promise.u -> job

type t = {
  queue : job Stream.t;
  cancelled : bool Atomic.t;
}

(* This function is the core of executor_pool.ml.
   Each worker runs in its own domain,
   taking jobs from [queue] whenever it has spare capacity. *)
let run_worker ~limit queue =
  Switch.run @@ fun sw ->
  let capacity = Semaphore.make limit in
  (* The main worker loop. *)
  let rec loop () =
    Semaphore.acquire capacity;
    let Pack (fn, w) = Stream.take queue in
    Fiber.fork ~sw (fun () ->
        Promise.resolve w (try Ok (fn ()) with ex -> Error ex);
        Semaphore.release capacity
      );
    (* Give a chance to other domains to start waiting on [queue]
       before the current thread blocks on [Stream.take] again. *)
    Fiber.yield ();
    (loop [@tailcall]) ()
  in
  loop ()

let rec reject_all queue =
  match Stream.take_nonblocking queue with
  | None -> ()
  | Some (Pack (_fn, w)) ->
    Promise.resolve_error w (Failure "Executor_pool: Switch finished");
    reject_all queue

let create ~sw ~domain_count ~domain_concurrency domain_mgr =
  let queue = Stream.create 0 in
  let t = { queue; cancelled = Atomic.make false } in
  Switch.on_release sw (fun () -> Atomic.set t.cancelled true; reject_all queue);
  for _ = 1 to domain_count do
    (* Workers run as daemons to not hold the user's switch from completing.
       It's up to the user to hold the switch open (and thus, the executor pool)
       by blocking on the jobs issued to the pool. *)
    Fiber.fork_daemon ~sw (fun () ->
        Domain_manager.run domain_mgr (fun () ->
            run_worker ~limit:domain_concurrency queue
          )
      )
  done;
  t

let enqueue { queue; cancelled } f =
  let p, w = Promise.create () in
  Fiber.both
    (fun () -> Stream.add queue (Pack (f, w)))
    (fun () ->
       (* By this point, the previous fiber suspended,
          so our job was either accepted or is waiting in the queue. *)
       if Atomic.get cancelled then (
         (* Possibly all the workers have already finished by now.
            To avoid our job stitting in the queue forever, reject any remaining
            jobs (including our own if noone else gets there first). *)
         reject_all queue;
       )
    );
  p

let submit t f =
  enqueue t f |> Promise.await

let submit_exn t f =
  enqueue t f |> Promise.await_exn

let submit_fork ~sw t f =
  (* [enqueue] blocks until the job is accepted, so we have to fork here. *)
  Fiber.fork_promise ~sw (fun () -> submit_exn t f)
