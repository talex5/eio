# Setting up the environment

```ocaml
# #require "eio_main";;
# #require "eio.mock";;
```

Creating some useful helper functions

```ocaml
open Eio.Std

module Executor_pool = Eio.Executor_pool

let () = Eio.Exn.Backend.show := false

let run fn =
  Eio_mock.Backend.run @@ fun () ->
  Eio_mock.Domain_manager.run @@ fun mgr ->
  let clock = Eio_mock.Clock.make () in
  let sleep ms =
    let t0 = Eio.Time.now clock in
    let t1 = t0 +. ms in
    traceln "Sleeping %.0f: %.0f -> %.0f" ms t0 t1;
    Fiber.both
      (fun () -> Eio.Time.sleep_until clock t1)
      (fun () ->
        Fiber.yield ();
        Fiber.yield ();
        Fiber.yield ();
        Fiber.yield ();
        Fiber.yield ();
        Fiber.yield ();
        Fiber.yield ();
        if Float.(Eio.Time.now clock <> t1) then
        Eio_mock.Clock.advance clock)
  in
  let duration expected f =
    let t0 = Eio.Time.now clock in
    let res = f () in
    let t1 = Eio.Time.now clock in
    let actual = t1 -. t0 in
    if Float.(actual = expected)
    then (traceln "Duration (valid): %.0f" expected; res)
    else failwith (Format.sprintf "Duration was not %.0f: %.0f" expected actual)
  in
  fn mgr sleep duration
```

# Concurrency

Runs jobs in parallel as much as possible (domains):

```ocaml
# run @@ fun mgr sleep duration ->
  Switch.run @@ fun sw ->
  let total = ref 0 in
  let ep = Executor_pool.create ~sw ~domain_count:2 ~domain_concurrency:1 mgr in
  duration 150. (fun () ->
    List.init 5 (fun i -> i + 1)
    |> Fiber.List.iter (fun i -> Executor_pool.submit_exn ep (fun () ->
      sleep 50.;
      total := !total + i
    ));
    !total
  );;
+[1] Sleeping 50: 0 -> 50
+[2] Sleeping 50: 0 -> 50
+[1] mock time is now 50
+[1] Sleeping 50: 50 -> 100
+[2] Sleeping 50: 50 -> 100
+[1] mock time is now 100
+[1] Sleeping 50: 100 -> 150
+[1] mock time is now 150
+[0] Duration (valid): 150
- : int = 15
```

Runs jobs in parallel as much as possible (workers):

```ocaml
# run @@ fun mgr sleep duration ->
  Switch.run @@ fun sw ->
  let total = ref 0 in
  let ep = Executor_pool.create ~sw ~domain_count:1 ~domain_concurrency:2 mgr in
  duration 150. (fun () ->
    List.init 5 (fun i -> i + 1)
    |> Fiber.List.iter (fun i -> Executor_pool.submit_exn ep (fun () ->
      sleep 50.;
      total := !total + i
    ));
    !total
  );;
+[1] Sleeping 50: 0 -> 50
+[1] Sleeping 50: 0 -> 50
+[1] mock time is now 50
+[1] Sleeping 50: 50 -> 100
+[1] Sleeping 50: 50 -> 100
+[1] mock time is now 100
+[1] Sleeping 50: 100 -> 150
+[1] mock time is now 150
+[0] Duration (valid): 150
- : int = 15
```

Runs jobs in parallel as much as possible (both):

```ocaml
# run @@ fun mgr sleep duration ->
  Switch.run @@ fun sw ->
  let total = ref 0 in
  let ep = Executor_pool.create ~sw ~domain_count:2 ~domain_concurrency:2 mgr in
  duration 100. (fun () ->
    List.init 5 (fun i -> i + 1)
    |> Fiber.List.iter (fun i -> Executor_pool.submit_exn ep (fun () ->
      sleep 50.;
      total := !total + i
    ));
    !total
  );;
+[1] Sleeping 50: 0 -> 50
+[2] Sleeping 50: 0 -> 50
+[1] Sleeping 50: 0 -> 50
+[2] Sleeping 50: 0 -> 50
+[1] mock time is now 50
+[1] Sleeping 50: 50 -> 100
+[1] mock time is now 100
+[0] Duration (valid): 100
- : int = 15
```

# Job error handling

`Executor_pool.submit` returns a Result:

```ocaml
# run @@ fun mgr sleep duration ->
  Switch.run @@ fun sw ->
  let total = ref 0 in
  let ep = Executor_pool.create ~sw ~domain_count:1 ~domain_concurrency:4 mgr in
  duration 100. (fun () ->
    let results =
      List.init 5 (fun i -> i + 1)
      |> Fiber.List.map (fun i -> Executor_pool.submit ep (fun () ->
        sleep 50.;
        if i mod 2 = 0
        then failwith (Int.to_string i)
        else (let x = !total in total := !total + i; x)
    ))
    in
    results, !total
  );;
+[1] Sleeping 50: 0 -> 50
+[1] Sleeping 50: 0 -> 50
+[1] Sleeping 50: 0 -> 50
+[1] Sleeping 50: 0 -> 50
+[1] mock time is now 50
+[1] Sleeping 50: 50 -> 100
+[1] mock time is now 100
+[0] Duration (valid): 100
- : (int, exn) result list * int =
([Ok 0; Error (Failure "2"); Ok 1; Error (Failure "4"); Ok 4], 9)
```

`Executor_pool.submit_exn` raises:

```ocaml
# run @@ fun mgr sleep duration ->
  Switch.run @@ fun sw ->
  let total = ref 0 in
  let ep = Executor_pool.create ~sw ~domain_count:1 ~domain_concurrency:2 mgr in
  List.init 5 (fun i -> i + 1)
  |> Fiber.List.map (fun i -> Executor_pool.submit_exn ep (fun () ->
    traceln "Started %d" i;
    let x = !total in
    total := !total + i;
    if x = 3
    then failwith (Int.to_string i)
    else x
  ));;
+[1] Started 1
+[1] Started 2
+[1] Started 3
+[1] Started 4
Exception: Failure "3".
```

# Blocking for capacity

`Executor_pool.submit` will block waiting for room in the queue:

```ocaml
# run @@ fun mgr sleep duration ->
  Switch.run @@ fun sw ->
  let ep = Executor_pool.create ~sw ~domain_count:1 ~domain_concurrency:1 mgr in

  let p1 = Fiber.fork_promise ~sw (fun () -> Executor_pool.submit_exn ep (fun () -> sleep 50.)) in

  duration 50. (fun () -> Executor_pool.submit_exn ep @@ fun () -> ());

  duration 0. (fun () -> Promise.await_exn p1)
  ;;
+[1] Sleeping 50: 0 -> 50
+[1] mock time is now 50
+[0] Duration (valid): 50
+[0] Duration (valid): 0
- : unit = ()
```

`Executor_pool.submit_fork` will not block if there's not enough room in the queue:

```ocaml
# run @@ fun mgr sleep duration ->
  Switch.run @@ fun sw ->
  let ep = Executor_pool.create ~sw ~domain_count:1 ~domain_concurrency:1 mgr in

  let p1 = duration 0. (fun () ->
    Fiber.fork_promise ~sw (fun () -> Executor_pool.submit_exn ep (fun () -> sleep 50.))
  )
  in
  let p2 = duration 0. (fun () ->
    Fiber.fork_promise ~sw (fun () -> Executor_pool.submit_exn ep (fun () -> sleep 50.))
  )
  in
  let p3 = duration 0. (fun () ->
    Executor_pool.submit_fork ~sw ep (fun () -> ())
  )
  in

  duration 100. (fun () ->
    Promise.await_exn p1;
    Promise.await_exn p2;
    Promise.await_exn p3;
    (* Value restriction :( *)
    Promise.create_resolved (Ok ())
  )
  |> Promise.await_exn
  ;;
+[0] Duration (valid): 0
+[0] Duration (valid): 0
+[0] Duration (valid): 0
+[1] Sleeping 50: 0 -> 50
+[1] mock time is now 50
+[1] Sleeping 50: 50 -> 100
+[1] mock time is now 100
+[0] Duration (valid): 100
- : unit = ()
```

# Checks switch status

```ocaml
# run @@ fun mgr sleep duration ->
  let leak = ref None in
  let count = ref 0 in

  let () =
    try (
      Switch.run @@ fun sw ->

      let ep = Executor_pool.create ~sw ~domain_count:1 ~domain_concurrency:1 mgr in
      leak := Some ep;

      let p1 = duration 0. (fun () ->
        Fiber.fork_promise ~sw (fun () -> Executor_pool.submit_exn ep (fun () -> sleep 50.; incr count))
      )
      in
      Switch.fail sw (Failure "Abort mission!");
      Promise.await_exn p1;
      Executor_pool.submit_exn ep (fun () -> sleep 50.; incr count) )
    with _ -> ()
  in
  match !leak with
  | None -> assert false
  | Some ep ->
    Executor_pool.submit_exn ep (fun () -> sleep 50.; incr count);
    traceln "Count: %d" !count
+[0] Duration (valid): 0
Exception: Failure "Executor_pool: Switch cancelled".
```
