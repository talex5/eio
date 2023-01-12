let debug = false

module T = Stream_state

let test ~capacity ~prod ~cons () =
  let expected_total = (prod * (1 + prod) / 2) in
  let messages = ref [] in
  let log fmt = (fmt ^^ "@.") |> Format.kasprintf @@ fun msg -> print_endline msg; messages := msg :: !messages in
  if debug then log "== start ==";
  let t = T.create capacity in
  let total = ref 0 in
  let add v fn =
    match T.add t v with
    | None -> fn (); None
    | Some request -> T.add2 request fn; Some request
  in
  let take fn =
    match T.take t with
    | Ok v -> fn v; None
    | Error request -> T.take2 request fn; Some request
  in
  for i = 1 to prod do
    Atomic.spawn (fun () ->
        match
          add i (fun () -> if debug then log "p%d: added %d" i i)
        with
        | None -> ()
        | Some request ->
          if T.cancel_add request then (
            if debug then log "p%d: cancelled add" i;
            ignore (add i (fun () -> if debug then log "p%d: added(2) %d" i i))
          )
      )
  done;
  for i = 1 to cons do
    Atomic.spawn (fun () ->
        match
          take (fun v -> total := !total + v; if debug then log "c%d: took %d" i v)
        with
        | None -> ()
        | Some request ->
          if T.cancel_take request then (
            if debug then log "c%d: cancelled take" i;
            ignore (take (fun v -> total := !total + v; if debug then log "c%d: took(2) %d" i v))
          )
      )
  done;
  (*   Atomic.every (fun () -> assert (Atomic.get running <= capacity)); *)
  Atomic.final (fun () ->
      if debug then (
        List.iter print_string (List.rev !messages);
        Fmt.pr "%a@." T.dump t;
        Fmt.pr "total = %d@." !total;
      );
      assert (T.items t = prod - cons);
      for i = prod + 1 to cons do
        let request = add 0 (fun () -> if debug then log "f%d: added 0" i) in
        assert (request = None);
      done;
      for i = cons + 1 to prod do
        let request = take (fun v -> total := !total + v; if debug then log "f%d: took %d" i v) in
        assert (request = None);
      done;
      assert (!total = expected_total);
      assert (T.items t = 0)
    )

let () =
  for capacity = 0 to 2 do
    Atomic.trace (test ~capacity ~prod:1 ~cons:2);
    Atomic.trace (test ~capacity ~prod:2 ~cons:1);
  done
