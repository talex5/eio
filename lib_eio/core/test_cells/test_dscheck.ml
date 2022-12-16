module Cqs1 = Simple_cqs.Make(struct let segment_order = 0 end)  (* 1 cell per segment *)
module Cqs2 = Simple_cqs.Make(struct let segment_order = 1 end)  (* 2 cells per segment *)

module type S = (module type of Cqs1)

let debug = false

let test_cells (module Cqs : S) n_values () =
  let expected_total = n_values in
  let t = Cqs.make () in
  let total = ref 0 in
  for i = 1 to n_values do
    Atomic.spawn (fun () ->
        if debug then Fmt.epr "%d: wrote value@." i;
        Cqs.resume t 1;
      );
    Atomic.spawn (fun () ->
        match
          Cqs.suspend t (fun v ->
              if debug then Fmt.epr "%d: resumed@." i;
              total := !total + v
            )
        with
        | None -> () (* Already resumed *)
        | Some request ->
          if Cqs.cancel request then (
            if debug then Fmt.epr "%d: cancelled@." i;
            total := !total + 1
          )
      );
  done;
  Atomic.final
    (fun () ->
       if debug then (
         Format.eprintf "%a@." Cqs.dump t;
         Format.eprintf "total=%d, expected_total=%d\n%!" !total expected_total;
       );
       Cqs.Cells.validate t;
       assert (!total = expected_total);
       (*        Printf.printf "total = %d\n%!" !total *)
    )

(* A list of unit-cells. Just for testing removing whole cancelled segments. *)
module Unit_cells(Config : sig val segment_order : int end) = struct
  module Cell = struct
    type _ t = Empty | Value | Cancelled
    let init = Empty
    let segment_order = Config.segment_order
    let dump f _ = Fmt.string f "()"
  end
  module Cells = Cells.Make(Cell)

  let cancel (segment, offset) =
    let cell = Cells.Segment.get segment offset in
    if Atomic.compare_and_set cell Empty Cancelled then (
      Cells.Segment.on_cancelled_cell segment;
      true
    ) else false

  let resume t =
    let segment, offset =  Cells.next_resume t in
    let cell = Cells.Segment.get segment offset in
    Atomic.compare_and_set cell Empty Value

  let suspend t = Cells.next_suspend t
  let make = Cells.make
  let dump = Cells.dump
end

(* A producer writes [n_items] to the queue (retrying if the cell gets cancelled first).
   A consumer reads [n_items] from the queue (cancelling if it can).
   At the end, the consumer and resumer are at the same position.
   This tests what happens if a whole segment gets cancelled and the producer therefore skips it. *)
let n_items = 3

let test_skip_segments ~segment_order () =
  let module Cells = Unit_cells(struct let segment_order = segment_order end) in
  if debug then print_endline "== start ==";
  let t = Cells.make () in
  Atomic.spawn (fun () ->
      for _ = 1 to n_items do
        let rec loop ~may_cancel =
          if debug then print_endline "suspend";
          let request = Cells.suspend t in
          if may_cancel && Cells.cancel request then (
            if debug then print_endline "cancelled";
            loop ~may_cancel:false
          )
        in
        loop ~may_cancel:true
      done
    );
  Atomic.spawn (fun () ->
      for _ = 1 to n_items do
        if debug then print_endline "resume";
        while not (Cells.resume t) do () done
      done
    );
  Atomic.final
    (fun () ->
       if debug then Fmt.pr "%a@." Cells.dump t;
       Cells.Cells.validate t;
       assert (Cells.Cells.Position.index t.suspend =
               Cells.Cells.Position.index t.resume);
    )

(* These tests take about 7s on my machine, with https://github.com/ocaml-multicore/dscheck/pull/3 *)
let () =
  print_endline "Test cancelling segments:";
  Atomic.trace (test_skip_segments ~segment_order:1);
  print_endline "Test with 1 cell per segment:";
  Atomic.trace (test_cells (module Cqs1) 2);
  print_endline "Test with 2 cells per segment:";
  Atomic.trace (test_cells (module Cqs2) 2)
