(* This module also checks that Eio doesn't pull in a dependency on Unix.
   See the [dune] file. *)

module Tracing = Eio.Private.Tracing

let () =
  let bs = Cstruct.create 8 in
  Cstruct.LE.set_uint64 bs 0 1234L;
  assert (Cstruct.LE.get_uint64 bs 0 = 1234L)
