module Cancel = Cancel

module Private = struct
  module Fiber_context = Cancel.Fiber_context

  module Effects = struct
    type 'a enqueue = 'a Suspend.enqueue
    type _ Effect.t += 
      | Suspend = Suspend.Suspend
      | Suspend_fast = Suspend.Suspend_fast
      | Fork = Fiber.Fork
      | Get_context = Cancel.Get_context
      | Trace : (?__POS__:(string * int * int * int) -> ('a, Format.formatter, unit, unit) format4 -> 'a) Effect.t
  end

  module Effect = Effect
  module Ctf = Ctf

  let traceln_mutex = Mutex.create ()

  let default_traceln ?__POS__:pos fmt =
    let k go =
      let b = Buffer.create 512 in
      let f = Format.formatter_of_buffer b in
      go f;
      Option.iter (fun (file, lnum, _, _) -> Format.fprintf f " [%s:%d]" file lnum) pos;
      Format.pp_close_box f ();
      Format.pp_print_flush f ();
      let msg = Buffer.contents b in
      Ctf.label msg;
      let lines = String.split_on_char '\n' msg in
      Mutex.lock traceln_mutex;
      Fun.protect ~finally:(fun () -> Mutex.unlock traceln_mutex) @@ fun () ->
      List.iter (Printf.eprintf "+%s\n") lines;
      flush stderr
    in
    Format.kdprintf k ("@[" ^^ fmt)

end

let traceln ?__POS__ fmt =
  try
    Effect.perform Private.Effects.Trace ?__POS__ fmt
  with Unhandled ->
    Private.default_traceln ?__POS__ fmt

module Promise = Promise
module Fiber = Fiber
module Fibre = Fiber
module Switch = Switch

module Std = struct
  module Promise = Promise
  module Fiber = Fiber
  module Fibre = Fiber
  module Switch = Switch
  let traceln = traceln
end

module Semaphore = Semaphore
module Mutex = Eio_mutex
module Stream = Stream
module Exn = Exn
module Generic = Generic
module Flow = Flow
module Buf_read = Buf_read
module Buf_write = Buf_write
module Net = Net
module Domain_manager = Domain_manager
module Time = Time
module Unix_perm = Dir.Unix_perm
module Dir = Dir

module Stdenv = struct
  type t = <
    stdin  : Flow.source;
    stdout : Flow.sink;
    stderr : Flow.sink;
    net : Net.t;
    domain_mgr : Domain_manager.t;
    clock : Time.clock;
    fs : Dir.t;
    cwd : Dir.t;
    secure_random : Flow.source;
  >

  let stdin  (t : <stdin  : #Flow.source; ..>) = t#stdin
  let stdout (t : <stdout : #Flow.sink;   ..>) = t#stdout
  let stderr (t : <stderr : #Flow.sink;   ..>) = t#stderr
  let net (t : <net : #Net.t; ..>) = t#net
  let domain_mgr (t : <domain_mgr : #Domain_manager.t; ..>) = t#domain_mgr
  let clock (t : <clock : #Time.clock; ..>) = t#clock
  let secure_random (t: <secure_random : #Flow.source; ..>) = t#secure_random
  let fs (t : <fs : #Dir.t; ..>) = t#fs
  let cwd (t : <cwd : #Dir.t; ..>) = t#cwd
end
