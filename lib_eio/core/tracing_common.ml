(* shared ID management *)

type id = int

let id_chunk_size = 1024

let next_id_chunk = Atomic.make 0

let next_id_key =
  Domain.DLS.new_key (fun () -> Atomic.fetch_and_add next_id_chunk id_chunk_size)

let mint_id () =
  let next_id_local = Domain.DLS.get next_id_key in
  let next_id_local_succ =
    if ((next_id_local + 1) mod id_chunk_size) = 0 then
      (* we're out of local IDs *)
      Atomic.fetch_and_add next_id_chunk id_chunk_size
    else
      next_id_local + 1
  in
  Domain.DLS.set next_id_key next_id_local_succ;
  next_id_local

open Bigarray

module BS = struct
  (* Replacement for endianBigstring that avoids pulling in a Unix dependency *)

  external set_64 : Cstruct.buffer -> int -> int64 -> unit = "%caml_bigstring_set64"
  external swap64 : int64 -> int64 = "%bswap_int64"
  external unsafe_chr : int -> char = "%identity"

  let set_int8 s off v = Array1.set s off (unsafe_chr v)
  [@@ocaml.inline]

  let set_int64_le s off v =
    if Sys.big_endian
    then set_64 s off (swap64 v)
    else set_64 s off v
  [@@ocaml.inline]
end

type log_buffer = (char, int8_unsigned_elt, c_layout) Array1.t

type hiatus_reason = Wait_for_work

type cancellation_context =
  | Choose
  | Pick
  | Join
  | Switch
  | Protect
  | Sub
  | Root

type event =
  | Wait
  | Task
  | Bind
  | Try
  | Map
  | Condition
  | On_success
  | On_failure
  | On_termination
  | On_any
  | Ignore_result
  | Async
  | Promise
  | Semaphore
  | Switch
  | Stream
  | Mutex
  | Cancellation_context of {
    purpose: cancellation_context;
    protected: bool;
  }
  | System_thread

let int_of_thread_type t =
  match t with
  | Wait -> Some 0
  | Task -> Some 1
  | Bind -> Some 2
  | Try -> Some 3
  | Map -> Some 7
  | Condition -> Some 8
  | On_success -> Some 9
  | On_failure -> Some 10
  | On_termination -> Some 11
  | On_any -> Some 12
  | Ignore_result -> Some 13
  | Async -> Some 14
  | Promise -> Some 15
  | Semaphore -> Some 16
  | Switch -> Some 17
  | Stream -> Some 18
  | Mutex -> Some 19
  | System_thread -> None
  | Cancellation_context _ -> None
