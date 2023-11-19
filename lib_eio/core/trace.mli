(** Trace Eio events using OCaml's runtime events system. *)

type id = private int
(** Each thread/fiber/promise is identified by a unique ID. *)

val mint_id : unit -> id
(** [mint_id ()] is a fresh unique [id]. *)

(** {2 Recording events}
    Libraries and applications can use these functions to make the traces more useful. *)

val log : string -> unit
(** [log msg] attaches text [msg] to the current fiber. *)

(** {2 Recording system events}
    These are normally only called by the scheduler. *)

val create : ?label:string -> id -> Eio_runtime_events.ty -> unit
(** [create id ty] records the creation of [id]. *)

val fiber_create : cc:id -> id -> unit
(** [fiber_create ~cc id] records the creation of fiber [id] in context [cc]. *)

val create_cc : id -> Eio_runtime_events.cc_ty -> unit
(** [create_cc id ty] records the creation of cancellation context [id]. *)

val get : id -> unit
(** [get src] records reading a promise, taking from a stream, taking a lock, etc. *)

val try_get : id -> unit
(** [try_get src] records that the current fiber wants to get from [src] (which is not currently ready). *)

val put : id -> unit
(** [put dst] records resolving a promise, adding to a stream, releasing a lock, etc. *)

val fiber : id -> unit
(** [fiber id] records that [id] is now the current fiber for this domain. *)

val suspend : Runtime_events.Type.span -> unit
(** [suspend] records when the event loop is stopped waiting for events from the OS. *)

val exit_cc : unit -> unit
(** [exit_cc ()] records that the current CC has finished. *)

val exit_fiber : id -> unit
(** [exit_fiber id] records that fiber [id] has finished. *)

val error : id -> exn -> unit
(** [error id exn] records that [id] received an error. *)
