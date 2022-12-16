type t

type request

val suspend : t -> (unit -> unit) -> request option

val resume_all : t -> unit

val cancel : request -> bool

val create : unit -> t

val dump : Format.formatter -> t -> unit
