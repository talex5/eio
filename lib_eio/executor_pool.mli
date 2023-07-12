type t

(** Creates a new Executor_pool with [domain_count].

    [domain_concurrency] is the maximum number of jobs that each domain can run at a time.

    The Executor_pool will not block the [~sw] Switch from completing. *)
val create :
  sw:Switch.t ->
  domain_count:int ->
  domain_concurrency:int ->
  _ Domain_manager.t ->
  t

(** Run a job on this Executor_pool. It is placed at the end of the queue. *)
val submit : t -> (unit -> 'a) -> ('a, exn) result

(** Same as [submit] but raises if the job failed. *)
val submit_exn : t -> (unit -> 'a) -> 'a

(** Same as [submit] but returns immediately, without blocking. *)
val submit_fork : sw:Switch.t -> t -> (unit -> 'a) -> ('a, exn) result Promise.t
