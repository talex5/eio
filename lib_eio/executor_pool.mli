(** An executor pool distributes jobs among a pool of domains.

    Usually you will only want one pool for an entire application,
    so the pool is typically created when the application starts:

    {[
      let () =
        Eio_main.run @@ fun env ->
        Switch.run @@ fun sw ->
        let pool =
          Eio.Executor_pool.create ~sw env#domain_mgr
            ~domain_count:2
            ~domain_concurrency:1
        in
        main ~pool
    ]}
*)

type t
(** An executor pool. *)

val create :
  sw:Switch.t ->
  domain_count:int ->
  domain_concurrency:int ->
  _ Domain_manager.t ->
  t
(** [create ~sw ~domain_count ~domain_concurrency dm] creates a new executor pool.

    The executor pool will not block switch [sw] from completing;
    when the switch finishes, all worker domains and running jobs are cancelled.

    @param domain_count The number of worker domains to create.
    @param domain_concurrency The maximum number of jobs that each domain can run at a time. *)

val submit : t -> (unit -> 'a) -> ('a, exn) result
(** [submit t fn] runs [fn ()] using this executor pool.

    The job is added to the back of the queue. *)

val submit_exn : t -> (unit -> 'a) -> 'a
(** Same as {!submit} but raises if the job fails. *)

val submit_fork : sw:Switch.t -> t -> (unit -> 'a) -> 'a Promise.or_exn
(** Same as {!submit} but returns immediately, without blocking. *)
