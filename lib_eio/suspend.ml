type 'a enqueue = ('a, exn) result -> unit
type _ Effect.t += Suspend : (Cancel.fiber_context -> 'a enqueue -> unit) -> 'a Effect.t
type _ Effect.t += Suspend_fast : ((unit -> unit) -> unit) -> unit Effect.t

let enter_unchecked fn = Effect.perform (Suspend fn)

let enter fn =
  enter_unchecked @@ fun fiber enqueue ->
  match Cancel.Fiber_context.get_error fiber with
  | None -> fn fiber enqueue
  | Some ex -> enqueue (Error ex)

let fast fn =
  Effect.perform (Suspend_fast fn)
