type id = int

type ty =
  | Promise
  | Semaphore
  | Switch
  | Stream
  | Mutex

let ty_to_uint8 = function
  | Promise -> 15
  | Semaphore -> 16
  | Switch -> 17
  | Stream -> 18
  | Mutex -> 19

let ty_of_uint8 = function
  | 15 -> Promise
  | 16 -> Semaphore
  | 17 -> Switch
  | 18 -> Stream
  | 19 -> Mutex
  | _ -> assert false

let ty_to_string (t : ty) =
  match t with
  | Promise -> "promise"
  | Semaphore -> "semaphore"
  | Switch -> "switch"
  | Stream -> "stream"
  | Mutex -> "mutex"

type cc_ty =
  | Choose
  | Pick
  | Join
  | Switch
  | Protect
  | Sub
  | Root
  | Any

let cc_ty_to_uint8 = function
  | Choose -> 0
  | Pick -> 1
  | Join -> 2
  | Switch -> 3
  | Protect -> 4
  | Sub -> 5
  | Root -> 6
  | Any -> 7

let cc_ty_of_uint8 = function
  | 0 -> Choose
  | 1 -> Pick
  | 2 -> Join
  | 3 -> Switch
  | 4 -> Protect
  | 5 -> Sub
  | 6 -> Root
  | 7 -> Any
  | _ -> assert false

let cc_ty_to_string = function
  | Choose -> "choose"
  | Pick -> "pick"
  | Join -> "join"
  | Switch -> "switch"
  | Protect -> "protect"
  | Sub -> "sub"
  | Root -> "root"
  | Any -> "any"

let string =
  let encode buf s =
    let len = min (Bytes.length buf) (String.length s) in
    Bytes.blit_string s 0 buf 0 len;
    len
  in
  let decode buf len = Bytes.sub_string buf 0 len in
  Runtime_events.Type.register ~encode ~decode

let id_ty_type =
  let encode buf (id, ty) =
    Bytes.set_int64_le buf 0 (Int64.of_int id);
    Bytes.set_int8 buf 8 (ty_to_uint8 ty);
    9
  in
  let decode buf _size =
    let id = Bytes.get_int64_le buf 0 |> Int64.to_int in
    let ty = ty_of_uint8 (Bytes.get_int8 buf 8) in
    (id, ty)
  in
  Runtime_events.Type.register ~encode ~decode

let id_id_type =
  let encode buf (id1, id2) =
    Bytes.set_int64_le buf 0 (Int64.of_int id1);
    Bytes.set_int64_le buf 8 (Int64.of_int id2);
    16
  in
  let decode buf _size =
    let id1 = Bytes.get_int64_le buf 0 |> Int64.to_int in
    let id2 = Bytes.get_int64_le buf 8 |> Int64.to_int in
    (id1, id2)
  in
  Runtime_events.Type.register ~encode ~decode

let id_cc_type =
  let encode buf (id, ty) =
    Bytes.set_int64_le buf 0 (Int64.of_int id);
    Bytes.set_int8 buf 8 (cc_ty_to_uint8 ty);
    9
  in
  let decode buf _size =
    let id = Bytes.get_int64_le buf 0 |> Int64.to_int in
    let ty = cc_ty_of_uint8 (Bytes.get_int8 buf 8) in
    (id, ty)
  in
  Runtime_events.Type.register ~encode ~decode

let id_string_type =
  let encode buf (id, msg) =
    (* Check size of buf and use smallest size which means we may
       have to truncate the label. *)
    let available_buf_len = Bytes.length buf - 8 in
    let msg_len = String.length msg in
    let data_len = min available_buf_len msg_len in
    Bytes.set_int64_le buf 0 (Int64.of_int id);
    Bytes.blit_string msg 0 buf 8 data_len;
    data_len + 8
  in
  let decode buf size =
    let id = Bytes.get_int64_le buf 0 |> Int64.to_int in
    (id, Bytes.sub_string buf 8 (size - 8))
  in
  Runtime_events.Type.register ~encode ~decode

let exn_type =
  let encode buf (id, exn) =
    (* Check size of buf and use smallest size which means we may
       have to truncate the label. *)
    let available_buf_len = Bytes.length buf - 8 in
    let msg = Printexc.to_string exn in
    let msg_len = String.length msg in
    let data_len = min available_buf_len msg_len in
    Bytes.set_int64_le buf 0 (Int64.of_int id);
    Bytes.blit_string msg 0 buf 8 data_len;
    data_len + 8
  in
  let decode buf size =
    let id = Bytes.get_int64_le buf 0 |> Int64.to_int in
    (id, Failure (Bytes.sub_string buf 8 (size - 8)))
  in
  Runtime_events.Type.register ~encode ~decode

(* Runtime events registration *)

type Runtime_events.User.tag += Create
let create = Runtime_events.User.register "eio.create" Create id_ty_type

type Runtime_events.User.tag += Create_cc
let create_cc = Runtime_events.User.register "eio.cc.create" Create_cc id_cc_type

type Runtime_events.User.tag += Fiber_create
let fiber_create = Runtime_events.User.register "eio.fiber.create" Fiber_create id_id_type

type Runtime_events.User.tag += Get
let get = Runtime_events.User.register "eio.get" Get Runtime_events.Type.int

type Runtime_events.User.tag += Try_get
let try_get = Runtime_events.User.register "eio.try_get" Try_get Runtime_events.Type.int

type Runtime_events.User.tag += Put | Error | Exit_cc | Exit_fiber
let put = Runtime_events.User.register "eio.put" Put Runtime_events.Type.int
let exit_cc = Runtime_events.User.register "eio.exit_cc" Exit_cc Runtime_events.Type.unit
let exit_fiber = Runtime_events.User.register "eio.exit_fiber" Exit_fiber Runtime_events.Type.int
let error = Runtime_events.User.register "eio.error" Error exn_type

type Runtime_events.User.tag += Name
let name = Runtime_events.User.register "eio.name" Name id_string_type

type Runtime_events.User.tag += Log
let log = Runtime_events.User.register "eio.log" Log string

type Runtime_events.User.tag += Fiber
let fiber = Runtime_events.User.register "eio.fiber" Fiber Runtime_events.Type.int

type Runtime_events.User.tag += Suspend
let suspend = Runtime_events.User.register "eio.suspend" Suspend Runtime_events.Type.span

type event = [
  | `Create of id * [
      | `Fiber_in of id
      | `Cc of cc_ty
      | `Sync of ty
    ]
  | `Fiber of id
  | `Name of id * string
  | `Log of string
  | `Get of id
  | `Try_get of id
  | `Put of id
  | `Error of (id * string)
  | `Exit_cc
  | `Exit_fiber of id
  | `Suspend of Runtime_events.Type.span
]

let pf = Format.fprintf

let pp_event f (e : event) =
  match e with
  | `Create (id, `Fiber_in cc) -> pf f "create fiber %d in CC %d" id cc
  | `Create (id, `Cc ty) -> pf f "create %s CC %d" (cc_ty_to_string ty) id
  | `Create (id, `Sync ty) -> pf f "create %s %d" (ty_to_string ty) id
  | `Fiber id -> pf f "fiber %d is now running" id
  | `Name (id, name) -> pf f "%d is named %S" id name
  | `Log msg -> pf f "log: %S" msg
  | `Get id -> pf f "get from %d" id
  | `Try_get id -> pf f "waiting to get from %d" id
  | `Put id -> pf f "put %d" id
  | `Error (id, msg) -> pf f "%d fails: %S" id msg
  | `Exit_cc -> pf f "CC finishes"
  | `Exit_fiber id -> pf f "fiber %d finishes" id
  | `Suspend Begin -> pf f "domain suspend"
  | `Suspend End -> pf f "domain resume"

type 'a handler = int -> Runtime_events.Timestamp.t -> 'a -> unit

let add_callbacks (fn : event handler) x =
  let create_event ring_id ts ev (id, ty) =
    match Runtime_events.User.tag ev with
    | Create -> fn ring_id ts (`Create (id, `Sync ty))
    | _ -> ()
  in
  let create_cc_event ring_id ts ev (id, ty) =
    match Runtime_events.User.tag ev with
    | Create_cc -> fn ring_id ts (`Create (id, `Cc ty))
    | _ -> ()
  in
  let int_event ring_id ts ev v =
    match Runtime_events.User.tag ev with
    | Get -> fn ring_id ts (`Get v)
    | Try_get -> fn ring_id ts (`Try_get v)
    | Put -> fn ring_id ts (`Put v)
    | Fiber -> fn ring_id ts (`Fiber v)
    | Exit_fiber -> fn ring_id ts (`Exit_fiber v)
    | _ -> ()
  in
  let span_event ring_id ts ev v =
    match Runtime_events.User.tag ev with
    | Suspend -> fn ring_id ts (`Suspend v)
    | _ -> ()
  in
  let id_id_event ring_id ts ev (id1, id2) =
    match Runtime_events.User.tag ev with
    | Fiber_create -> fn ring_id ts (`Create (id1, `Fiber_in id2))
    | _ -> ()
  in
  let int_exn_event ring_id ts ev (id, ex) =
    match Runtime_events.User.tag ev, ex with
    | Error, Failure msg -> fn ring_id ts (`Error (id, msg))
    | _ -> ()
  in
  let id_string_event ring_id ts ev v =
    match Runtime_events.User.tag ev with
    | Name -> fn ring_id ts (`Name v)
    | _ -> ()
  in
  let string_event ring_id ts ev v =
    match Runtime_events.User.tag ev with
    | Log -> fn ring_id ts (`Log v)
    | _ -> ()
  in
  let unit_event ring_id ts ev () =
    match Runtime_events.User.tag ev with
    | Exit_cc -> fn ring_id ts `Exit_cc
    | _ -> ()
  in
  x
  |> Runtime_events.Callbacks.add_user_event id_ty_type create_event
  |> Runtime_events.Callbacks.add_user_event id_id_type id_id_event
  |> Runtime_events.Callbacks.add_user_event id_cc_type create_cc_event
  |> Runtime_events.Callbacks.add_user_event Runtime_events.Type.int int_event
  |> Runtime_events.Callbacks.add_user_event exn_type int_exn_event
  |> Runtime_events.Callbacks.add_user_event string string_event
  |> Runtime_events.Callbacks.add_user_event id_string_type id_string_event
  |> Runtime_events.Callbacks.add_user_event Runtime_events.Type.span span_event
  |> Runtime_events.Callbacks.add_user_event Runtime_events.Type.unit unit_event
