open Eio.Std

module Sockopt = Eio.Net.Sockopt

let try_setsockopt sock opt v : unit =
  match Eio.Net.getsockopt sock opt with
  | exception ex -> traceln "%a -> %a" Eio.Net.Sockopt.pp opt Fmt.exn ex
  | old ->
    Eio.Net.setsockopt sock opt v;
    let v = Eio.Net.getsockopt sock opt in
    traceln "%a -> %a"
      Eio.Net.Sockopt.pp_binding (opt, old)
      Eio.Net.Sockopt.pp_binding (opt, v)

let addr = `Tcp (Eio.Net.Ipaddr.V4.loopback, 0)

let () =
  Eio_main.run @@ fun env ->
  Switch.run @@ fun sw ->
  let net = env#net in
  let listen_sock = Eio.Net.listen net ~sw ~reuse_addr:true ~backlog:5 addr in
  let listening_addr = Eio.Net.listening_addr listen_sock in
  (* Test TCP_DEFER_ACCEPT on listening socket *)
  Eio.Net.setsockopt listen_sock Sockopt.TCP_DEFER_ACCEPT 5;
  let defer = Eio.Net.getsockopt listen_sock Sockopt.TCP_DEFER_ACCEPT in
  traceln "TCP_DEFER_ACCEPT on listening socket: %s" (if defer > 0 then "enabled" else "disabled");

  (* Create a TCP connection for other tests *)
  let client = Eio.Net.connect ~sw net listening_addr in

  try_setsockopt client Sockopt.TCP_CORK true;
  try_setsockopt client Sockopt.TCP_CORK false;
  try_setsockopt client Sockopt.TCP_KEEPIDLE 60;
  try_setsockopt client Sockopt.TCP_KEEPINTVL 10;
  try_setsockopt client Sockopt.TCP_KEEPCNT 5;
  try_setsockopt client Sockopt.TCP_LINGER2 (Some 110);

  try_setsockopt client Sockopt.TCP_CONGESTION "reno";

  try_setsockopt client Sockopt.TCP_QUICKACK true;
  try_setsockopt client Sockopt.TCP_SYNCNT 42;
  try_setsockopt client Sockopt.TCP_WINDOW_CLAMP 32768;

  try_setsockopt listen_sock Sockopt.TCP_FASTOPEN 5;

  let client_sock = Eio.Net.connect ~sw net listening_addr in
  try_setsockopt client_sock Sockopt.IP_FREEBIND true;
  try_setsockopt client_sock Sockopt.IP_BIND_ADDRESS_NO_PORT true;

  Eio.Flow.close client;;
