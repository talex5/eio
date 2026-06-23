# Linux-specific networking tests

```ocaml
# #require "eio_linux";;
# #require "eio.unix";;
# open Eio.Std;;
# let addr = `Tcp (Eio.Net.Ipaddr.V4.loopback, 0);;
val addr : [> `Tcp of [> `V4 ] Eio.Net.Ipaddr.t * int ] =
  `Tcp ("\127\000\000\001", 0)
```

```ocaml
module LL = Eio_linux.Low_level

let try_getsockopt sock opt =
  match Eio.Net.getsockopt sock opt with
  | v -> traceln "%a" Eio.Net.Sockopt.pp_binding (opt, v)
  | exception ex -> traceln "%a -> %a" Eio.Net.Sockopt.pp opt Fmt.exn ex

let try_setsockopt sock opt v =
  match Eio.Net.setsockopt sock opt v with
  | () -> try_getsockopt sock opt
  | exception (Unix.Unix_error(Unix.EINVAL, "setsockopt", _)) ->
    traceln "%a -> EINVAL" Eio.Net.Sockopt.pp_binding (opt, v)
  | exception (Invalid_argument msg) ->
    traceln "%a -> Invalid_argument %s" Eio.Net.Sockopt.pp_binding (opt, v) msg
```

## Getting and setting socket options

Set the TCP congestion-control algorithm and read it back. "reno" is always
available, so the round-trip is deterministic; the value is returned without the
kernel's NUL padding, so we get back exactly what was set:

```ocaml
# Eio_linux.run @@ fun env ->
  Switch.run @@ fun sw ->
  let server = Eio.Net.listen env#net ~sw ~reuse_addr:true ~backlog:1 addr in
  try_setsockopt server LL.TCP_CONGESTION "reno";;
+TCP_CONGESTION = reno
- : unit = ()
```

## Linux-specific TCP socket options

Test Linux-specific TCP socket options:

```ocaml
# Eio_linux.run @@ fun env ->
  Switch.run @@ fun sw ->
  let net = env#net in
  let listen_sock = Eio.Net.listen net ~sw ~reuse_addr:true ~backlog:5 addr in
  let listening_addr = Eio.Net.listening_addr listen_sock in
  (* Test TCP_DEFER_ACCEPT on listening socket *)
  Eio.Net.setsockopt listen_sock LL.TCP_DEFER_ACCEPT 5;
  let defer = Eio.Net.getsockopt listen_sock LL.TCP_DEFER_ACCEPT in
  traceln "TCP_DEFER_ACCEPT on listening socket: %s" (if defer > 0 then "enabled" else "disabled");

  (* Create a TCP connection for other tests *)
  Eio.Fiber.both
    (fun () ->
      let client = Eio.Net.connect ~sw net listening_addr in

      try_setsockopt client LL.TCP_CORK true;
      try_setsockopt client LL.TCP_CORK false;
      try_setsockopt client LL.TCP_KEEPIDLE 60;
      try_setsockopt client LL.TCP_KEEPINTVL 10;
      try_setsockopt client LL.TCP_KEEPCNT 5;
      try_setsockopt client LL.TCP_LINGER2 (Some 110);
      try_getsockopt client LL.TCP_CONGESTION;

      (* Try to set cubic if available - may fail based on system config *)
      (try
        try_setsockopt client LL.TCP_CONGESTION "cubic";
      with _ ->
        traceln "Could not change TCP_CONGESTION (normal for unprivileged)");

      try_setsockopt client LL.TCP_QUICKACK true;
      try_setsockopt client LL.TCP_SYNCNT 42;
      try_setsockopt client LL.TCP_WINDOW_CLAMP 32768;

      Eio.Flow.close client)
    (fun () ->
      let conn, _addr = Eio.Net.accept ~sw listen_sock in
      Eio.Flow.close conn);;
+TCP_DEFER_ACCEPT on listening socket: enabled
+TCP_CORK = true
+TCP_CORK = false
+TCP_KEEPIDLE = 60
+TCP_KEEPINTVL = 10
+TCP_KEEPCNT = 5
+TCP_LINGER2 = 110
+TCP_CONGESTION = cubic
+TCP_CONGESTION = cubic
+TCP_QUICKACK = true
+TCP_SYNCNT = 42
+TCP_WINDOW_CLAMP = 32768
- : unit = ()
```

Test additional Linux socket options:

```ocaml
# Eio_linux.run @@ fun env ->
  Switch.run @@ fun sw ->
  let net = env#net in
  (* Test TCP_FASTOPEN on listening socket *)
  let listen_sock = Eio.Net.listen net ~sw ~reuse_addr:true ~backlog:5 addr in
  let listening_addr = Eio.Net.listening_addr listen_sock in
  try_setsockopt listen_sock LL.TCP_FASTOPEN 5;
  (* TODO avsm: implement client support to test this properly *)
  (* Test IP options with connection *)
  Eio.Fiber.both
    (fun () ->
      let client_sock = Eio.Net.connect ~sw net listening_addr in
      try_setsockopt client_sock LL.IP_FREEBIND true;
      try_setsockopt client_sock LL.IP_BIND_ADDRESS_NO_PORT true;
      Eio.Flow.close client_sock)
    (fun () ->
      let conn, _addr = Eio.Net.accept ~sw listen_sock in
      Eio.Flow.close conn);;
+TCP_FASTOPEN = 5
+IP_FREEBIND = true
+IP_BIND_ADDRESS_NO_PORT = true
- : unit = ()
```

## Socket option validation tests

Test that invalid values are rejected properly:

```ocaml
# Eio_linux.run @@ fun env ->
  Switch.run @@ fun sw ->
  let net = env#net in

  (* Create a TCP socket for testing *)
  let listen_sock = Eio.Net.listen net ~sw ~reuse_addr:true ~backlog:5 addr in
  let listening_addr = Eio.Net.listening_addr listen_sock in

  let client = Eio.Net.connect ~sw net listening_addr in

  try_setsockopt client LL.TCP_SYNCNT 256;
  try_setsockopt client LL.TCP_SYNCNT (-1);
  try_setsockopt client LL.TCP_SYNCNT 0;

  (* Valid TCP_SYNCNT values should work *)
  try_setsockopt client LL.TCP_SYNCNT 3;
  try_setsockopt client LL.TCP_SYNCNT 6;

  try_setsockopt client LL.TCP_KEEPIDLE (-1);
  try_setsockopt client LL.TCP_WINDOW_CLAMP (-100);

  try_setsockopt client LL.TCP_LINGER2 (Some (-2));
  Eio.Net.setsockopt client LL.TCP_LINGER2 None;
  traceln "TCP_LINGER2 accepts None for system default";

  try_setsockopt client LL.IP_TTL 64;
  try_setsockopt client LL.IP_TTL 0;
  try_setsockopt client LL.IP_TTL 256;

  try_setsockopt client LL.IP_MTU_DISCOVER `Dont;

  (* Test IP_MTU (read-only, should work on connected socket) *)
  try_getsockopt client LL.IP_MTU;

  (* Test that IP_MTU cannot be set *)
  try_setsockopt client LL.IP_MTU 1500;

  (* For IP_LOCAL_PORT_RANGE we ignore ENOPROTOOPT since its >6.3 Linux only *)

  (* Test IP_LOCAL_PORT_RANGE validation *)
  (* Lower bound > upper bound *)
  try_setsockopt client LL.IP_LOCAL_PORT_RANGE (2000, 1000);

  (* Test out of range port values *)
  try_setsockopt client LL.IP_LOCAL_PORT_RANGE (40000, 70000);

  (* Valid port range should work *)
  try_setsockopt client LL.IP_LOCAL_PORT_RANGE (40000, 50000);

  (* Reset to system default *)
  Eio.Net.setsockopt client LL.IP_LOCAL_PORT_RANGE (0, 0);
  traceln "IP_LOCAL_PORT_RANGE reset";

  Eio.Flow.close client;;
+TCP_SYNCNT = 256 -> Invalid_argument TCP_SYNCNT must be between 1 and 255, got 256
+TCP_SYNCNT = -1 -> Invalid_argument TCP_SYNCNT must be between 1 and 255, got -1
+TCP_SYNCNT = 0 -> Invalid_argument TCP_SYNCNT must be between 1 and 255, got 0
+TCP_SYNCNT = 3
+TCP_SYNCNT = 6
+TCP_KEEPIDLE = -1 -> EINVAL
+TCP_WINDOW_CLAMP = -100 -> Invalid_argument TCP_WINDOW_CLAMP must be non-negative, got -100
+TCP_LINGER2 = -2 -> Invalid_argument TCP_LINGER2 must be non-negative, got -2
+TCP_LINGER2 accepts None for system default
+IP_TTL = 64
+IP_TTL = 0 -> EINVAL
+IP_TTL = 256 -> EINVAL
+IP_MTU_DISCOVER = Dont
+IP_MTU = 65535
+IP_MTU = 1500 -> Invalid_argument IP_MTU is a read-only socket option
+IP_LOCAL_PORT_RANGE = (2000, 1000) -> EINVAL
+IP_LOCAL_PORT_RANGE = (40000, 70000) -> EINVAL
+IP_LOCAL_PORT_RANGE = (40000, 50000)
+IP_LOCAL_PORT_RANGE reset
- : unit = ()
```
