open Eio.Std

type t = <
  Eio.Net.t;
  on_listen : Eio.Net.listening_socket Handler.t;
  on_connect : <Eio.Net.stream_socket; Eio.Flow.close> Handler.t;
  on_datagram_socket : <Eio.Net.datagram_socket; Eio.Flow.close> Handler.t;
  on_getaddrinfo : Eio.Net.Sockaddr.t list Handler.t;
  on_getnameinfo : (string * string) Handler.t;
>

let make label =
  let on_listen = Handler.make (`Raise (Failure "Mock listen handler not configured")) in
  let on_connect = Handler.make (`Raise (Failure "Mock connect handler not configured")) in
  let on_datagram_socket = Handler.make (`Raise (Failure "Mock datagram_socket handler not configured")) in
  let on_getaddrinfo = Handler.make (`Raise (Failure "Mock getaddrinfo handler not configured")) in
  let on_getnameinfo = Handler.make (`Raise (Failure "Mock getnameinfo handler not configured")) in
  object
    inherit Eio.Net.t

    method on_listen = on_listen
    method on_connect = on_connect
    method on_datagram_socket = on_datagram_socket
    method on_getaddrinfo = on_getaddrinfo
    method on_getnameinfo = on_getnameinfo

    method listen ~reuse_addr:_ ~reuse_port:_ ~backlog:_ ~sw addr =
      traceln "%s: listen on %a" label Eio.Net.Sockaddr.pp addr;
      let socket = Handler.run on_listen in
      Switch.on_release sw (fun () -> Eio.Flow.close socket);
      socket

    method connect ~sw addr =
      traceln "%s: connect to %a" label Eio.Net.Sockaddr.pp addr;
      let socket = Handler.run on_connect in
      Switch.on_release sw (fun () -> Eio.Flow.close socket);
      socket

    method datagram_socket ~sw addr =
      traceln "%s: datagram_socket %a" label Eio.Net.Sockaddr.pp addr;
      let socket = Handler.run on_datagram_socket in
      Switch.on_release sw (fun () -> Eio.Flow.close socket);
      socket

    method getaddrinfo ~service node =
      traceln "%s: getaddrinfo ~service:%s %s" label service node;
      Handler.run on_getaddrinfo

    method getnameinfo sockaddr =
      traceln "%s: getnameinfo %a" label Eio.Net.Sockaddr.pp sockaddr;
      Handler.run on_getnameinfo
  end

let on_connect (t:t) actions =
  let as_socket x = (x :> <Eio.Net.stream_socket; Eio.Flow.close>) in
  Handler.seq t#on_connect (List.map (Action.map as_socket) actions)

let on_listen (t:t) actions =
  let as_socket x = (x :> <Eio.Net.listening_socket; Eio.Flow.close>) in
  Handler.seq t#on_listen (List.map (Action.map as_socket) actions)

let on_datagram_socket (t:t) actions =
  let as_socket x = (x :> <Eio.Net.datagram_socket; Eio.Flow.close>) in
  Handler.seq t#on_datagram_socket (List.map (Action.map as_socket) actions)

let on_getaddrinfo (t:t) actions = Handler.seq t#on_getaddrinfo actions

let on_getnameinfo (t:t) actions = Handler.seq t#on_getnameinfo actions

type listening_socket = <
  Eio.Net.listening_socket;
  on_accept : (Flow.t * Eio.Net.Sockaddr.stream) Handler.t;
>

let listening_socket label =
  let on_accept = Handler.make (`Raise (Failure "Mock accept handler not configured")) in
  object
    inherit Eio.Net.listening_socket

    method on_accept = on_accept

    method accept ~sw =
      let socket, addr = Handler.run on_accept in
      Flow.attach_to_switch socket sw;
      traceln "%s: accepted connection from %a" label Eio.Net.Sockaddr.pp addr;
      (socket :> <Eio.Net.stream_socket; Eio.Flow.close>), addr

    method close =
      traceln "%s: closed" label
  end

let on_accept (l:listening_socket) actions =
  let as_accept_pair x = (x :> Flow.t * Eio.Net.Sockaddr.stream) in
  Handler.seq l#on_accept (List.map (Action.map as_accept_pair) actions)

type datagram_socket = <
  Eio.Net.datagram_socket;
  close : unit;
  on_recv : (Eio.Net.Sockaddr.datagram * string) Handler.t;
>

let datagram_socket ?(pp=Flow.pp_default) label =
  let on_recv = Handler.make (`Raise (Failure "Mock on_recv handler not configured")) in
  object (_ : datagram_socket)
    inherit Eio.Net.datagram_socket
    method on_recv = on_recv

    method send addr data =
      traceln "%s: send to %a @[<v>%a@]" label Eio.Net.Sockaddr.pp addr pp (Cstruct.to_string data)

    method recv buf =
      let addr, data = Handler.run on_recv in
      let len = String.length data in
      if Cstruct.length buf < len then
        Fmt.failwith "%s: recv buffer too short for %a!" label pp data;
      Cstruct.blit_from_string data 0 buf 0 len;
      traceln "%s: recv from %a @[<v>%a@]" label Eio.Net.Sockaddr.pp addr pp data;
      addr, len

    method close = traceln "%s: closed" label
  end

let on_recv (t:datagram_socket) actions =
  let as_recv_pair x = (x :> Eio.Net.Sockaddr.datagram * string) in
  Handler.seq t#on_recv (List.map (Action.map as_recv_pair) actions)
