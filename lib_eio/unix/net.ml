open Eio.Std

module Ipaddr = struct
  let to_unix : _ Eio.Net.Ipaddr.t -> Unix.inet_addr = Obj.magic
  let of_unix : Unix.inet_addr -> _ Eio.Net.Ipaddr.t = Obj.magic
end

class virtual stream_socket = object (_ : <Resource.t; Eio.Flow.close; ..>)
  inherit Eio.Flow.two_way

  method virtual send_msg :
    ?dst:Eio.Net.Sockaddr.stream -> fds:Fd.t list -> Cstruct.t list -> unit

  method virtual recv_msg_with_fds :
    sw:Switch.t -> max_fds:int -> Cstruct.t list -> Eio.Net.Sockaddr.stream * int * Fd.t list

end

let send_msg (t:#stream_socket) = t#send_msg
let recv_msg_with_fds (t:#stream_socket) = t#recv_msg_with_fds

class virtual t = object
  inherit Eio.Net.t
  method virtual connect_unix : sw:Switch.t -> Eio.Net.Sockaddr.stream -> stream_socket
end

let stream_addr_to_unix = function
  | `Unix path -> Unix.ADDR_UNIX path
  | `Tcp (host, port)  ->
    let host = Ipaddr.to_unix host in
    Unix.ADDR_INET (host, port)

let stream_addr_of_unix = function
  | Unix.ADDR_UNIX path -> `Unix path
  | Unix.ADDR_INET (host, port) ->
    let host = Ipaddr.of_unix host in
    `Tcp (host, port)
