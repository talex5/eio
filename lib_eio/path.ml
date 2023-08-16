type 'a t = 'a Fs.dir * Fs.path

let ( / ) (dir, p1) p2 =
  match p1, p2 with
  | p1, "" -> (dir, Filename.concat p1 p2)
  | _, p2 when not (Filename.is_relative p2) -> (dir, p2)
  | ".", p2 -> (dir, p2)
  | p1, p2 -> (dir, Filename.concat p1 p2)

let pp f (Resource.T (t, ops), p) =
  let module X = (val (Resource.get ops Fs.Pi.Dir)) in
  if p = "" then Fmt.pf f "<%a>" X.pp t
  else Fmt.pf f "<%a:%s>" X.pp t (String.escaped p)

let native (Resource.T (t, ops), p) =
  let module X = (val (Resource.get ops Fs.Pi.Dir)) in
  X.native t p

let native_exn t =
  match native t with
  | Some p -> p
  | None ->
    raise (Fs.err (Not_native (Fmt.str "%a" pp t)))

let split_last_seg p =
  (* Get the last instance of a character in a string *)
  let rec rindex i c path =
    if i < 0 then None else
    if Char.equal (String.unsafe_get path i) c
    then Some i
    else rindex (i - 1) c path
  in
  (* Trim the last instances of a character in a string *)
  let rtrim c path =
    let len = String.length path in
    let rec remove i =
      if i > len then i else
      if Char.equal (String.unsafe_get path (len - 1 - i)) c then remove (i + 1)
      else i
    in
    let remove_last = remove 0 in
    String.sub path 0 (len - remove_last)
  in
  let len = String.length p - 1 in
  match rindex len '/' p with
  | None -> None, Some p
  | Some idx ->
    (* We want the separator in the head not the tail *)
    let idx = idx + 1 in
    let child = String.sub p idx (len - idx + 1) in
    Some (rtrim '/' @@ String.sub p 0 idx), (if String.equal child "" then None else Some child)

let open_in ~sw t =
  let (Resource.T (dir, ops), path) = t in
  let module X = (val (Resource.get ops Fs.Pi.Dir)) in
  try X.open_in dir ~sw path
  with Exn.Io _ as ex ->
    let bt = Printexc.get_raw_backtrace () in
    Exn.reraise_with_context ex bt "opening %a" pp t

let open_out ~sw ?(append=false) ~create t =
  let (Resource.T (dir, ops), path) = t in
  let module X = (val (Resource.get ops Fs.Pi.Dir)) in
  try X.open_out dir ~sw ~append ~create path
  with Exn.Io _ as ex ->
    let bt = Printexc.get_raw_backtrace () in
    Exn.reraise_with_context ex bt "opening %a" pp t

let open_dir ~sw t =
  let (Resource.T (dir, ops), path) = t in
  let module X = (val (Resource.get ops Fs.Pi.Dir)) in
  try X.open_dir dir ~sw path, ""
  with Exn.Io _ as ex ->
    let bt = Printexc.get_raw_backtrace () in
    Exn.reraise_with_context ex bt "opening directory %a" pp t

let mkdir ~perm t =
  let (Resource.T (dir, ops), path) = t in
  let module X = (val (Resource.get ops Fs.Pi.Dir)) in
  try X.mkdir dir ~perm path
  with Exn.Io _ as ex ->
    let bt = Printexc.get_raw_backtrace () in
    Exn.reraise_with_context ex bt "creating directory %a" pp t

let read_dir t =
  let (Resource.T (dir, ops), path) = t in
  let module X = (val (Resource.get ops Fs.Pi.Dir)) in
  try List.sort String.compare (X.read_dir dir path)
  with Exn.Io _ as ex ->
    let bt = Printexc.get_raw_backtrace () in
    Exn.reraise_with_context ex bt "reading directory %a" pp t

let stat ~follow t =
  let (Resource.T (dir, ops), path) = t in
  let module X = (val (Resource.get ops Fs.Pi.Dir)) in
  try X.stat ~follow dir path
  with Exn.Io _ as ex ->
    let bt = Printexc.get_raw_backtrace () in
    Exn.reraise_with_context ex bt "examining %a" pp t

let with_open_in path fn =
  Switch.run @@ fun sw -> fn (open_in ~sw path)

let with_open_out ?append ~create path fn =
  Switch.run @@ fun sw -> fn (open_out ~sw ?append ~create path)

let with_open_dir path fn =
  Switch.run @@ fun sw -> fn (open_dir ~sw path)

let with_lines path fn =
  with_open_in path @@ fun flow ->
  let buf = Buf_read.of_flow flow ~max_size:max_int in
  fn (Buf_read.lines buf)

let load (t, path) =
  with_open_in (t, path) @@ fun flow ->
  try
    let size = File.size flow in
    if Optint.Int63.(compare size (of_int Sys.max_string_length)) = 1 then
      raise @@ Fs.err File_too_large;
    let buf = Cstruct.create (Optint.Int63.to_int size) in
    let rec loop buf got =
      match Flow.single_read flow buf with
      | n -> loop (Cstruct.shift buf n) (n + got)
      | exception End_of_file -> got
    in
    let got = loop buf 0 in
    Cstruct.to_string ~len:got buf
  with Exn.Io _ as ex ->
    let bt = Printexc.get_raw_backtrace () in
    Exn.reraise_with_context ex bt "loading %a" pp (t, path)

let save ?append ~create path data =
  with_open_out ?append ~create path @@ fun flow ->
  Flow.copy_string data flow

let unlink t =
  let (Resource.T (dir, ops), path) = t in
  let module X = (val (Resource.get ops Fs.Pi.Dir)) in
  try X.unlink dir path
  with Exn.Io _ as ex ->
    let bt = Printexc.get_raw_backtrace () in
    Exn.reraise_with_context ex bt "removing file %a" pp t

let rmdir t =
  let (Resource.T (dir, ops), path) = t in
  let module X = (val (Resource.get ops Fs.Pi.Dir)) in
  try X.rmdir dir path
  with Exn.Io _ as ex ->
    let bt = Printexc.get_raw_backtrace () in
    Exn.reraise_with_context ex bt "removing directory %a" pp t

let rename t1 t2 =
  let (dir2, new_path) = t2 in
  let (Resource.T (dir, ops), old_path) = t1 in
  let module X = (val (Resource.get ops Fs.Pi.Dir)) in
  try X.rename dir old_path (dir2 :> _ Fs.dir) new_path
  with Exn.Io _ as ex ->
    let bt = Printexc.get_raw_backtrace () in
    Exn.reraise_with_context ex bt "renaming %a to %a" pp t1 pp t2

let exists ~follow path =
  try ignore (stat ~follow path : File.Stat.t); true
  with Exn.Io (Fs.E Not_found _, _) -> false

let rec mkdirs ?(exists_ok=false) ~perm (dir, name) =
  match
    let parent, child =
      match split_last_seg name with
      | (Some _ as p), (Some _ as c) -> p, c
      | Some parent, None -> split_last_seg parent
      | None, _ -> None, None
    in
    match parent, child with
    | Some parent, Some _ -> (
        let parent = (dir, parent) in
        if not (exists ~follow:false parent) then mkdirs ~exists_ok:true ~perm parent
      )
    | _ -> ()
  with
  | exception (Exn.Io _ as ex) ->
    let bt = Printexc.get_raw_backtrace () in
    Exn.reraise_with_context ex bt "creating directory %a" pp (dir, name)
  | () ->
    try mkdir (dir, name) ~perm
    with Exn.Io ((Fs.E Fs.Already_exists _), _) when exists_ok -> ()
