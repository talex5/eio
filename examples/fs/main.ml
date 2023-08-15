let ( / ) = Eio.Path.( / )

let find ~output ~pattern dir =
  let pattern = Str.regexp_string_case_fold pattern in
  Eio.Buf_write.with_flow output @@ fun w ->
  let rec scan dir =
    Eio.Path.read_dir dir |> List.iter (fun item ->
        begin
          match Str.search_forward pattern item 0 with
          | _ ->
            Eio.Buf_write.string w item;
            Eio.Buf_write.char w '\n';
          | exception Not_found -> ()
        end;
        let item = dir / item in
        let info = Eio.Path.stat item ~follow:false in
        if info.kind = `Directory then scan item
      )
  in
  scan dir

let () =
  Eio_main.run @@ fun env ->
  let output = env#stdout in
  match Array.to_list Sys.argv with
  | [] -> failwith "Missing program name!"
  | [prog] -> Fmt.failwith "Usage: %s PATTERN [DIR]..." prog
  | [_; pattern] -> find ~output ~pattern env#cwd
  | _ :: pattern :: dirs ->
    dirs |> List.iter (fun dir ->
        Eio.Path.with_open_dir (env#fs / dir) (find ~output ~pattern)
      )
