let () =
  Eio_main.run @@ fun env ->
  Eio.traceln "is_dir fs = %b"  (Eio.Path.is_directory env#fs);
  Eio.traceln "is_dir cwd = %b" (Eio.Path.is_directory env#cwd);
