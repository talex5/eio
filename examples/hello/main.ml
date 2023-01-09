open Eio.Std

let () =
  print_endline "";
  Eio_mock.Backend.run @@ fun () ->
  let s = Eio.Stream.create 1 in
  try
    Fiber.both
      (fun () ->
         for x = 1 to 3 do
           traceln "Sending %d" x;
           Eio.Stream.add s x
         done;
         raise Exit
      )
      (fun () ->
         while true do
           traceln "Got %d" (Eio.Stream.take s)
         done
      )
  with Exit ->
    traceln "Finished!"
