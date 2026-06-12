let variants = [
  "Regular_file", `Regular_file;
  "Socket", `Socket;
  "Symbolic_link", `Symbolic_link;
  "Block_device", `Block_device;
  "Directory", `Directory;
  "Character_special", `Character_special;
  "Fifo", `Fifo;
  "Unknown", `Unknown;
]

external dump_variant : string -> Eio.File.Stat.kind -> unit = "caml_dump_variant"

let () = List.iter (fun (k, v) -> dump_variant k v) variants
