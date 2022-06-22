```ocaml
# #require "eio";;
# #require "eio.mock";;
# #require "eio_main";;
```
```ocaml
open Eio.Std

module W = Eio.Buf_write

let flow = Eio_mock.Flow.make "flow"
```

## A simple run-through

```ocaml
# Eio_main.run @@ fun _ ->
  W.with_flow flow @@ fun w ->
  W.write_string w "Hello"; W.write_char w ' '; W.write_string w "world";;
+flow: wrote "Hello world"
- : unit = ()
```

## Auto-commit

If we yield then we flush the data so far:

```ocaml
# Eio_main.run @@ fun _ ->
  W.with_flow flow @@ fun w ->
  W.write_string w "Hello"; W.write_char w ' ';
  Fiber.yield ();
  W.write_string w "world";;
+flow: wrote "Hello "
+flow: wrote "world"
- : unit = ()
```
