#include <stdio.h>
#include <caml/mlvalues.h>

CAMLexport value caml_dump_variant(value name, value variant) {
  printf("#define v_%s %d\n", String_val(name), (int) variant);
  return Val_unit;
}
