module type CELL = sig
  type 'a t

  val init : 'a t
  (** The value to give newly-allocated cells. *)

  val segment_order : int
  (** The number of bits to use for the offset into the segment.
      The number of cells per segment is [2 ** segment_order]. *)

  val dump : _ t Fmt.t
  (** For debugging. *)
end
