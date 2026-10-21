defmodule Absinthe.Blueprint.Execution.DeferredFragment do
  @moduledoc false

  # A `@defer`red fragment awaiting resolution. `emitter` is the parent node (a
  # field, or the operation for root-level defers) narrowed to the fragment with
  # its `:defer` flag removed; `path` is the reversed resolution path of the parent.

  @enforce_keys [:label, :emitter, :parent_type, :source, :path]
  defstruct [:label, :emitter, :parent_type, :source, :path]

  @type t :: %__MODULE__{
          label: String.t() | nil,
          emitter:
            Absinthe.Blueprint.Document.Field.t() | Absinthe.Blueprint.Document.Operation.t(),
          parent_type: Absinthe.Type.Object.t(),
          source: term(),
          path: [term()]
        }
end
