# Incremental Delivery

> **Note:** `@defer` and `@stream` come from the [Incremental Delivery RFC](https://github.com/graphql/graphql-spec/blob/main/rfcs/DeferStream.md) and are not part of the finalized GraphQL specification yet.

`@defer` lets a client mark fragments it can wait for. The server answers with an initial payload that leaves those fragments out, then sends the deferred data in later payloads on the same response.

## Enabling the directives

Import the directives into your schema:

```elixir
defmodule MyAppWeb.Schema do
  use Absinthe.Schema

  import_directives Absinthe.Type.BuiltIns.IncrementalDirectives

  query do
    # ...
  end
end
```

On their own, the directives only validate: a deferred fragment resolves inline like any other fragment, and `@stream` always returns the complete list. That is a valid execution under the RFC, since servers may ignore both directives.

## Executing `@defer` incrementally

Pass the document pipeline through `Absinthe.Incremental.enable/1`, run it, and drive the deferred waves with `Absinthe.Incremental.next/2`:

```elixir
pipeline =
  MyAppWeb.Schema
  |> Absinthe.Pipeline.for_document(context: context)
  |> Absinthe.Incremental.enable()

{:ok, blueprint, _phases} = Absinthe.Pipeline.run(document, pipeline)

send_payload(Absinthe.Incremental.initial_payload(blueprint))

deliver = fn
  blueprint, deliver ->
    {payload, blueprint} = Absinthe.Incremental.next(blueprint)
    send_payload(payload)
    if payload.hasNext, do: deliver.(blueprint, deliver)
end

if Absinthe.Incremental.pending?(blueprint), do: deliver.(blueprint, deliver)
```

`absinthe_plug` does this for you over HTTP when configured for it.

### Execution model

- During the initial resolution, every `@defer`red fragment whose type condition matches is recorded instead of resolved. The deferred fields' resolvers do not run, so the initial payload is ready as soon as the rest of the document is.
- Each call to `next/2` resolves one **wave**: every fragment recorded so far, in the calling process, sharing one resolution state. A single dataloader or batch round covers all of them.
- `@defer`s found while resolving a wave form the next wave.
- A fragment whose parent was nulled out (for example by a non-null violation) is dropped.
- `@defer` directly on the root of a mutation or subscription executes inline; defers below the root still apply.
- `@defer(if: false)` executes inline.
- `Absinthe.Resolution.project/2` still returns deferred fields, so look-ahead in a parent resolver sees everything its children may need.

## Payloads

Payloads use the 2022 format (`deferSpec=20220824`) understood by urql and Apollo Client.

Initial payload:

```elixir
%{data: %{"item" => %{"id" => "1"}}, hasNext: true}
```

Each subsequent payload carries one item per fragment of the wave. `path` points at the object the fragment's data merges into; `label` is present when the directive set one, `errors` when resolution produced any:

```elixir
%{
  incremental: [
    %{data: %{"name" => "Foo"}, path: ["item"], label: "rest"}
  ],
  hasNext: false
}
```

A non-null violation inside a fragment nulls that fragment's `data` and reports the error in its `errors`.

If a deferred resolver raises, the exception is logged and the wave is answered with `"Internal server error"` items and `hasNext: false`, so the response can still end cleanly.
