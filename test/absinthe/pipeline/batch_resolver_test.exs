defmodule Absinthe.Pipeline.BatchResolverTest do
  use Absinthe.Case, async: true

  alias Absinthe.Pipeline.BatchResolver

  defmodule Schema do
    use Absinthe.Schema

    import Absinthe.Resolution.Helpers, only: [on_load: 2]

    def plugins, do: [Absinthe.Middleware.Dataloader] ++ Absinthe.Plugin.defaults()

    def context(ctx) do
      source = Dataloader.KV.new(fn _batch, ids -> Map.new(ids, &{&1, "loaded"}) end)
      Map.put(ctx, :loader, Dataloader.add_source(Dataloader.new(), :test, source))
    end

    query do
      field :plain, :string do
        resolve(fn _, _ -> {:ok, "ok"} end)
      end

      field :deferred, :string do
        resolve(fn _, _, %{context: %{loader: loader}} ->
          loader
          |> Dataloader.load(:test, :thing, "a")
          |> on_load(&{:ok, Dataloader.get(&1, :test, :thing, "a")})
        end)
      end

      field :boom, :string do
        resolve(fn _, _ -> raise "resolver exploded" end)
      end
    end
  end

  defp blueprint(document) do
    pipeline =
      Schema
      |> Absinthe.Pipeline.for_document(jump_phases: false, context: Schema.context(%{}))
      |> Absinthe.Pipeline.before(Absinthe.Phase.Document.Execution.Resolution)

    {:ok, blueprint, _} = Absinthe.Pipeline.run(document, pipeline)
    blueprint
  end

  defp resolve(documents, options) do
    documents
    |> Enum.map(&blueprint/1)
    |> BatchResolver.run(Keyword.put(options, :schema, Schema))
  end

  defp data(blueprint) do
    {:ok, blueprint, _} =
      Absinthe.Pipeline.run(blueprint, [Absinthe.Phase.Document.Result])

    blueprint.result
  end

  describe "abort_on_error: false" do
    test "replaces only the raising document with the :error sentinel" do
      assert [first, :error, third] =
               resolve(["{ plain }", "{ boom }", "{ plain }"], abort_on_error: false)

      assert %{data: %{"plain" => "ok"}} = data(first)
      assert %{data: %{"plain" => "ok"}} = data(third)
    end

    # The regression. A raising document is replaced by :error, and do_resolve/6
    # feeds the result list back into execute/5 for each additional resolution
    # round. Every document above resolves in ONE round, so the sentinel is never
    # re-read; a pending dataloader batch forces a second round, where it is.
    test "keeps the sentinel across additional resolution rounds" do
      assert [first, :error, third] =
               resolve(["{ deferred }", "{ boom }", "{ deferred }"], abort_on_error: false)

      assert %{data: %{"deferred" => "loaded"}} = data(first)
      assert %{data: %{"deferred" => "loaded"}} = data(third)
    end

    test "resolves a batch that needs several rounds and never raises" do
      assert [first, second] = resolve(["{ deferred }", "{ plain }"], abort_on_error: false)

      assert %{data: %{"deferred" => "loaded"}} = data(first)
      assert %{data: %{"plain" => "ok"}} = data(second)
    end
  end

  describe "abort_on_error: true" do
    test "lets the exception through" do
      assert_raise RuntimeError, "resolver exploded", fn ->
        resolve(["{ plain }", "{ boom }"], abort_on_error: true)
      end
    end
  end
end
