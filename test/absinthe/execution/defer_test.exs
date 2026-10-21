defmodule Absinthe.Execution.DeferTest do
  use Absinthe.Case, async: true

  import ExUnit.CaptureLog

  defmodule Schema do
    use Absinthe.Schema

    import Absinthe.Resolution.Helpers, only: [on_load: 2]

    import_directives Absinthe.Type.BuiltIns.IncrementalDirectives

    def plugins, do: [Absinthe.Middleware.Dataloader | Absinthe.Plugin.defaults()]

    def context(%{test_pid: pid} = ctx) do
      source =
        Dataloader.KV.new(fn :loaded, ids ->
          send(pid, {:batch, ids})
          Map.new(ids, &{&1, "loaded-" <> &1})
        end)

      Map.put(ctx, :loader, Dataloader.add_source(Dataloader.new(), :kv, source))
    end

    object :item do
      field :id, non_null(:id)

      field :slow, :string do
        resolve fn %{id: id}, _, %{context: %{test_pid: pid}} ->
          send(pid, {:slow, id})
          {:ok, "slow-" <> id}
        end
      end

      field :loaded, :string do
        resolve fn %{id: id}, _, %{context: %{loader: loader}} ->
          loader
          |> Dataloader.load(:kv, :loaded, id)
          |> on_load(fn loader -> {:ok, Dataloader.get(loader, :kv, :loaded, id)} end)
        end
      end

      field :child, :item do
        resolve fn %{id: id}, _, _ -> {:ok, %{id: id <> "-child"}} end
      end

      field :required, non_null(:string) do
        resolve fn _, _, _ -> {:ok, nil} end
      end

      field :boom, :string do
        resolve fn _, _, _ -> raise "boom" end
      end
    end

    query do
      field :item, :item do
        resolve fn _, info ->
          send(
            info.context.test_pid,
            {:projected, Enum.map(Absinthe.Resolution.project(info), & &1.name)}
          )

          {:ok, %{id: "1"}}
        end
      end

      field :items, list_of(:item) do
        resolve fn _, _, _ -> {:ok, [%{id: "1"}, %{id: "2"}, %{id: "3"}]} end
      end
    end

    mutation do
      field :touch, :item do
        resolve fn _, _, _ -> {:ok, %{id: "1"}} end
      end
    end
  end

  defp execute(document, incremental? \\ true) do
    pipeline = Absinthe.Pipeline.for_document(Schema, context: %{test_pid: self()})
    pipeline = if incremental?, do: Absinthe.Incremental.enable(pipeline), else: pipeline
    {:ok, bp, _phases} = Absinthe.Pipeline.run(document, pipeline)
    bp
  end

  test "a deferred fragment is left out of the initial result and resolved by next/2" do
    bp = execute(~S|{ item { id ... @defer(label: "s") { slow } } }|)

    assert bp.result == %{data: %{"item" => %{"id" => "1"}}}
    refute_received {:slow, _}
    assert_received {:projected, ["id", "slow"]}
    assert %{hasNext: true} = Absinthe.Incremental.initial_payload(bp)

    assert {%{
              incremental: [%{data: %{"slow" => "slow-1"}, path: ["item"], label: "s"}],
              hasNext: false
            }, _bp} = Absinthe.Incremental.next(bp)

    assert_received {:slow, "1"}
  end

  test "a deferred named fragment without a label" do
    bp =
      execute("""
      { item { id ...F @defer } }
      fragment F on Item { slow }
      """)

    assert bp.result == %{data: %{"item" => %{"id" => "1"}}}
    refute_received {:slow, _}

    assert {%{incremental: [item], hasNext: false}, _} = Absinthe.Incremental.next(bp)
    assert item == %{data: %{"slow" => "slow-1"}, path: ["item"]}
  end

  test "one wave shares a single dataloader batch" do
    bp = execute("{ items { id ... @defer { loaded } } }")

    refute_received {:batch, _}
    assert {%{incremental: items, hasNext: false}, _} = Absinthe.Incremental.next(bp)

    assert_received {:batch, ids}
    assert ids |> Enum.to_list() |> Enum.sort() == ["1", "2", "3"]
    refute_received {:batch, _}

    assert items == [
             %{data: %{"loaded" => "loaded-1"}, path: ["items", 0]},
             %{data: %{"loaded" => "loaded-2"}, path: ["items", 1]},
             %{data: %{"loaded" => "loaded-3"}, path: ["items", 2]}
           ]
  end

  test "a defer nested in a deferred fragment forms the next wave" do
    bp =
      execute(
        ~S|{ item { ... @defer(label: "outer") { child { id ... @defer(label: "inner") { slow } } } } }|
      )

    assert bp.result == %{data: %{"item" => %{}}}

    assert {%{
              incremental: [
                %{data: %{"child" => %{"id" => "1-child"}}, path: ["item"], label: "outer"}
              ],
              hasNext: true
            }, bp} = Absinthe.Incremental.next(bp)

    refute_received {:slow, _}

    assert {%{
              incremental: [
                %{data: %{"slow" => "slow-1-child"}, path: ["item", "child"], label: "inner"}
              ],
              hasNext: false
            }, _} = Absinthe.Incremental.next(bp)
  end

  test "a root-level defer" do
    bp = execute("{ ... @defer { item { id } } }")

    assert bp.result == %{data: %{}}

    assert {%{incremental: [%{data: %{"item" => %{"id" => "1"}}, path: []}], hasNext: false}, _} =
             Absinthe.Incremental.next(bp)
  end

  test "fragments under a nulled-out parent are dropped" do
    bp = execute("{ items { id required ... @defer { slow } } }")

    assert %{data: %{"items" => [nil, nil, nil]}} = bp.result
    assert {%{hasNext: false}, _} = Absinthe.Incremental.next(bp)
    refute_received {:slow, _}
  end

  test "a non-null violation inside a deferred fragment nulls the fragment" do
    bp = execute("{ item { id ... @defer { required } } }")

    assert {%{incremental: [%{data: nil, path: ["item"], errors: [error]}], hasNext: false}, _} =
             Absinthe.Incremental.next(bp)

    assert error.path == ["item", "required"]
  end

  test "a raising deferred resolver ends the stream with an internal error" do
    bp = execute(~S|{ item { id ... @defer(label: "b") { boom } } }|)

    capture_log(fn ->
      assert {%{
                incremental: [
                  %{
                    data: nil,
                    path: ["item"],
                    label: "b",
                    errors: [%{message: "Internal server error", path: ["item"]}]
                  }
                ],
                hasNext: false
              }, bp} = Absinthe.Incremental.next(bp)

      refute Absinthe.Incremental.pending?(bp)
    end)
  end

  test "without enable/1 deferred fields resolve inline" do
    bp = execute("{ item { id ... @defer { slow } } }", false)

    assert bp.result == %{data: %{"item" => %{"id" => "1", "slow" => "slow-1"}}}
    refute Absinthe.Incremental.pending?(bp)
  end

  test "@defer(if: false) resolves inline" do
    bp = execute("{ item { id ... @defer(if: false) { slow } } }")

    assert bp.result == %{data: %{"item" => %{"id" => "1", "slow" => "slow-1"}}}
    refute Absinthe.Incremental.pending?(bp)
  end

  test "a defer on a mutation root resolves inline" do
    bp = execute("mutation { ... @defer { touch { id } } }")

    assert bp.result == %{data: %{"touch" => %{"id" => "1"}}}
    refute Absinthe.Incremental.pending?(bp)
  end
end
