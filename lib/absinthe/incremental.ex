defmodule Absinthe.Incremental do
  @moduledoc """
  Executes `@defer` incrementally.

  With the pipeline passed through `enable/1`, the resolution phase leaves every
  `@defer`red fragment out of the result and records it instead. The initial result
  is then complete and can be sent right away; each call to `next/2` resolves one
  wave of the recorded fragments and returns its payload. Fragments deferred inside
  a wave form the next one.

  All fragments of a wave resolve in the calling process with one shared resolution
  state, so a single dataloader batch covers them. Payloads follow the 2022 incremental
  delivery format (`deferSpec=20220824`):

      # initial_payload/1
      %{data: %{"item" => %{"id" => "1"}}, hasNext: true}

      # next/2
      %{incremental: [%{data: %{"name" => "Foo"}, path: ["item"], label: "rest"}], hasNext: false}

  `@defer` directly on the root of a mutation or subscription executes inline.
  Without `enable/1`, deferred fragments resolve inline like any other fragment.

  ## Example

      pipeline =
        MyApp.Schema
        |> Absinthe.Pipeline.for_document(context: context)
        |> Absinthe.Incremental.enable()

      {:ok, blueprint, _phases} = Absinthe.Pipeline.run(document, pipeline)
      send_payload(Absinthe.Incremental.initial_payload(blueprint))

      Stream.unfold({Absinthe.Incremental.pending?(blueprint), blueprint}, fn
        {false, _} -> nil
        {true, blueprint} ->
          {payload, blueprint} = Absinthe.Incremental.next(blueprint)
          {payload, {payload.hasNext, blueprint}}
      end)
      |> Enum.each(&send_payload/1)
  """

  alias Absinthe.Blueprint
  alias Absinthe.Blueprint.Execution.DeferredFragment
  alias Absinthe.Phase.Document.Execution.Resolution
  alias Absinthe.Phase.Document.Result

  @type payload :: %{
          optional(:data) => map | nil,
          optional(:errors) => [map],
          optional(:incremental) => [map],
          required(:hasNext) => boolean
        }

  @doc """
  Enables `@defer` on the resolution phase of a pipeline.
  """
  @spec enable(Absinthe.Pipeline.t()) :: Absinthe.Pipeline.t()
  def enable(pipeline) do
    pipeline
    |> List.flatten()
    |> Enum.map(fn
      {Resolution, opts} -> {Resolution, Keyword.put(opts, :incremental, true)}
      Resolution -> {Resolution, [incremental: true]}
      phase -> phase
    end)
  end

  @doc """
  Whether deferred fragments are waiting to be resolved.
  """
  @spec pending?(Blueprint.t()) :: boolean
  def pending?(%Blueprint{execution: %{acc: acc}}) do
    match?([_ | _], Map.get(acc, DeferredFragment))
  end

  @doc """
  The initial payload: the formatted result plus `hasNext`.
  """
  @spec initial_payload(Blueprint.t()) :: payload
  def initial_payload(%Blueprint{} = bp) do
    Map.put(bp.result, :hasNext, pending?(bp))
  end

  @doc """
  Resolves the next wave of deferred fragments and returns its payload.

  Fragments whose parent was nulled out are dropped. `result_opts` are the options
  of the `Absinthe.Phase.Document.Result` phase, used to format errors.
  """
  @spec next(Blueprint.t(), Keyword.t()) :: {payload, Blueprint.t()}
  def next(%Blueprint{} = bp, result_opts \\ []) do
    records = bp.execution.acc |> Map.get(DeferredFragment, []) |> Enum.reverse()
    bp = put_in(bp.execution.acc[DeferredFragment], [])
    data = Map.get(bp.result, :data)

    records
    |> Enum.map(&{&1, Absinthe.Resolution.path(%{path: &1.path})})
    |> Enum.filter(fn {_record, path} -> is_map(get_at(data, path)) end)
    |> case do
      [] -> {%{hasNext: false}, bp}
      records -> resolve(bp, records, result_opts)
    end
  end

  defp resolve(bp, records, result_opts) do
    {bp, nodes} = Resolution.resolve_deferred(bp, Enum.map(records, &elem(&1, 0)))

    items =
      Enum.zip_with(records, nodes, fn {record, path}, node ->
        {data, errors} = Result.format_node(node, result_opts)
        item(record, path, data, errors)
      end)

    data =
      Enum.reduce(items, bp.result.data, fn
        %{data: nil}, data -> data
        %{data: item_data, path: path}, data -> merge_at(data, path, item_data)
      end)

    bp = %{bp | result: Map.put(bp.result, :data, data)}

    {%{incremental: items, hasNext: pending?(bp)}, bp}
  rescue
    exception ->
      # The response is already underway, so the stream has to end cleanly.
      Absinthe.Pipeline.BatchResolver.pipeline_error(exception, __STACKTRACE__)

      items =
        Enum.map(records, fn {record, path} ->
          item(record, path, nil, [%{message: "Internal server error", path: path}])
        end)

      {%{incremental: items, hasNext: false}, bp}
  end

  defp item(record, path, data, errors) do
    %{data: data, path: path}
    |> put_unless(:label, record.label, nil)
    |> put_unless(:errors, errors, [])
  end

  defp put_unless(map, _key, empty, empty), do: map
  defp put_unless(map, key, value, _empty), do: Map.put(map, key, value)

  defp get_at(value, []), do: value
  defp get_at(%{} = map, [key | rest]) when is_binary(key), do: get_at(Map.get(map, key), rest)

  defp get_at(list, [index | rest]) when is_list(list) and is_integer(index),
    do: get_at(Enum.at(list, index), rest)

  defp get_at(_, _), do: nil

  defp merge_at(value, [], data), do: deep_merge(value, data)

  defp merge_at(%{} = map, [key | rest], data),
    do: Map.update!(map, key, &merge_at(&1, rest, data))

  defp merge_at(list, [index | rest], data) when is_list(list),
    do: List.update_at(list, index, &merge_at(&1, rest, data))

  defp deep_merge(%{} = left, %{} = right),
    do: Map.merge(left, right, fn _key, l, r -> deep_merge(l, r) end)

  defp deep_merge(left, right)
       when is_list(left) and is_list(right) and length(left) == length(right),
       do: Enum.zip_with(left, right, &deep_merge/2)

  defp deep_merge(_left, right), do: right
end
