defmodule Absinthe.Phase.Document.Execution.Resolution do
  @moduledoc false

  # Runs resolution functions in a blueprint.
  #
  # Blueprint results are placed under `blueprint.result.execution`. This is
  # because the results form basically a new tree from the original blueprint.

  alias Absinthe.{Blueprint, Type, Phase}
  alias Blueprint.{Result, Execution}
  alias Blueprint.Execution.DeferredFragment
  alias Absinthe.Resolution.Projector

  alias Absinthe.Phase
  use Absinthe.Phase

  @spec run(Blueprint.t(), Keyword.t()) :: Phase.result_t()
  def run(bp_root, options \\ []) do
    case Blueprint.current_operation(bp_root) do
      nil -> {:ok, bp_root}
      op -> resolve_current(bp_root, op, options)
    end
  end

  defp resolve_current(bp_root, operation, options) do
    execution = perform_resolution(bp_root, operation, options)

    blueprint = %{bp_root | execution: execution}

    if Keyword.get(options, :plugin_callbacks, true) do
      bp_root.schema.plugins()
      |> Absinthe.Plugin.pipeline(execution)
      |> case do
        [] ->
          {:ok, blueprint}

        pipeline ->
          {:insert, blueprint, pipeline}
      end
    else
      {:ok, blueprint}
    end
  end

  defp perform_resolution(bp_root, operation, options) do
    exec = Execution.get(bp_root, operation)

    exec =
      if Keyword.get(options, :incremental, false) do
        %{exec | acc: Map.put_new(exec.acc, DeferredFragment, [])}
      else
        exec
      end

    plugins = bp_root.schema.plugins()
    run_callbacks? = Keyword.get(options, :plugin_callbacks, true)

    exec = plugins |> run_callbacks(:before_resolution, exec, run_callbacks?)

    res = build_root_resolution(exec)

    {result, res} =
      exec.result
      |> walk_result(operation, operation.schema_node, res, [operation])
      |> propagate_null_trimming

    exec = update_persisted_fields(exec, res)

    exec = plugins |> run_callbacks(:after_resolution, exec, run_callbacks?)

    %{exec | result: result}
  end

  @doc false
  # Resolves one wave of deferred fragments with a shared resolution struct, so a
  # single dataloader batch covers every record. Fragments deferred inside them are
  # left in `execution.acc` for the next wave. Results are returned in record order.
  @spec resolve_deferred(Blueprint.t(), [DeferredFragment.t()]) ::
          {Blueprint.t(), [Result.Object.t() | Result.Leaf.t()]}
  def resolve_deferred(bp_root, records) do
    plugins = bp_root.schema.plugins()

    slots =
      Enum.map(records, fn record ->
        {%Result.Object{root_value: record.source, emitter: record.emitter}, %{}}
      end)

    {exec, slots} = resolve_deferred_round(bp_root.execution, records, slots, plugins)

    {%{bp_root | execution: exec}, Enum.map(slots, &elem(&1, 0))}
  end

  defp resolve_deferred_round(exec, records, slots, plugins) do
    exec = run_callbacks(plugins, :before_resolution, exec, true)

    {slots, res} =
      records
      |> Enum.zip(slots)
      |> Enum.map_reduce(build_root_resolution(exec), fn {record, {result, cache}}, res ->
        {result, res} =
          result
          |> walk_result(
            record.emitter,
            record.parent_type,
            %{res | fields_cache: cache},
            record.path
          )
          |> propagate_null_trimming()

        {{result, res.fields_cache}, res}
      end)

    exec = %{update_persisted_fields(exec, res) | fields_cache: exec.fields_cache}
    exec = run_callbacks(plugins, :after_resolution, exec, true)

    case Absinthe.Plugin.pipeline(plugins, exec) do
      [] -> {exec, slots}
      _ -> resolve_deferred_round(exec, records, slots, plugins)
    end
  end

  defp build_root_resolution(exec) do
    common =
      Map.take(exec, [:adapter, :context, :acc, :root_value, :schema, :fragments, :fields_cache])

    %Absinthe.Resolution{
      path: nil,
      source: nil,
      parent_type: nil,
      middleware: nil,
      definition: nil,
      arguments: nil
    }
    |> Map.merge(common)
  end

  defp run_callbacks(plugins, callback, acc, true) do
    Enum.reduce(plugins, acc, &apply(&1, callback, [&2]))
  end

  defp run_callbacks(_, _, acc, _), do: acc

  @doc """
  This function walks through any existing results. If no results are found at a
  given node, it will call the requisite function to expand and build those results
  """
  def walk_result(%{fields: nil} = result, bp_node, _schema_type, res, path) do
    {fields, res} = resolve_fields(bp_node, res, result.root_value, path)
    {%{result | fields: fields}, res}
  end

  def walk_result(%{fields: fields} = result, bp_node, schema_type, res, path) do
    {fields, res} = walk_results(fields, bp_node, schema_type, res, [0 | path], [])

    {%{result | fields: fields}, res}
  end

  def walk_result(%Result.Leaf{} = result, _, _, res, _) do
    {result, res}
  end

  def walk_result(%{values: values} = result, bp_node, schema_type, res, path) do
    {values, res} = walk_results(values, bp_node, schema_type, res, [0 | path], [])
    {%{result | values: values}, res}
  end

  def walk_result(%Absinthe.Resolution{} = old_res, _bp_node, _schema_type, res, _path) do
    res = update_persisted_fields(old_res, res)
    do_resolve_field(res, res.source, res.path)
  end

  # walk list results
  defp walk_results([value | values], bp_node, inner_type, res, [i | sub_path] = path, acc) do
    {result, res} = walk_result(value, bp_node, inner_type, %{res | path: path}, path)
    walk_results(values, bp_node, inner_type, res, [i + 1 | sub_path], [result | acc])
  end

  defp walk_results([], _, _, res = %{path: [_ | sub_path]}, _, acc),
    do: {:lists.reverse(acc), %{res | path: sub_path}}

  defp walk_results([], _, _, res, _, acc), do: {:lists.reverse(acc), res}

  defp resolve_fields(parent, res, source, path) do
    # parent is the parent field, we need to get the return type of that field
    # that return type could be an interface or union, so let's make it concrete
    parent
    |> get_return_type
    |> get_concrete_type(source, res)
    |> case do
      nil ->
        {[], res}

      parent_type ->
        {fields, res} = project_fields(parent, parent_type, source, path, res)

        {values, res} = do_resolve_fields(fields, res, source, parent_type, path, [])
        {values, %{res | path: path}}
    end
  end

  defp project_fields(parent, parent_type, source, path, res) do
    if defer_enabled?(parent, res) do
      {fields, deferred, fields_cache} =
        Projector.project_with_deferred(
          parent.selections,
          parent_type,
          path,
          res.fields_cache,
          res
        )

      records =
        Enum.reduce(deferred, Map.fetch!(res.acc, DeferredFragment), fn fragment, records ->
          {defer, flags} = Map.pop(fragment.flags, :defer)

          record = %DeferredFragment{
            label: Map.get(defer, :label),
            emitter: %{parent | selections: [%{fragment | flags: flags}]},
            parent_type: parent_type,
            source: source,
            path: path
          }

          [record | records]
        end)

      acc = Map.put(res.acc, DeferredFragment, records)
      {fields, %{res | fields_cache: fields_cache, acc: acc}}
    else
      {fields, fields_cache} =
        Projector.project(parent.selections, parent_type, path, res.fields_cache, res)

      {fields, %{res | fields_cache: fields_cache}}
    end
  end

  # Root fields of mutations and subscriptions run inline; defers below them still apply.
  defp defer_enabled?(%Blueprint.Document.Operation{type: type}, _res) when type != :query,
    do: false

  defp defer_enabled?(_parent, %{acc: acc}), do: Map.has_key?(acc, DeferredFragment)

  defp get_return_type(%{schema_node: %Type.Field{type: type}}) do
    Type.unwrap(type)
  end

  defp get_return_type(%{schema_node: schema_node}) do
    Type.unwrap(schema_node)
  end

  defp get_return_type(type), do: type

  defp get_concrete_type(%Type.Union{} = parent_type, source, res) do
    Type.Union.resolve_type(parent_type, source, res)
  end

  defp get_concrete_type(%Type.Interface{} = parent_type, source, res) do
    Type.Interface.resolve_type(parent_type, source, res)
  end

  defp get_concrete_type(parent_type, _source, _res) do
    parent_type
  end

  defp do_resolve_fields([field | fields], res, source, parent_type, path, acc) do
    field = %{field | parent_type: parent_type}
    {result, res} = resolve_field(field, res, source, parent_type, [field | path])
    do_resolve_fields(fields, res, source, parent_type, path, [result | acc])
  end

  defp do_resolve_fields([], res, _, _, _, acc), do: {:lists.reverse(acc), res}

  def resolve_field(field, res, source, parent_type, path) do
    res
    |> build_resolution_struct(field, source, parent_type, path)
    |> do_resolve_field(source, path)
  end

  # bp_field needs to have a concrete schema node, AKA no unions or interfaces
  defp do_resolve_field(res, source, path) do
    res
    |> reduce_resolution
    |> case do
      %{state: :resolved} = res ->
        build_result(res, source, path)

      %{state: :suspended} = res ->
        {res, res}

      final_res ->
        raise """
        Should have halted or suspended middleware
        Started with: #{inspect(res)}
        Ended with: #{inspect(final_res)}
        """
    end
  end

  defp update_persisted_fields(dest, %{acc: acc, context: context, fields_cache: cache}) do
    %{dest | acc: acc, context: context, fields_cache: cache}
  end

  defp build_resolution_struct(
         res,
         %{argument_data: args, schema_node: %{middleware: middleware}} = bp_field,
         source,
         parent_type,
         path
       ) do
    %{
      res
      | path: path,
        state: :unresolved,
        value: nil,
        errors: [],
        source: source,
        parent_type: parent_type,
        middleware: middleware,
        definition: bp_field,
        arguments: args
    }
  end

  defp reduce_resolution(%{middleware: []} = res), do: res

  defp reduce_resolution(%{middleware: [middleware | remaining_middleware]} = res) do
    case call_middleware(middleware, %{res | middleware: remaining_middleware}) do
      %{state: :suspended} = res ->
        res

      res ->
        reduce_resolution(res)
    end
  end

  defp call_middleware({{mod, fun}, opts}, res) do
    apply(mod, fun, [res, opts])
  end

  defp call_middleware({mod, opts}, res) do
    apply(mod, :call, [res, opts])
  end

  defp call_middleware(mod, res) when is_atom(mod) do
    apply(mod, :call, [res, []])
  end

  defp call_middleware(fun, res) when is_function(fun, 2) do
    fun.(res, [])
  end

  defp build_result(res, source, path) do
    %{
      value: value,
      definition: bp_field,
      extensions: extensions,
      schema: schema,
      errors: errors
    } = res

    full_type = Type.expand(bp_field.schema_node.type, schema)

    bp_field = put_in(bp_field.schema_node.type, full_type)

    # if there are any errors, the value is always nil
    value =
      case errors do
        [] -> value
        _ -> nil
      end

    errors = maybe_add_non_null_error(errors, value, full_type)

    value
    |> to_result(bp_field, full_type, extensions)
    |> add_errors(Enum.reverse(errors), &put_result_error_value(&1, &2, bp_field, source, path))
    |> walk_result(bp_field, full_type, res, path)
    |> propagate_null_trimming
  end

  defp maybe_add_non_null_error(errors, value, type, path \\ [])

  defp maybe_add_non_null_error([], nil, %Type.NonNull{}, []) do
    ["Cannot return null for non-nullable field"]
  end

  defp maybe_add_non_null_error([], nil, %Type.NonNull{}, path) do
    [%{message: "Cannot return null for non-nullable field", path: Enum.reverse(path)}]
  end

  defp maybe_add_non_null_error([], value, %Type.NonNull{of_type: %Type.List{} = type}, path) do
    maybe_add_non_null_error([], value, type, path)
  end

  defp maybe_add_non_null_error([], [_ | _] = values, %Type.List{of_type: type}, path) do
    values
    |> Enum.with_index()
    |> Enum.flat_map(fn {value, index} ->
      maybe_add_non_null_error([], value, type, [index | path])
    end)
  end

  defp maybe_add_non_null_error(errors, _, _, _path) do
    errors
  end

  defp propagate_null_trimming({%{values: values} = node, res}) do
    values = Enum.map(values, &do_propagate_null_trimming/1)
    node = %{node | values: values}
    {do_propagate_null_trimming(node), res}
  end

  defp propagate_null_trimming({node, res}) do
    {do_propagate_null_trimming(node), res}
  end

  defp do_propagate_null_trimming(node) do
    if bad_child = find_bad_child(node) do
      bp_field = node.emitter

      full_type =
        with %{type: type} <- bp_field.schema_node do
          type
        end

      nil
      |> to_result(bp_field, full_type, node.extensions)
      |> Map.put(:errors, node.errors ++ bad_child.errors)
    else
      node
    end
  end

  defp find_bad_child(%{fields: fields}) do
    Enum.find(fields, &non_null_violation?/1)
  end

  defp find_bad_child(%{values: values}) do
    Enum.find(values, &non_null_list_violation?/1)
  end

  defp find_bad_child(_) do
    false
  end

  # FIXME: Not super happy with this lookup process
  defp non_null_violation?(%{value: nil, emitter: %{schema_node: %{type: %Type.NonNull{}}}}) do
    true
  end

  defp non_null_violation?(_) do
    false
  end

  defp non_null_list_violation?(%{values: values}) do
    Enum.find(values, &non_null_list_violation?/1)
  end

  # FIXME: Not super happy with this lookup process.
  # Also it would be nice if we could use the same function as above.
  defp non_null_list_violation?(%{value: nil, emitter: %{schema_node: %{type: type}}}) do
    !null_allowed_in_list?(type)
  end

  defp non_null_list_violation?(_) do
    false
  end

  defp null_allowed_in_list?(%Type.List{of_type: wrapped_type}) do
    null_allowed_in_list?(wrapped_type)
  end

  defp null_allowed_in_list?(%Type.NonNull{of_type: %Type.List{of_type: wrapped_type}}) do
    null_allowed_in_list?(wrapped_type)
  end

  defp null_allowed_in_list?(%Type.NonNull{of_type: _wrapped_type}) do
    false
  end

  defp null_allowed_in_list?(_type) do
    true
  end

  defp add_errors(result, errors, fun) do
    Enum.reduce(errors, result, fun)
  end

  defp put_result_error_value(error_value, result, bp_field, source, path) do
    case split_error_value(error_value) do
      {[], _} ->
        raise Absinthe.Resolution.result_error(error_value, bp_field, source)

      {[message: message, path: error_path], extra} ->
        put_error(
          result,
          error(bp_field, message, Enum.reverse(error_path) ++ path, Map.new(extra))
        )

      {[message: message], extra} ->
        put_error(result, error(bp_field, message, path, Map.new(extra)))
    end
  end

  defp split_error_value(error_value)
       when is_list(error_value) or (is_map(error_value) and not is_struct(error_value)) do
    Keyword.split(Enum.to_list(error_value), [:message, :path])
  end

  defp split_error_value(error_value) when is_binary(error_value) do
    {[message: error_value], []}
  end

  defp split_error_value(error_value) do
    {[message: to_string(error_value)], []}
  end

  defp to_result(nil, blueprint, _, extensions) do
    %Result.Leaf{emitter: blueprint, value: nil, extensions: extensions}
  end

  defp to_result(root_value, blueprint, %Type.NonNull{of_type: inner_type}, extensions) do
    to_result(root_value, blueprint, inner_type, extensions)
  end

  defp to_result(root_value, blueprint, %Type.Object{}, extensions) do
    %Result.Object{root_value: root_value, emitter: blueprint, extensions: extensions}
  end

  defp to_result(root_value, blueprint, %Type.Interface{}, extensions) do
    %Result.Object{root_value: root_value, emitter: blueprint, extensions: extensions}
  end

  defp to_result(root_value, blueprint, %Type.Union{}, extensions) do
    %Result.Object{root_value: root_value, emitter: blueprint, extensions: extensions}
  end

  defp to_result(root_value, blueprint, %Type.List{of_type: inner_type}, extensions) do
    values =
      root_value
      |> List.wrap()
      |> Enum.map(&to_result(&1, blueprint, inner_type, extensions))

    %Result.List{values: values, emitter: blueprint, extensions: extensions}
  end

  defp to_result(root_value, blueprint, %Type.Scalar{}, extensions) do
    %Result.Leaf{
      emitter: blueprint,
      value: root_value,
      extensions: extensions
    }
  end

  defp to_result(root_value, blueprint, %Type.Enum{}, extensions) do
    %Result.Leaf{
      emitter: blueprint,
      value: root_value,
      extensions: extensions
    }
  end

  def error(node, message, path, extra) do
    %Phase.Error{
      phase: __MODULE__,
      message: message,
      locations: [node.source_location],
      path: Absinthe.Resolution.path(%{path: path}),
      extra: extra
    }
  end
end
