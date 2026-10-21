defmodule Absinthe.Resolution.Projector do
  @moduledoc false

  alias Absinthe.{Blueprint, Type}

  @doc """
  Project one layer down from where we are right now.

  Projection amounts to collecting the next set of fields to operate on, based on
  the current field. This is a non trivial operation because you have to handle
  the various type conditions that come along with fragments / inline fragments,
  field merging, and other wonderful stuff like that.
  """
  def project(selections, %{identifier: parent_ident} = parent_type, path, cache, exec) do
    key = cache_key(parent_ident, path)

    case Map.fetch(cache, key) do
      {:ok, fields} ->
        {fields, cache}

      _ ->
        {fields, []} = collect(selections, parent_type, exec, false)
        {fields, Map.put(cache, key, fields)}
    end
  end

  @doc """
  Like `project/5`, except that `@defer`red fragments whose type condition passes
  are returned separately, in document order, instead of being collected.
  """
  def project_with_deferred(
        selections,
        %{identifier: parent_ident} = parent_type,
        path,
        cache,
        exec
      ) do
    # Disjoint from `project/5` keys: look-ahead through `Absinthe.Resolution.project/2`
    # shares this cache and must keep seeing deferred fields.
    key = {:deferred, cache_key(parent_ident, path)}

    case Map.fetch(cache, key) do
      {:ok, {fields, deferred}} ->
        {fields, deferred, cache}

      _ ->
        {fields, deferred} = projection = collect(selections, parent_type, exec, true)
        {fields, deferred, Map.put(cache, key, projection)}
    end
  end

  defp cache_key(parent_ident, path) do
    path =
      for %{parent_type: %{identifier: i}, name: name, alias: alias} <- path do
        {i, alias || name}
      end

    [parent_ident | path]
  end

  defp response_key(%{alias: nil, name: name}), do: name
  defp response_key(%{alias: alias}), do: alias
  defp response_key(%{name: name}), do: name

  defp collect(selections, parent_type, %{fragments: fragments, schema: schema}, defer?) do
    ctx = %{fragments: fragments, parent_type: parent_type, schema: schema, defer?: defer?}
    {acc, _index, deferred} = do_collect(selections, ctx, {%{}, 0, []})
    {rectify_order(acc), Enum.reverse(deferred)}
  end

  defp do_collect([], _ctx, state), do: state

  defp do_collect([selection | selections], ctx, {acc, index, deferred} = state) do
    case selection do
      %{flags: %{skip: _}} ->
        do_collect(selections, ctx, state)

      %Blueprint.Document.Field{} = field ->
        field = update_schema_node(field, ctx.parent_type)
        key = response_key(field)

        acc =
          Map.update(acc, key, {index, [field]}, fn {existing_index, fields} ->
            {existing_index, [field | fields]}
          end)

        do_collect(selections, ctx, {acc, index + 1, deferred})

      %Blueprint.Document.Fragment.Inline{
        type_condition: %{schema_node: condition},
        selections: inner_selections
      } ->
        state = collect_fragment(selection, condition, inner_selections, ctx, state)
        do_collect(selections, ctx, state)

      %Blueprint.Document.Fragment.Spread{name: name} ->
        %{type_condition: condition, selections: inner_selections} =
          Map.fetch!(ctx.fragments, name)

        state = collect_fragment(selection, condition, inner_selections, ctx, state)
        do_collect(selections, ctx, state)
    end
  end

  defp collect_fragment(fragment, condition, selections, ctx, {acc, index, deferred} = state) do
    cond do
      not passes_type_condition?(normalize_condition(condition, ctx.schema), ctx.parent_type) ->
        state

      ctx.defer? and deferred?(fragment) ->
        {acc, index, [fragment | deferred]}

      true ->
        do_collect(selections, ctx, state)
    end
  end

  defp deferred?(%{flags: %{defer: %{enabled: true}}}), do: true
  defp deferred?(_), do: false

  defp rectify_order(grouped_fields) do
    grouped_fields
    |> Enum.sort(fn {_, {i1, _}}, {_, {i2, _}} ->
      i1 <= i2
    end)
    |> Enum.map(fn
      {_k, {_index, [field]}} ->
        field

      {_k, {_index, [%{selections: selections} = field | rest]}} ->
        %{field | selections: flatten(rest, selections)}
    end)
  end

  defp flatten([], acc), do: acc

  defp flatten([%{selections: selections} | fields], acc) do
    flatten(fields, selections ++ acc)
  end

  # necessary when the field in question is on an abstract type.
  defp update_schema_node(%{name: "__" <> _} = field, _) do
    field
  end

  defp update_schema_node(%{schema_node: %{identifier: identifier}} = field, %{
         fields: concrete_fields
       }) do
    %{field | schema_node: :maps.get(identifier, concrete_fields)}
  end

  defp normalize_condition(%{schema_node: condition}, schema) do
    normalize_condition(condition, schema)
  end

  defp normalize_condition(condition, schema) do
    case Type.unwrap(condition) do
      %{} = condition -> condition
      value -> Absinthe.Schema.lookup_type(schema, value)
    end
  end

  defp passes_type_condition?(%Type.Object{name: name}, %Type.Object{name: name}) do
    true
  end

  defp passes_type_condition?(%Type.Interface{} = condition, %Type.Object{} = type) do
    Type.Interface.member?(condition, type)
  end

  defp passes_type_condition?(%Type.Union{} = condition, %Type.Object{} = type) do
    Type.Union.member?(condition, type)
  end

  defp passes_type_condition?(_, _) do
    false
  end
end
