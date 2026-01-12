defmodule Absinthe.Schema.Prototype.Notation do
  @moduledoc false

  defmacro __using__(opts \\ []) do
    content(opts)
  end

  def content(_opts \\ []) do
    quote do
      use Absinthe.Schema
      @schema_provider Absinthe.Schema.Compiled
      @pipeline_modifier __MODULE__

      directive :deprecated do
        description "Marks an element of a GraphQL schema as no longer supported."
        arg :reason, :string

        repeatable false

        on [
          :field_definition,
          :input_field_definition,
          # Technically, argument deprecation is not yet defined by the GraphQL
          # specification. Absinthe does provide some support, but deprecating
          # arguments is not recommended.
          #
          # For more information, see:
          # - https://github.com/graphql/graphql-spec/pull/525
          # - https://github.com/absinthe-graphql/absinthe/issues/207
          :argument_definition,
          :enum_value
        ]

        expand &__MODULE__.expand_deprecate/2
      end

      directive :specified_by do
        description "Exposes a URL that specifies the behavior of this scalar."

        repeatable false

        arg :url, non_null(:string),
          description: "The URL that specifies the behavior of this scalar."

        on [:scalar]
      end

      # https://spec.graphql.org/September2025/#sec--oneOf
      directive :one_of do
        description """
        The @oneOf built-in directive is used within the type system definition language to indicate an Input Object is a OneOf Input Object.

        A OneOf Input Object is a special variant of Input Object where exactly one field must be set and non-null, all others being omitted.
        This is useful for representing situations where an input may be one of many different options.
        """

        repeatable false
        on [:input_object]

        expand(fn _args, node ->
          %{node | __private__: Keyword.put(node.__private__, :one_of, true)}
        end)
      end

      # https://github.com/graphql/nullability-wg
      directive :semantic_non_null do
        description """
        Indicates that a field is semantically non-null: the resolver never intentionally returns null,
        but null may still be returned due to errors.

        This decouples nullability from error handling, allowing clients to understand which fields
        may be null only due to errors versus fields that may intentionally be null.
        """

        arg :levels, non_null(list_of(non_null(:integer))),
          default_value: [0],
          description: """
          Specifies which levels of the return type are semantically non-null.
          - [0] means the field itself is semantically non-null
          - [1] for list fields means the list items are semantically non-null
          - [0, 1] means both the field and its items are semantically non-null
          """

        repeatable false
        on [:field_definition]

        expand(fn args, node ->
          levels = Map.get(args, :levels, [0])
          %{node | __private__: Keyword.put(node.__private__, :semantic_non_null, levels)}
        end)
      end

      def pipeline(pipeline) do
        pipeline
        |> Absinthe.Pipeline.without(Absinthe.Phase.Schema.Validation.QueryTypeMustBeObject)
        |> Absinthe.Pipeline.without(Absinthe.Phase.Schema.ImportPrototypeDirectives)
        |> Absinthe.Pipeline.without(Absinthe.Phase.Schema.DirectiveImports)
        |> Absinthe.Pipeline.replace(
          Absinthe.Phase.Schema.TypeExtensionImports,
          {Absinthe.Phase.Schema.TypeExtensionImports, []}
        )
      end

      @doc """
      Add a deprecation (with an optional reason) to a node.
      """
      @spec expand_deprecate(
              arguments :: %{optional(:reason) => String.t()},
              node :: Absinthe.Blueprint.node_t()
            ) :: Absinthe.Blueprint.node_t()
      def expand_deprecate(arguments, node) do
        %{node | deprecation: %Absinthe.Type.Deprecation{reason: arguments[:reason]}}
      end

      defoverridable(pipeline: 1)
    end
  end
end
