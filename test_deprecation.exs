defmodule TestDeprecationSchema do
  use Absinthe.Schema
  
  query do
    field :current_field, :string do
      resolve fn _, _ -> {:ok, "current"} end
    end
    
    field :old_field, :string do
      deprecate "Use currentField instead"
      resolve fn _, _ -> {:ok, "old"} end
    end
  end
end

# Run introspection
{:ok, result} = Absinthe.Schema.introspect(TestDeprecationSchema)

# Check if deprecated field is included
query_type = Enum.find(result.data["__schema"]["types"], &(&1["name"] == "RootQueryType"))
fields = query_type["fields"]

IO.puts("Total fields: #{length(fields)}")
for field <- fields do
  deprecated = if field["isDeprecated"], do: " (deprecated: #{field["deprecationReason"]})", else: ""
  IO.puts("  - #{field["name"]}#{deprecated}")
end

# Also test schema.json generation
{:ok, json} = Mix.Tasks.Absinthe.Schema.Json.generate_schema(%Mix.Tasks.Absinthe.Schema.Json.Options{
  schema: TestDeprecationSchema,
  json_codec: Jason,
  pretty: true
})

# Parse and check
decoded = Jason.decode!(json)
query_type_json = Enum.find(decoded["data"]["__schema"]["types"], &(&1["name"] == "RootQueryType"))
IO.puts("\nIn schema.json:")
IO.puts("Total fields: #{length(query_type_json["fields"])}")
for field <- query_type_json["fields"] do
  deprecated = if field["isDeprecated"], do: " (deprecated: #{field["deprecationReason"]})", else: ""
  IO.puts("  - #{field["name"]}#{deprecated}")
end
