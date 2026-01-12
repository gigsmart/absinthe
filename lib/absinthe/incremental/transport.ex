defmodule Absinthe.Incremental.Transport do
  @moduledoc """
  Protocol for incremental delivery across different transports.

  This module provides a behaviour and common functionality for implementing
  incremental delivery over various transport mechanisms (HTTP/SSE, WebSocket, etc.).
  """

  alias Absinthe.Blueprint
  alias Absinthe.Incremental.Response

  @type conn_or_socket :: Plug.Conn.t() | Phoenix.Socket.t() | any()
  @type state :: any()
  @type response :: map()

  @doc """
  Initialize the transport for incremental delivery.
  """
  @callback init(conn_or_socket, options :: Keyword.t()) :: {:ok, state} | {:error, term()}

  @doc """
  Send the initial response containing immediately available data.
  """
  @callback send_initial(state, response) :: {:ok, state} | {:error, term()}

  @doc """
  Send an incremental response containing deferred or streamed data.
  """
  @callback send_incremental(state, response) :: {:ok, state} | {:error, term()}

  @doc """
  Complete the incremental delivery stream.
  """
  @callback complete(state) :: :ok | {:error, term()}

  @doc """
  Handle errors during incremental delivery.
  """
  @callback handle_error(state, error :: term()) :: {:ok, state} | {:error, term()}

  @optional_callbacks [handle_error: 2]

  @default_timeout 30_000

  defmacro __using__(_opts) do
    quote do
      @behaviour Absinthe.Incremental.Transport

      alias Absinthe.Incremental.{Response, ErrorHandler}

      @doc """
      Handle a streaming response from the resolution phase.

      This is the main entry point for transport implementations.
      """
      def handle_streaming_response(conn_or_socket, blueprint, options \\ []) do
        timeout = Keyword.get(options, :timeout, unquote(@default_timeout))

        with {:ok, state} <- init(conn_or_socket, options),
             {:ok, state} <- send_initial_response(state, blueprint),
             {:ok, state} <- execute_and_stream_incremental(state, blueprint, timeout) do
          complete(state)
        else
          {:error, reason} = error ->
            handle_transport_error(conn_or_socket, error, options)
        end
      end

      defp send_initial_response(state, blueprint) do
        initial = Response.build_initial(blueprint)
        send_initial(state, initial)
      end

      # Execute deferred/streamed tasks and deliver results as they complete
      defp execute_and_stream_incremental(state, blueprint, timeout) do
        streaming_context = get_streaming_context(blueprint)

        all_tasks =
          Map.get(streaming_context, :deferred_tasks, []) ++
          Map.get(streaming_context, :stream_tasks, [])

        if Enum.empty?(all_tasks) do
          {:ok, state}
        else
          execute_tasks_with_streaming(state, all_tasks, timeout)
        end
      end

      # Execute tasks using Task.async_stream for controlled concurrency
      defp execute_tasks_with_streaming(state, tasks, timeout) do
        task_count = length(tasks)

        # Use Task.async_stream for backpressure and proper supervision
        results =
          tasks
          |> Task.async_stream(
            fn task ->
              # Wrap execution with error handling
              wrapped_fn = ErrorHandler.wrap_streaming_task(task.execute)
              {task, wrapped_fn.()}
            end,
            timeout: timeout,
            on_timeout: :kill_task,
            max_concurrency: System.schedulers_online() * 2
          )
          |> Enum.with_index()
          |> Enum.reduce_while({:ok, state}, fn
            {{:ok, {task, result}}, index}, {:ok, acc_state} ->
              has_next = index < task_count - 1

              case send_task_result(acc_state, task, result, has_next) do
                {:ok, new_state} -> {:cont, {:ok, new_state}}
                {:error, _} = error -> {:halt, error}
              end

            {{:exit, :timeout}, _index}, {:ok, acc_state} ->
              # Handle timeout - send error response and continue
              error_response = Response.build_error(
                [%{message: "Operation timed out"}],
                [],
                nil,
                false
              )
              case send_incremental(acc_state, error_response) do
                {:ok, new_state} -> {:cont, {:ok, new_state}}
                error -> {:halt, error}
              end

            {{:exit, reason}, _index}, {:ok, acc_state} ->
              # Handle other exits
              error_response = Response.build_error(
                [%{message: "Operation failed: #{inspect(reason)}"}],
                [],
                nil,
                false
              )
              case send_incremental(acc_state, error_response) do
                {:ok, new_state} -> {:cont, {:ok, new_state}}
                error -> {:halt, error}
              end
          end)

        results
      end

      # Send the result of a single task
      defp send_task_result(state, task, result, has_next) do
        response = build_task_response(task, result, has_next)
        send_incremental(state, response)
      end

      # Build the appropriate response based on task type and result
      defp build_task_response(task, {:ok, result}, has_next) do
        case task.type do
          :defer ->
            Response.build_incremental(
              result.data,
              result.path,
              result.label,
              has_next
            )

          :stream ->
            Response.build_stream_incremental(
              result.items,
              result.path,
              result.label,
              has_next
            )
        end
      end

      defp build_task_response(task, {:error, error}, has_next) do
        errors = case error do
          %{message: _} = err -> [err]
          message when is_binary(message) -> [%{message: message}]
          other -> [%{message: inspect(other)}]
        end

        Response.build_error(
          errors,
          task.path,
          task.label,
          has_next
        )
      end

      defp get_streaming_context(blueprint) do
        get_in(blueprint.execution.context, [:__streaming__]) || %{}
      end

      defp handle_transport_error(conn_or_socket, error, options) do
        if function_exported?(__MODULE__, :handle_error, 2) do
          with {:ok, state} <- init(conn_or_socket, options) do
            apply(__MODULE__, :handle_error, [state, error])
          end
        else
          error
        end
      end

      defoverridable [handle_streaming_response: 3]
    end
  end

  @doc """
  Check if a blueprint has incremental delivery enabled.
  """
  @spec incremental_delivery_enabled?(Blueprint.t()) :: boolean()
  def incremental_delivery_enabled?(blueprint) do
    get_in(blueprint.execution, [:incremental_delivery]) == true
  end

  @doc """
  Get the operation ID for tracking incremental delivery.
  """
  @spec get_operation_id(Blueprint.t()) :: String.t() | nil
  def get_operation_id(blueprint) do
    get_in(blueprint.execution.context, [:__streaming__, :operation_id])
  end

  @doc """
  Get streaming context from a blueprint.
  """
  @spec get_streaming_context(Blueprint.t()) :: map()
  def get_streaming_context(blueprint) do
    get_in(blueprint.execution.context, [:__streaming__]) || %{}
  end

  @doc """
  Execute incremental delivery for a blueprint.

  This is the main entry point that transport implementations call.
  """
  @spec execute(module(), conn_or_socket, Blueprint.t(), Keyword.t()) ::
    {:ok, state} | {:error, term()}
  def execute(transport_module, conn_or_socket, blueprint, options \\ []) do
    if incremental_delivery_enabled?(blueprint) do
      transport_module.handle_streaming_response(conn_or_socket, blueprint, options)
    else
      {:error, :incremental_delivery_not_enabled}
    end
  end

  @doc """
  Create a simple collector that accumulates all incremental responses.

  Useful for testing and non-streaming contexts.
  """
  @spec collect_all(Blueprint.t(), Keyword.t()) :: {:ok, map()} | {:error, term()}
  def collect_all(blueprint, options \\ []) do
    timeout = Keyword.get(options, :timeout, @default_timeout)
    streaming_context = get_streaming_context(blueprint)

    initial = Response.build_initial(blueprint)

    all_tasks =
      Map.get(streaming_context, :deferred_tasks, []) ++
      Map.get(streaming_context, :stream_tasks, [])

    incremental_results =
      all_tasks
      |> Task.async_stream(
        fn task -> {task, task.execute.()} end,
        timeout: timeout,
        on_timeout: :kill_task
      )
      |> Enum.map(fn
        {:ok, {task, {:ok, result}}} ->
          %{
            type: task.type,
            label: task.label,
            path: task.path,
            data: Map.get(result, :data),
            items: Map.get(result, :items),
            errors: Map.get(result, :errors)
          }

        {:ok, {task, {:error, error}}} ->
          %{
            type: task.type,
            label: task.label,
            path: task.path,
            errors: [error]
          }

        {:exit, reason} ->
          %{errors: [%{message: "Task failed: #{inspect(reason)}"}]}
      end)

    {:ok, %{
      initial: initial,
      incremental: incremental_results,
      hasNext: false
    }}
  end
end
