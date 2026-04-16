from flask import Flask

from route_registry import PIPELINE_ROUTES
from utils.threadpool_manager import get_thread_pool

app = Flask(__name__)


def create_pipeline_route(route_config):
    """
    Factory function that creates a route handler for a pipeline.
    Uses thread pool to manage concurrent task execution.

    Args:
        route_config: Dict with keys: path, function, description, args_names

    Returns:
        A route handler function
    """
    def handler(*args, **kwargs):
        try:
            # Get thread pool and submit task
            thread_pool = get_thread_pool()
            pipeline_args = args if args else tuple(kwargs.values())

            # Submit task to thread pool instead of spawning raw thread
            future = thread_pool.submit_task(route_config["function"], *pipeline_args)

            # Don't wait for completion - return immediately (fire-and-forget)
            # Future will be executed asynchronously in thread pool
        except Exception as e:
            return f"Erro ao executar o pipeline: {e}"

        return f"Pipeline {route_config['description']} executado com sucesso!"

    return handler


# Register all routes from the registry
for route_config in PIPELINE_ROUTES:
    handler = create_pipeline_route(route_config)
    app.add_url_rule(
        route_config["path"],
        endpoint=route_config["path"],
        view_func=handler,
        methods=["GET"]
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)

