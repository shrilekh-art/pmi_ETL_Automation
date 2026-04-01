from framework.logger import get_logger
import traceback

logger = get_logger()


def handle_error(error, context=""):
    error_message = f"Error occurred in {context}: {str(error)}"
    stack_trace = traceback.format_exc()

    logger.error(error_message)
    logger.error(stack_trace)

    return {
        "status": "FAIL",
        "context": context,
        "error": str(error)
    }