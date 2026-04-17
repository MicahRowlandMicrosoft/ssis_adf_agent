"""
Azure Function stub — auto-generated from SSIS Script Task: Script Task
Original language: CSharp
Entry point: Main

TODO: Implement the business logic below.  The function receives the SSIS
      variables listed under Args as JSON body fields and returns the
      read-write variables in the JSON response.

Args:


"""
import logging
import json
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Executing Script_Task")

    try:
        body = req.get_json()
    except ValueError:
        body = {}

    pass  # no variables declared

    # TODO: implement converted logic here
    raise NotImplementedError(
        "Script Task 'Script Task' has not been implemented yet. "
        "See the original CSharp code above."
    )

    return func.HttpResponse(
        json.dumps({}),
        mimetype="application/json",
        status_code=200,
    )
