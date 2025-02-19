import io
import json
import datetime
import pytz

import string
import random

from openai import OpenAI
from openai import AssistantEventHandler

from typing_extensions import override
from azure.cosmos import CosmosClient, exceptions

client = OpenAI(api_key="")

COSMOS_DB_ENDPOINT = ""
COSMOS_DB_KEY = ""
DATABASE_NAME = ""

system_prompt = """You are a database expert who can query from the Microsoft Azure Cosmos DB Core (NoSQL).
MAKE SURE YOU FOLLOW Microsoft Azure Cosmos DB NOSQL SYNTAX.

The "patients" document lists all the patients and their information.
The schema of patients looks like this:
{
id: str # patient id - internal use
practiceId: str # type of practice
Name: str # patient name
PhoneNumber: str # patient's phone number
_rid: str
_self: str
_etag: str
_attachments: str
_ts: int
}
sample rows:
{'id': 'ad600eab-8781-4685-8206-ff387930b999', 'practiceId': 'id1', 'Name': 'John Doe', 'PhoneNumber': '+1-123-456-3344', '_rid': 'hWUuAO0UkT8QAAAAAAAAAA==', '_self': 'dbs/hWUuAA==/colls/hWUuAO0UkT8=/docs/hWUuAO0UkT8QAAAAAAAAAA==/', '_etag': '"0100f84a-0000-0300-0000-673fef270000"', '_attachments': 'attachments/', '_ts': 1732243239}
{'id': 'a9a80888-2c2b-4506-82a8-8a9df2f5c888', 'practiceId': 'id1', 'Name': 'Jane Doe', 'PhoneNumber': '+1-123-456-1122', '_rid': 'hWUuAO0UkT8RAAAAAAAAAA==', '_self': 'dbs/hWUuAA==/colls/hWUuAO0UkT8=/docs/hWUuAO0UkT8RAAAAAAAAAA==/', '_etag': '"0100f94a-0000-0300-0000-673fef270000"', '_attachments': 'attachments/', '_ts': 1732243239}

The "appointments" document lists all the appointments a specific patient had made.
The schema of appointments looks like this:
{
id: str # appointment id
Time: str # appointment date and time
practiceId: str # type of practice
PatientName: str # name of patient
PatientId: str # patient id - internal use
PatientStatus: str # status of the appointment - (pending, cancelled, confirmed)
Column: str # op code
Provider: str # name of provider
_rid: str
_self: str
_etag: str
_attachments: str
_ts: int
}

sample rows:
{'id': 'aafde70f-5126-421a-9595-0be34b79ce05', 'Time': '2024-09-26T12:49:47', 'practiceId': 'id3', 'PatientName': 'John Doe', 'PatientId': 'ad600eab-8781-4685-8206-ff387930b999', 'PatientStatus': 'cancelled', 'Column': 'OP2', 'Provider': 'DEN4', '_rid': 'hWUuAIE0Bh8QAAAAAAAAAA==', '_self': 'dbs/hWUuAA==/colls/hWUuAIE0Bh8=/docs/hWUuAIE0Bh8QAAAAAAAAAA==/', '_etag': '"0000dcfe-0000-0300-0000-673fef330000"', '_attachments': 'attachments/', '_ts': 1732243251}
{'id': 'd5575e9d-483e-4f50-b8cb-d4c7442536f1', 'Time': '2024-05-26T04:45:00', 'practiceId': 'id2', 'PatientName': 'Jane John', 'PatientId': '7ac51468-57b4-44f2-91fc-572179119e51', 'PatientStatus': 'pending', 'Column': 'OP3', 'Provider': 'DEN1', '_rid': 'hWUuAIE0Bh8RAAAAAAAAAA==', '_self': 'dbs/hWUuAA==/colls/hWUuAIE0Bh8=/docs/hWUuAIE0Bh8RAAAAAAAAAA==/', '_etag': '"0000ddfe-0000-0300-0000-673fef330000"', '_attachments': 'attachments/', '_ts': 1732243251}

Relationship between patients and appointments in one to many. One patient can have multiple appointments but one appointment can only have one patient. A patient can have no appointments minimum while an appointment should have one and only one patient.

Your job is to take a user query in plain english, break it down to query the database, double check your work and present the information to plain english.
Make sure customer experience is very important here and medical staff will be reading your output. Do not use technical information to present your answer.
Respond in plain english language.

1- Patient's name is important for outputs while id is an internal thing.
2- You might have to query database multiple times.
3- Calculate date_time_now = datetime.now() and use it to filter appointments.


STEPS TO FOLLOW:

Step 1: Break down the user request, look at the sample schema and analyze.
Step 2: Prepare a plan of action to answer user request.
Step 3: Determine and calculate date_time_now if necessary.
Step 4: Assess whether information is required from one or multiple documents.
Step 5: Follow Azure Cosmos DB NOSQL SYNTAX and prepare optimized queries. Apply filters or conditions at the query level if needed.
Step 6: Execute the query using the query_database tool.
Step 7: If an error occurs, read the error, resolve it, and re-run the query_database tool.
Step 8: The query_database tool returns a query_result_file (json file) containing the results. 
Step 9: Read the json file with the code interpreter tool and read the result of query made.
Step 10: Use the code interpreter to perform additional computations if needed.
Step 11: Aggregate and calculate the final answer if multiple queries were executed.
Step 12: Format the response to be human-readable and display the output.
"""

tool_functions = [
    {
        "type": "function",
        "function": {
            "name": "query_database",
            "description": "Queries a specified Cosmos DB container using the provided query string in Azure cosmos Core Nosql format and container name. It returns the matching items as array, or an error message if the query fails.",
            "strict": False,
            "parameters": {
                "type": "object",
                "properties": {
                    "query_string": {
                        "type": "string",
                        "description": "Azure Cosmos Core (Nosql) query string to execute on the container."
                    },
                    "container_name": {
                        "type": "string",
                        "description": "The name of the container to query within the Cosmos DB. ['patients', 'appointments']"
                    }
                },
                "required": [
                    "query_string",
                    "container_name"
                ]
            }
        }
    },

    {
        "type": "function",
        "function": {
            "name": "calculate_date_time_now",
            "description": "Calculates the current date and time based on the provided timezone.",
            "strict": True,
            "parameters": {
                "type": "object",
                "required": [
                    "timezone"
                ],
                "properties": {
                    "timezone": {
                        "type": "string",
                        "description": "The timezone for which the current date and time should be calculated."
                    }
                },
                "additionalProperties": False
            }
        }
    }
]

file_ids = []

file_to_delete = []

curr_thread_id = None


def create_file_on_storage(data):
    random_string = ''.join(random.choices(string.ascii_letters + string.digits, k=5))

    json_data = json.dumps(data, indent=4)
    file_like = io.BytesIO(json_data.encode("utf-8"))
    file_like.name = f"temp-{random_string}.json"  # Set a name for the file

    # Upload the file to OpenAI storage
    try:
        response = client.files.create(
            file=file_like,
            purpose="assistants"
        )
        # print(response)
        return response.id
    except Exception as e:
        print("Error uploading file:", e)


def query_database(query_string, container_name):
    # Initialize the Cosmos client
    cosmos_client = CosmosClient(COSMOS_DB_ENDPOINT, COSMOS_DB_KEY)

    # Get the database client
    database = cosmos_client.get_database_client(DATABASE_NAME)

    # Get the container client for the 'patients' container
    container = database.get_container_client(container_name)

    # Define a sample query. Here we select all items.
    # Note: The alias 'c' is used to represent items in the container.

    try:
        # Query items from the container. enable_cross_partition_query is set to True if needed.
        response = list(container.query_items(
            query=query_string,
            enable_cross_partition_query=True
        ))

        file_id = create_file_on_storage(response)
        print(response)
        file_ids.append(file_id)
        file_to_delete.append(file_id)

        client.beta.threads.update(
            curr_thread_id,
            tool_resources={
                "code_interpreter": {
                    "file_ids": file_ids[-20:],  # Truncate as max number of files in 20 for now
                }
            }
        )

        return {"query_result_file": file_id}

    except exceptions.CosmosHttpResponseError as e:
        return {"error": f"An error occurred: {e.message}"}


def calculate_date_time_now(timezone):
    """
    Calculates and returns the current date and time in ISO format
    for the provided timezone.
    """
    try:
        tz = pytz.timezone(timezone)
    except Exception:
        return f"Error: Invalid timezone '{timezone}'"
    now = datetime.datetime.now(tz)
    return now.isoformat()


class EventHandler(AssistantEventHandler):
    @override
    def on_text_created(self, text) -> None:
        print(f"\nassistant > ", end="", flush=True)

    @override
    def on_text_delta(self, delta, snapshot):
        print(delta.value, end="", flush=True)

    def on_tool_call_created(self, tool_call):
        print(f"\nassistant : {tool_call.type}\n", flush=True)

    def on_tool_call_delta(self, delta, snapshot):
        if delta.type == 'code_interpreter':
            if delta.code_interpreter.input:
                print(delta.code_interpreter.input, end="", flush=True)
            if delta.code_interpreter.outputs:
                print(f"\n\noutput >", flush=True)
                for output in delta.code_interpreter.outputs:
                    if output.type == "logs":
                        print(f"\n{output.logs}", flush=True)

    @override
    def on_event(self, event):
        # Retrieve events that are denoted with 'requires_action'
        if event.event == 'thread.run.requires_action':
            run_id = event.data.id  # Retrieve the run ID from the event data
            self.handle_requires_action(event.data, run_id)

    def handle_requires_action(self, data, run_id):
        tool_outputs = []

        for tool in data.required_action.submit_tool_outputs.tool_calls:
            # Use 'arguments' instead of 'parameters'
            try:
                args = json.loads(tool.function.arguments) if isinstance(tool.function.arguments,
                                                                         str) else tool.function.arguments
            except Exception:
                args = {}

            print(tool.function.name, args)
            if tool.function.name == "query_database":
                query_string = args.get("query_string")
                container_name = args.get("container_name")
                result = query_database(query_string, container_name)
                tool_outputs.append({"tool_call_id": tool.id, "output": json.dumps(result)})
                print(result)
            elif tool.function.name == "calculate_date_time_now":
                timezone = args.get("timezone")
                result = calculate_date_time_now(timezone)
                tool_outputs.append({"tool_call_id": tool.id, "output": json.dumps(result)})
                print(result)

        self.submit_tool_outputs(tool_outputs, run_id)

    def submit_tool_outputs(self, tool_outputs, run_id):
        # Use the submit_tool_outputs_stream helper to send the outputs back
        with client.beta.threads.runs.submit_tool_outputs_stream(
                thread_id=self.current_run.thread_id,
                run_id=self.current_run.id,
                tool_outputs=tool_outputs,
                event_handler=EventHandler(),
        ) as stream:
            for text in stream.text_deltas:
                # print(text, end="", flush=True)
                pass


def run_assistant_thread(assistant_id, thread_id, query):
    client.beta.threads.messages.create(
        thread_id=thread_id,
        role="user",
        content=query,
    )

    with client.beta.threads.runs.stream(
            thread_id=thread_id,
            assistant_id=assistant_id,
            event_handler=EventHandler(),
    ) as stream:
        stream.until_done()


def create_assistant():
    assistant = client.beta.assistants.create(
        name="Query",
        instructions=system_prompt,
        tools=[{"type": "code_interpreter"}] + tool_functions,
        model="gpt-4o",
        temperature=0.30,
        top_p=0.70,
    )
    print(assistant)
    return assistant.id


def create_judge_assistant():
    judge_system_prompt = """You are an expert Software Tester. You have access to a Microsoft Azure Cosmos DB Core (NoSQL).
MAKE SURE YOU FOLLOW Microsoft Azure Cosmos DB NOSQL SYNTAX.

You need to test a function that returns answers to the queries.

You will generate a query based on the sample schema.

You will use code_interpreter to calculate the answer and compare the answer from the fuction.

So you will be acting like a tester who generates query, calculates the result and compare it with the result of the function.


The "patients" document lists all the patients and their information.
The schema of patients looks like this:
{}
id: str # patient id - internal use
practiceId: str # type of practice
Name: str # patient name
PhoneNumber: str # patient's phone number
_rid: str
_self: str
_etag: str
_attachments: str
_ts: int
}
sample rows:
{'id': 'ad600eab-8781-4685-8206-ff387930b999', 'practiceId': 'id1', 'Name': 'John Doe', 'PhoneNumber': '+1-123-456-3344', '_rid': 'hWUuAO0UkT8QAAAAAAAAAA==', '_self': 'dbs/hWUuAA==/colls/hWUuAO0UkT8=/docs/hWUuAO0UkT8QAAAAAAAAAA==/', '_etag': '"0100f84a-0000-0300-0000-673fef270000"', '_attachments': 'attachments/', '_ts': 1732243239}
{'id': 'a9a80888-2c2b-4506-82a8-8a9df2f5c888', 'practiceId': 'id1', 'Name': 'Jane Doe', 'PhoneNumber': '+1-123-456-1122', '_rid': 'hWUuAO0UkT8RAAAAAAAAAA==', '_self': 'dbs/hWUuAA==/colls/hWUuAO0UkT8=/docs/hWUuAO0UkT8RAAAAAAAAAA==/', '_etag': '"0100f94a-0000-0300-0000-673fef270000"', '_attachments': 'attachments/', '_ts': 1732243239}

The "appointments" document lists all the appointments a specific patient had made.
The schema of appointments looks like this:
{
id: str # appointment id
Time: str # appointment date and time
practiceId: str # type of practice
PatientName: str # name of patient
PatientId: str # patient id - internal use
PatientStatus: str # status of the appointment - (pending, cancelled, confirmed)
Column: str # op code
Provider: str # name of provider
_rid: str
_self: str
_etag: str
_attachments: str
_ts: int
}

sample rows:
{'id': 'aafde70f-5126-421a-9595-0be34b79ce05', 'Time': '2024-09-26T12:49:47', 'practiceId': 'id3', 'PatientName': 'John Doe', 'PatientId': 'ad600eab-8781-4685-8206-ff387930b999', 'PatientStatus': 'cancelled', 'Column': 'OP2', 'Provider': 'DEN4', '_rid': 'hWUuAIE0Bh8QAAAAAAAAAA==', '_self': 'dbs/hWUuAA==/colls/hWUuAIE0Bh8=/docs/hWUuAIE0Bh8QAAAAAAAAAA==/', '_etag': '"0000dcfe-0000-0300-0000-673fef330000"', '_attachments': 'attachments/', '_ts': 1732243251}
{'id': 'd5575e9d-483e-4f50-b8cb-d4c7442536f1', 'Time': '2024-05-26T04:45:00', 'practiceId': 'id2', 'PatientName': 'Jane John', 'PatientId': '7ac51468-57b4-44f2-91fc-572179119e51', 'PatientStatus': 'pending', 'Column': 'OP3', 'Provider': 'DEN1', '_rid': 'hWUuAIE0Bh8RAAAAAAAAAA==', '_self': 'dbs/hWUuAA==/colls/hWUuAIE0Bh8=/docs/hWUuAIE0Bh8RAAAAAAAAAA==/', '_etag': '"0000ddfe-0000-0300-0000-673fef330000"', '_attachments': 'attachments/', '_ts': 1732243251}

Relationship between patients and appointments in one to many. One patient can have multiple appointments but one appointment can only have one patient. A patient can have no appointments minimum while an appointment should have one and only one patient.

STEPS TO FOLLOW:

Step 1: Generate 5 queries in context to the give schema. for example: how many patients have appointments next week.

Step 2: Use code_interpreter to evaluate the query on the given data set.

Step 3: Pass the same query to the function. Make sure it is Azure Cosmos DB NOSQL SYNTAX.

Step 4: Compare results and report the correctness of the function.
"""

    assistant = client.beta.assistants.create(
        name="Judge",
        instructions=judge_system_prompt,
        tools=[{"type": "code_interpreter"}] + tool_functions,
        model="gpt-4o",
    )
    print(assistant)


if __name__ == "__main__":
    # pip install pytz openai azure-cosmos

    # Could have spent more time, organizing and refining the code but
    # as per the instructions focused on demonstrating the thought process.

    # Also could have added conditional statements (Case 1) or loops (Repeat Step 1) in system prompt.
    # For example, if response is short, return response directly instead of writing to file.
    # And modifying steps by making Cases or sending result type as a part of response.

    assistant_id = create_assistant()
    # assistant_id = ""  # Judge
    # assistant_id = ""  # Query

    thread = client.beta.threads.create()
    curr_thread_id = thread.id

    query = "Which patients have an appointment today?"
    print("How can I help you?")
    while (True):
        query = input()

        if query == "exit()":
            break

        run_assistant_thread(assistant_id, thread.id, query)

        runs = client.beta.threads.runs.list(
            curr_thread_id
        )
        print()
        print(runs.data[0].usage)
        print()

    # Delete the files
    # TODO: Put this code in a destructor method.
    for ftd in file_to_delete:
        client.files.delete(ftd)

"""
Price per million input = $2.5
Price per million output = $10

Avg token per query input = 8000
Avg token per query input = 300

Cost per query = 0.02 + 0.003 = 0.023 
Almost 2 cents per query

Ignoring cost of code_interpreter invoke
"""
