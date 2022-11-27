import lakefs_client
from lakefs_client.client import LakeFSClient
from email_validator import validate_email, EmailNotValidError


from .config import PlaygroundDetails
from .fs import register_fs

PLAYGROUND_CONTROL_PLANE_URL = "https://demo.lakefs.io/api/v1/notebook"

WELCOME_BANNER = """
     ██╗      █████╗ ██╗  ██╗███████╗███████╗███████╗
     ██║     ██╔══██╗██║ ██╔╝██╔════╝██╔════╝██╔════╝
     ██║     ███████║█████╔╝ █████╗  █████╗  ███████╗
     ██║     ██╔══██║██╔═██╗ ██╔══╝  ██╔══╝  ╚════██║
     ███████╗██║  ██║██║  ██╗███████╗██║     ███████║
     ╚══════╝╚═╝  ╚═╝╚═╝  ╚═╝╚══════╝╚═╝     ╚══════╝

│
│ 🎉 Your lakeFS playground is ready and will be available for 24h! 🎉
| 
| ⚠️ Do keep in mind that this environment is for learning purposes only
|
│ 💡 Check out the lakeFS UI for your playground environment at:
|        URL: https://{host}/
|        Access Key ID: {key}
|        Secret Access Key: {secret}
| 
│ 📖 Documentation and resources are available at https://docs.lakefs.io/ 
│ 👩‍💻 For support or any other question, join the lakeFS Slack channel at https://docs.lakefs.io/slack
|
"""


class LakeFSPlaygroundError(RuntimeError):
    pass


def check_email(email_addr: str) -> bool:
    """
    Make sure that the given email address is valid
    :param email_addr: email address
    :return: True if a valid email address was given, False otherwise
    """
    try:
        validate_email(email_addr)
    except EmailNotValidError:
        return False
    return True


def get_or_create(email: str, silent: bool = False) -> PlaygroundDetails:
    """
    Create a new ephemeral lakeFS Playground environment, or return an existing one
    for the specified email, if exists
    :param email: The email used to create the environment
    :param silent: if False, will print a friendly banner
    :return: a PlaygroundDetails object that contains information about the environment
    """
    import requests
    import base64
    import json

    resp = requests.post(PLAYGROUND_CONTROL_PLANE_URL, params={"email": email})
    if resp.status_code != 200:
        raise LakeFSPlaygroundError(f"HTTP {resp.status_code}: {resp.text}")

    # Decode Message
    message_bytes = base64.b64decode(resp.content)
    message = json.loads(message_bytes.decode("ascii"))

    details = PlaygroundDetails(
        access_key_id=message["LakeFSCreds"]["AccessKeyID"],
        secret_access_key=message["LakeFSCreds"]["SecretAccessKey"],
        endpoint_url=message["Host"],
    )
    if not silent:
        print(WELCOME_BANNER.format(
            host=details.endpoint_url, key=details.access_key_id, secret=details.secret_access_key))
    return details


def mount(details: PlaygroundDetails):
    """
    Register a `lakefs://` URI handler with fsspec (used by pandas and other common data tools)
    :param details: PlaygroundDetails object with information about the lakeFS installation
        (as returned from get_or_create)
    """
    register_fs(details=details)


def client(details: PlaygroundDetails) -> LakeFSClient:
    """
    Get an API client configured from the details provided
    :param details: PlaygroundDetails object with information about the lakeFS installation
        (as returned from get_or_create)
    :return: a lakefs_client.ApiClient configured to use the provided details
    """
    conf = lakefs_client.Configuration(
        host=f"https://{details.endpoint_url}/api/v1",
        username=details.access_key_id,
        password=details.secret_access_key
    )
    return LakeFSClient(conf)
