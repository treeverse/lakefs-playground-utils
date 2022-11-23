from .config import PlaygroundDetails
from .fs import register_fs

PLAYGROUND_CONTROL_PLANE_URL = "https://demo.lakefs.io/api/v1/notebook"


class LakeFSPlaygroundError(RuntimeError):
    pass


def get_or_create(email: str) -> PlaygroundDetails:
    import requests
    import base64
    import json

    resp = requests.post(PLAYGROUND_CONTROL_PLANE_URL, params={"email": email})
    if resp.status_code != 200:
        raise LakeFSPlaygroundError(f"HTTP {resp.status_code}: {resp.text}")

    # Decode Message
    message_bytes = base64.b64decode(resp.content)
    message = json.loads(message_bytes.decode("ascii"))

    return PlaygroundDetails(
        access_key_id=message["LakeFSCreds"]["AccessKeyID"],
        secret_access_key=message["LakeFSCreds"]["SecretAccessKey"],
        endpoint_url=message["Host"],
    )


def mount(details: PlaygroundDetails):
    register_fs(details=details)
