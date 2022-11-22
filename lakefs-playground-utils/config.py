
class PlaygroundDetails:
    access_key_id: str
    secret_access_key: str
    endpoint_url: str 

    def __init__(self, access_key_id: str, secret_access_key: str, endpoint_url: str) -> None:
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.endpoint_url = endpoint_url
