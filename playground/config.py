class PlaygroundDetails:
    access_key_id: str
    secret_access_key: str
    endpoint_url: str

    def __init__(
        self, access_key_id: str, secret_access_key: str, endpoint_url: str
    ) -> None:
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.endpoint_url = endpoint_url

    def __repr__(self):
        return f"PlaygroundDetails(access_key_id={self.access_key_id}, secret_access_key={self.secret_access_key}, endpoint_url={self.endpoint_url})"
