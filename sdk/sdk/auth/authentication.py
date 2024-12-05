from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from keycloak import KeycloakOpenID
from vyper import v

from sdk.auth.exceptions import FailedToGetTokenError


@dataclass
class AuthenticationABC(ABC):
    @abstractmethod
    async def get_token(self) -> str:
        pass


@dataclass
class Authentication(AuthenticationABC):
    auth_server_url: str = field(init=False)
    auth_server_client_id: str = field(init=False)
    auth_server_client_secret: str = field(init=False)
    auth_server_realm_name: str = field(init=False)
    grant_type: str = field(init=False)
    username: str = field(init=False)
    password: str = field(init=False)
    scope: str = field(init=False)

    def __post_init__(self) -> None:
        self.auth_server_url = v.get_string("auth.endpoint")
        self.auth_server_client_id = v.get_string("auth.client")
        self.auth_server_client_secret = v.get_string("auth.client_secret")
        self.auth_server_realm_name = v.get_string("auth.realm")
        self.grant_type = "password"
        self.scope = "openid"
        self.username = v.get_string("minio.client_user")
        self.password = v.get_string("minio.client_password")

    def get_token(self) -> dict:
        try:
            keycloak_openid = KeycloakOpenID(
                server_url=self.auth_server_url,
                client_id=self.auth_server_client_id,
                client_secret_key=self.auth_server_client_secret,
                realm_name=self.auth_server_realm_name,
            )

            access_token = keycloak_openid.token(
                grant_type=self.grant_type,
                username=self.username,
                password=self.password,
                scope=self.scope,
            )

            return access_token
        except Exception as e:
            raise FailedToGetTokenError(e)
