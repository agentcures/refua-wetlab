from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class WetLabConfig:
    host: str = "127.0.0.1"
    port: int = 8788
    data_dir: Path = Path(".refua-wetlab")
    max_workers: int = 2
    auth_tokens: tuple[str, ...] = ()
    operator_tokens: tuple[str, ...] = ()
    admin_tokens: tuple[str, ...] = ()

    @property
    def database_path(self) -> Path:
        return self.data_dir / "runs.sqlite3"

    @property
    def auth_enabled(self) -> bool:
        return bool(self._all_tokens())

    def roles_for_token(self, token: str) -> frozenset[str]:
        normalized = token.strip()
        if not normalized:
            return frozenset()
        all_tokens = self._all_tokens()
        if normalized not in all_tokens:
            return frozenset()

        roles = {"viewer"}
        if normalized in set(self.operator_tokens):
            roles.add("operator")
        if normalized in set(self.admin_tokens):
            roles.add("admin")
            roles.add("operator")
        return frozenset(roles)

    def _all_tokens(self) -> frozenset[str]:
        tokens: set[str] = set()
        tokens.update(item.strip() for item in self.auth_tokens if item.strip())
        tokens.update(item.strip() for item in self.operator_tokens if item.strip())
        tokens.update(item.strip() for item in self.admin_tokens if item.strip())
        return frozenset(tokens)
