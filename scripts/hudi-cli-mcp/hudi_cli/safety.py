"""Safety manager for write operations — token-based confirmation protocol."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field

from hudi_cli.commands import RiskLevel

# Confirmation tokens expire after 5 minutes.
TOKEN_TTL_SECONDS = 300


class TokenExpiredError(Exception):
    """Raised when a confirmation token has expired."""

    pass


class TokenNotFoundError(Exception):
    """Raised when a confirmation token does not exist."""

    pass


@dataclass
class PendingOperation:
    """A write operation awaiting confirmation."""

    command: str
    risk_level: RiskLevel
    table_path: str
    description: str
    token: str = field(default_factory=lambda: str(uuid.uuid4()))
    dry_run_result: str | None = None
    created_at: float = field(default_factory=time.time)

    @property
    def expires_at(self) -> float:
        return self.created_at + TOKEN_TTL_SECONDS

    @property
    def is_expired(self) -> bool:
        return time.time() > self.expires_at

    @property
    def seconds_remaining(self) -> int:
        return max(0, int(self.expires_at - time.time()))

    def to_dict(self) -> dict:
        return {
            "token": self.token,
            "command": self.command,
            "risk_level": self.risk_level.value,
            "table_path": self.table_path,
            "description": self.description,
            "dry_run_result": self.dry_run_result,
            "expires_in_seconds": self.seconds_remaining,
        }


class SafetyManager:
    """Manages pending write operations and their confirmation tokens.

    In-memory only — tokens are discarded on server restart.
    """

    def __init__(self):
        self._pending: dict[str, PendingOperation] = {}

    def prepare_operation(
        self,
        command: str,
        risk_level: RiskLevel,
        table_path: str,
        description: str,
        dry_run_result: str | None = None,
    ) -> PendingOperation:
        """Create a pending operation and return it with a confirmation token."""
        self._cleanup_expired()
        op = PendingOperation(
            command=command,
            risk_level=risk_level,
            table_path=table_path,
            description=description,
            dry_run_result=dry_run_result,
        )
        self._pending[op.token] = op
        return op

    def confirm(self, token: str) -> PendingOperation:
        """Confirm a pending operation and return it for execution.

        The token is consumed — it cannot be used again.
        Raises TokenNotFoundError or TokenExpiredError.
        """
        self._cleanup_expired()
        op = self._pending.pop(token, None)
        if op is None:
            raise TokenNotFoundError(
                f"Token '{token}' not found. It may have expired or already been used."
            )
        if op.is_expired:
            raise TokenExpiredError(
                f"Token '{token}' has expired. Re-run the write operation to get a new token."
            )
        return op

    def cancel(self, token: str) -> PendingOperation:
        """Cancel a pending operation.

        Raises TokenNotFoundError if the token does not exist.
        """
        op = self._pending.pop(token, None)
        if op is None:
            raise TokenNotFoundError(
                f"Token '{token}' not found. It may have expired or already been used."
            )
        return op

    def list_pending(self) -> list[PendingOperation]:
        """Return all non-expired pending operations."""
        self._cleanup_expired()
        return list(self._pending.values())

    def _cleanup_expired(self) -> None:
        """Remove expired tokens."""
        expired = [t for t, op in self._pending.items() if op.is_expired]
        for t in expired:
            del self._pending[t]
