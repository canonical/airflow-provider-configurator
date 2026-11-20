"""Provider Configurator security-event logging using owasp-logger."""

import json
import logging
from datetime import datetime, timezone
from typing import Any

import ops
from owasp_logger import OWASPLogger

import constants

_OWASP_EVENT_KEY = "owasp_event"
UNATTRIBUTED_ACTOR = "unattributed"


class _ContextLogger(logging.LoggerAdapter):
    """Add Canonical and provider-configurator context to OWASP events."""

    def process(self, message: str, kwargs: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        extra = dict(kwargs.get("extra") or {})
        event = extra.get(_OWASP_EVENT_KEY)
        if event is not None:
            event = dict(event)
            event.update(self.extra)
            event["datetime"] = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
            if event.get("level") == "WARNING":
                event["level"] = "WARN"
            extra[_OWASP_EVENT_KEY] = event
            kwargs["extra"] = extra
            message = json.dumps({_OWASP_EVENT_KEY: event}, separators=(",", ":"))
        return message, kwargs


class ProviderSecurityLogger:
    """Emit provider-configurator administrative and git-sync lifecycle events."""

    def __init__(self, container: ops.model.Container, target: str) -> None:
        self._container = container
        self._context_logger = _ContextLogger(
            logging.getLogger(__name__),
            {
                "appid": constants.APP_ID,
                "target": target,
                "actor": UNATTRIBUTED_ACTOR,
                "data": {},
            },
        )
        self._owasp_logger = OWASPLogger(appid=constants.APP_ID, logger=self._context_logger)

    def _git_sync_service(self) -> ops.pebble.ServiceInfo | None:
        """Return git-sync's Pebble service info; raise if Pebble cannot be queried."""
        return self._container.get_services(constants.GIT_SYNC_SERVICE).get(
            constants.GIT_SYNC_SERVICE
        )

    def is_running(self) -> bool | None:
        """Return git-sync state, or None if Pebble cannot be queried."""
        try:
            service = self._git_sync_service()
        except (ops.pebble.APIError, ops.pebble.ConnectionError):
            return None
        return bool(service and service.is_running())

    def log_startup_if_transitioned(self, was_running: bool | None) -> None:
        """Log git-sync startup only when a stopped service becomes running."""
        if was_running is False and self.is_running() is True:
            self._emit(
                "sys_startup",
                ("pebble",),
                "pebble",
                "git-sync started by Pebble after charm reconciliation.",
                trigger="pebble_replan",
            )

    def log_shutdown_if_running(self, was_running: bool | None, trigger: str) -> None:
        """Log a successful charm-requested stop of a running git-sync service."""
        if was_running:
            self._emit(
                "sys_shutdown",
                ("juju-charm",),
                "juju-charm",
                "git-sync stopped by the charm.",
                trigger=trigger,
            )

    def log_restart(self, trigger: str) -> None:
        """Log a restart after the charm's Pebble restart call succeeds."""
        self._emit(
            "sys_restart",
            ("juju-charm",),
            "juju-charm",
            "git-sync restarted after its service plan changed.",
            trigger=trigger,
        )

    def log_crash_if_error(self) -> None:
        """Log a crash only when Pebble reports git-sync in ERROR state."""
        try:
            service = self._git_sync_service()
        except (ops.pebble.APIError, ops.pebble.ConnectionError):
            return
        if service and service.current == ops.pebble.ServiceStatus.ERROR:
            self._emit(
                "sys_crash",
                ("PebbleServiceError",),
                "pebble",
                "Pebble reports git-sync in an error state.",
                status=service.current.value,
            )

    def log_configuration_change(self, action: str, data: dict[str, Any]) -> None:
        """Log an administrative action on provider configuration."""
        self._emit(
            "authz_admin",
            (UNATTRIBUTED_ACTOR, action),
            UNATTRIBUTED_ACTOR,
            f"Provider configuration {action.replace('_', ' ')}.",
            **data,
        )

    def log_denied_key(self, key: str) -> None:
        """Log rejection of a provider configuration key by the denylist.

        A key committed to the git repository is invalid input to a policy check,
        not an access attempt, so this is an input-validation failure (WARN) rather
        than authz_fail (CRITICAL).
        """
        self._emit(
            "input_validation_discrete_fail",
            (key, UNATTRIBUTED_ACTOR),
            UNATTRIBUTED_ACTOR,
            "Provider configuration key rejected by the denylist.",
            config_key=key,
            actor_attribution="repository_audit_log",
        )

    def _emit(
        self,
        event_method: str,
        event_parameters: tuple[str, ...],
        actor: str,
        description: str,
        **data: Any,
    ) -> None:
        self._set_context(actor, data)
        try:
            getattr(self._owasp_logger, event_method)(
                *event_parameters,
                description=description,
            )
        finally:
            self._clear_context()

    def _set_context(self, actor: str, data: dict[str, Any]) -> None:
        self._context_logger.extra.update(
            {
                "actor": actor,
                "data": data,
            }
        )

    def _clear_context(self) -> None:
        self._context_logger.extra.update({"actor": UNATTRIBUTED_ACTOR, "data": {}})
