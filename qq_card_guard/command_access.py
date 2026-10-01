"""Authorize responses before help, validation errors or service errors are sent."""

from .config import CommandPermissionError


class CommandAccess:
    def __init__(self):
        self.next_check = {}

    async def allowed(self, service, *, text, actor, platform, account, bot_admin):
        parts = text.strip().split(maxsplit=3)
        if parts and parts[0].lstrip("/") in ("名片规范", "qcard"):
            parts.pop(0)
        # Never enumerate all groups in response to an unscoped help request.
        if len(parts) < 2:
            return bot_admin
        try:
            policy = service.settings().group(parts[1])
        except Exception:
            return bot_admin
        now = service.clock()
        key = (platform, account, actor)
        if self.next_check.get(key, 0) > now:
            return False
        if len(self.next_check) >= 2000:
            self.next_check = {k: v for k, v in self.next_check.items() if v > now}
            if len(self.next_check) >= 2000 and key not in self.next_check:
                return False
        # Set before awaiting so concurrent commands cannot multiply lookups.
        self.next_check[key] = now + 5
        try:
            await service.authorize(policy, actor, platform, account)
        except CommandPermissionError:
            self.next_check[key] = service.clock() + 30
            return False
        except Exception:
            self.next_check[key] = service.clock() + 30
            return bot_admin  # Only known bot administrators may see diagnostics.
        return True
