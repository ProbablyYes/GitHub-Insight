from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional


@dataclass
class GithubEvent:
    event_id: str
    event_type: str
    created_at: datetime
    actor_login: str
    actor_category: str
    repo_id: int
    repo_name: str
    public: bool

    @classmethod
    def from_raw_event(cls, raw: Dict[str, Any]) -> "GithubEvent":
        created_at = datetime.fromisoformat(
            raw["created_at"].replace("Z", "+00:00")
        ).astimezone(timezone.utc)
        actor = raw.get("actor") or {}
        repo = raw.get("repo") or {}
        actor_login = actor.get("login") or "unknown"
        actor_category = "bot" if "bot" in actor_login.lower() else "human"
        return cls(
            event_id=str(raw.get("id", "")),
            event_type=str(raw.get("type", "UnknownEvent")),
            created_at=created_at,
            actor_login=actor_login,
            actor_category=actor_category,
            repo_id=int(repo.get("id", 0) or 0),
            repo_name=str(repo.get("name", "unknown/unknown")),
            public=bool(raw.get("public", True)),
        )

    def to_record(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "created_at": self.created_at.isoformat(),
            "actor_login": self.actor_login,
            "actor_category": self.actor_category,
            "repo_id": self.repo_id,
            "repo_name": self.repo_name,
            "public": self.public,
        }


def safe_get_language_guess(repo_name: Optional[str]) -> str:
    if not repo_name:
        return "unknown"
    name = repo_name.lower()
    if "spark" in name or "scala" in name:
        return "Scala"
    if "python" in name or "pandas" in name:
        return "Python"
    if "java" in name or "spring" in name:
        return "Java"
    if "react" in name or "js" in name:
        return "JavaScript"
    if "go" in name or "golang" in name:
        return "Go"
    return "unknown"
