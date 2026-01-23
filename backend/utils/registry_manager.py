"""
Script Registry Manager with Version Control
=============================================

Manages the script registry with:
- Version control for individual scripts
- Change history tracking
- Rollback capabilities
- Audit logging
- Script validation

Author: AI Agent Platform
Version: 4.0.0
"""

import os
import json
import hashlib
import shutil
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, asdict, field
from datetime import datetime
from pathlib import Path
import structlog

logger = structlog.get_logger()


@dataclass
class ScriptVersion:
    """Represents a version of a script"""
    version: str
    hash: str
    created_at: str
    created_by: str
    changes: str
    script_data: Dict[str, Any]


@dataclass
class ScriptHistory:
    """Version history for a script"""
    script_id: str
    current_version: str
    versions: List[ScriptVersion] = field(default_factory=list)
    created_at: str = ""
    updated_at: str = ""


class RegistryManager:
    """
    Registry Manager with version control for scripts.

    Features:
    - Script versioning with semantic versioning
    - Change tracking and audit logs
    - Rollback to previous versions
    - Backup and restore
    - Validation before updates
    """

    def __init__(
        self,
        registry_path: str = "/home/samrattidke600/ai_agent_app/registry.json",
        history_dir: str = "/home/samrattidke600/ai_agent_app/backend/data/registry_history"
    ):
        self.registry_path = registry_path
        self.history_dir = Path(history_dir)
        self.history_dir.mkdir(parents=True, exist_ok=True)

        self._registry: Optional[Dict] = None
        self._histories: Dict[str, ScriptHistory] = {}

        self._load_histories()
        logger.info("registry_manager_initialized", registry=registry_path)

    @property
    def registry(self) -> Dict:
        """Get current registry (lazy load)"""
        if self._registry is None:
            self._registry = self._load_registry()
        return self._registry

    def _load_registry(self) -> Dict:
        """Load registry from file"""
        try:
            with open(self.registry_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error("registry_load_failed", error=str(e))
            return {"scripts": [], "version": "1.0.0"}

    def _save_registry(self):
        """Save registry to file"""
        try:
            # Backup first
            backup_path = f"{self.registry_path}.backup"
            if os.path.exists(self.registry_path):
                shutil.copy(self.registry_path, backup_path)

            # Update version
            self._registry["last_updated"] = datetime.utcnow().strftime("%Y-%m-%d")

            with open(self.registry_path, 'w') as f:
                json.dump(self._registry, f, indent=2)

            logger.info("registry_saved")
        except Exception as e:
            logger.error("registry_save_failed", error=str(e))
            raise

    def _load_histories(self):
        """Load version histories from disk"""
        try:
            history_file = self.history_dir / "script_histories.json"
            if history_file.exists():
                with open(history_file, 'r') as f:
                    data = json.load(f)
                    for script_id, history_data in data.items():
                        versions = [
                            ScriptVersion(**v) for v in history_data.get("versions", [])
                        ]
                        self._histories[script_id] = ScriptHistory(
                            script_id=script_id,
                            current_version=history_data.get("current_version", "1.0.0"),
                            versions=versions,
                            created_at=history_data.get("created_at", ""),
                            updated_at=history_data.get("updated_at", "")
                        )
        except Exception as e:
            logger.warning("history_load_failed", error=str(e))

    def _save_histories(self):
        """Save version histories to disk"""
        try:
            history_file = self.history_dir / "script_histories.json"
            data = {}
            for script_id, history in self._histories.items():
                data[script_id] = {
                    "script_id": history.script_id,
                    "current_version": history.current_version,
                    "versions": [asdict(v) for v in history.versions],
                    "created_at": history.created_at,
                    "updated_at": history.updated_at
                }
            with open(history_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error("history_save_failed", error=str(e))

    def _compute_hash(self, script_data: Dict) -> str:
        """Compute hash of script data for change detection"""
        # Exclude volatile fields
        stable_data = {k: v for k, v in script_data.items()
                      if k not in ['version', 'last_modified']}
        content = json.dumps(stable_data, sort_keys=True)
        return hashlib.sha256(content.encode()).hexdigest()[:12]

    def _increment_version(self, current: str, change_type: str = "patch") -> str:
        """Increment semantic version"""
        try:
            parts = current.split('.')
            major, minor, patch = int(parts[0]), int(parts[1]), int(parts[2])

            if change_type == "major":
                major += 1
                minor = 0
                patch = 0
            elif change_type == "minor":
                minor += 1
                patch = 0
            else:  # patch
                patch += 1

            return f"{major}.{minor}.{patch}"
        except:
            return "1.0.1"

    # ==========================================================================
    # Script Operations
    # ==========================================================================

    def get_script(self, script_id: str) -> Optional[Dict]:
        """Get a script by ID"""
        for script in self.registry.get("scripts", []):
            if script.get("id") == script_id:
                return script
        return None

    def get_script_version(self, script_id: str) -> str:
        """Get current version of a script"""
        if script_id in self._histories:
            return self._histories[script_id].current_version
        return "1.0.0"

    def get_script_history(self, script_id: str) -> Optional[ScriptHistory]:
        """Get version history for a script"""
        return self._histories.get(script_id)

    def add_script(
        self,
        script_data: Dict,
        author: str = "system",
        comment: str = "Initial version"
    ) -> Dict:
        """
        Add a new script to the registry.

        Args:
            script_data: Script definition
            author: Who is adding the script
            comment: Description of the addition

        Returns:
            Added script with version info
        """
        script_id = script_data.get("id")
        if not script_id:
            raise ValueError("Script must have an 'id' field")

        # Check if already exists
        if self.get_script(script_id):
            raise ValueError(f"Script {script_id} already exists. Use update_script instead.")

        # Validate script
        self._validate_script(script_data)

        # Add version info
        now = datetime.utcnow().isoformat()
        script_data["version"] = "1.0.0"
        script_data["created_at"] = now
        script_data["last_modified"] = now

        # Add to registry
        self.registry["scripts"].append(script_data)
        self._save_registry()

        # Create version history
        script_hash = self._compute_hash(script_data)
        history = ScriptHistory(
            script_id=script_id,
            current_version="1.0.0",
            versions=[
                ScriptVersion(
                    version="1.0.0",
                    hash=script_hash,
                    created_at=now,
                    created_by=author,
                    changes=comment,
                    script_data=script_data.copy()
                )
            ],
            created_at=now,
            updated_at=now
        )
        self._histories[script_id] = history
        self._save_histories()

        logger.info(
            "script_added",
            script_id=script_id,
            version="1.0.0",
            author=author
        )

        return script_data

    def update_script(
        self,
        script_id: str,
        updates: Dict,
        author: str = "system",
        comment: str = "Updated script",
        change_type: str = "patch"
    ) -> Dict:
        """
        Update an existing script.

        Args:
            script_id: Script to update
            updates: Fields to update
            author: Who is making the update
            comment: Description of changes
            change_type: 'major', 'minor', or 'patch'

        Returns:
            Updated script with new version
        """
        script = self.get_script(script_id)
        if not script:
            raise ValueError(f"Script {script_id} not found")

        # Get current version
        current_version = self.get_script_version(script_id)
        new_version = self._increment_version(current_version, change_type)

        # Apply updates
        old_script = script.copy()
        for key, value in updates.items():
            if key != "id":  # Don't allow ID changes
                script[key] = value

        # Update metadata
        now = datetime.utcnow().isoformat()
        script["version"] = new_version
        script["last_modified"] = now

        # Validate
        self._validate_script(script)

        # Save registry
        self._save_registry()

        # Update history
        script_hash = self._compute_hash(script)
        if script_id not in self._histories:
            self._histories[script_id] = ScriptHistory(
                script_id=script_id,
                current_version="1.0.0",
                versions=[],
                created_at=old_script.get("created_at", now),
                updated_at=now
            )

        history = self._histories[script_id]
        history.current_version = new_version
        history.updated_at = now
        history.versions.append(ScriptVersion(
            version=new_version,
            hash=script_hash,
            created_at=now,
            created_by=author,
            changes=comment,
            script_data=script.copy()
        ))

        # Keep only last 20 versions
        if len(history.versions) > 20:
            history.versions = history.versions[-20:]

        self._save_histories()

        logger.info(
            "script_updated",
            script_id=script_id,
            old_version=current_version,
            new_version=new_version,
            author=author,
            changes=comment
        )

        return script

    def rollback_script(
        self,
        script_id: str,
        target_version: str,
        author: str = "system"
    ) -> Dict:
        """
        Rollback a script to a previous version.

        Args:
            script_id: Script to rollback
            target_version: Version to rollback to
            author: Who is performing the rollback

        Returns:
            Rolled back script
        """
        history = self.get_script_history(script_id)
        if not history:
            raise ValueError(f"No history found for script {script_id}")

        # Find target version
        target = None
        for version in history.versions:
            if version.version == target_version:
                target = version
                break

        if not target:
            raise ValueError(f"Version {target_version} not found for script {script_id}")

        # Restore script data
        return self.update_script(
            script_id,
            target.script_data,
            author=author,
            comment=f"Rollback to version {target_version}",
            change_type="minor"
        )

    def delete_script(
        self,
        script_id: str,
        author: str = "system",
        reason: str = "Script removed"
    ) -> bool:
        """
        Delete a script from the registry.

        The script is removed but history is preserved for audit.

        Args:
            script_id: Script to delete
            author: Who is deleting
            reason: Why it's being deleted

        Returns:
            True if deleted
        """
        scripts = self.registry.get("scripts", [])
        original_count = len(scripts)

        self.registry["scripts"] = [s for s in scripts if s.get("id") != script_id]

        if len(self.registry["scripts"]) < original_count:
            self._save_registry()

            # Mark in history as deleted
            if script_id in self._histories:
                history = self._histories[script_id]
                history.current_version = "deleted"
                history.updated_at = datetime.utcnow().isoformat()
                self._save_histories()

            logger.info(
                "script_deleted",
                script_id=script_id,
                author=author,
                reason=reason
            )
            return True

        return False

    # ==========================================================================
    # Validation
    # ==========================================================================

    def _validate_script(self, script: Dict) -> bool:
        """Validate script structure"""
        required_fields = ["id", "name", "type", "description", "path", "workflow"]
        missing = [f for f in required_fields if f not in script]

        if missing:
            raise ValueError(f"Missing required fields: {', '.join(missing)}")

        # Validate risk level
        valid_risk_levels = ["low", "medium", "high", "critical"]
        if script.get("risk_level") not in valid_risk_levels:
            script["risk_level"] = "medium"

        # Validate type
        valid_types = ["ansible", "shell", "terraform", "kubernetes"]
        if script.get("type") not in valid_types:
            raise ValueError(f"Invalid script type: {script.get('type')}")

        return True

    # ==========================================================================
    # Queries
    # ==========================================================================

    def list_scripts(
        self,
        script_type: Optional[str] = None,
        risk_level: Optional[str] = None,
        service: Optional[str] = None
    ) -> List[Dict]:
        """List scripts with optional filters"""
        scripts = self.registry.get("scripts", [])

        if script_type:
            scripts = [s for s in scripts if s.get("type") == script_type]

        if risk_level:
            scripts = [s for s in scripts if s.get("risk_level") == risk_level]

        if service:
            scripts = [s for s in scripts if s.get("service") == service]

        # Add version info
        for script in scripts:
            script["_version"] = self.get_script_version(script["id"])

        return scripts

    def get_stats(self) -> Dict:
        """Get registry statistics"""
        scripts = self.registry.get("scripts", [])

        stats = {
            "total_scripts": len(scripts),
            "by_type": {},
            "by_risk_level": {},
            "by_service": {},
            "with_versions": len(self._histories),
            "registry_version": self.registry.get("version", "unknown"),
            "last_updated": self.registry.get("last_updated", "unknown")
        }

        for script in scripts:
            script_type = script.get("type", "unknown")
            stats["by_type"][script_type] = stats["by_type"].get(script_type, 0) + 1

            risk = script.get("risk_level", "unknown")
            stats["by_risk_level"][risk] = stats["by_risk_level"].get(risk, 0) + 1

            service = script.get("service", "unknown")
            stats["by_service"][service] = stats["by_service"].get(service, 0) + 1

        return stats

    def export_registry(self, include_history: bool = False) -> Dict:
        """Export registry with optional history"""
        export = {
            "registry": self.registry,
            "exported_at": datetime.utcnow().isoformat(),
            "version": "4.0.0"
        }

        if include_history:
            export["histories"] = {
                script_id: {
                    "current_version": h.current_version,
                    "versions": [asdict(v) for v in h.versions],
                    "created_at": h.created_at,
                    "updated_at": h.updated_at
                }
                for script_id, h in self._histories.items()
            }

        return export


# Global instance
registry_manager = RegistryManager()
