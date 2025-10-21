from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping

try:
    import tomllib  # Python 3.11+
except ModuleNotFoundError:  # pragma: no cover - safeguard for <3.11
    import tomli as tomllib  # type: ignore[no-redef]


class ConfigError(Exception):
    """Raised when the user configuration is invalid."""


@dataclass(slots=True)
class BackloggdConfig:
    username: str
    public_only: bool = True
    collection: str = "games"
    host_override_ip: str | None = None


@dataclass(slots=True)
class HLTBConfig:
    rate_limit_per_sec: float = 0.75
    user_agent: str = "backlog-enricher/0.1 (+https://localhost)"
    max_retries: int = 5
    backoff_min_seconds: int = 2
    backoff_max_seconds: int = 60
    use_library: bool = True
    fallback_html: bool = True


@dataclass(slots=True)
class MatchConfig:
    fuzzy_auto: int = 95
    fuzzy_queue_min: int = 90
    year_tolerance: int = 1
    require_platform_overlap: bool = True


@dataclass(slots=True)
class PathsConfig:
    cache_dir: str = ".cache"
    db_path: str = "backlog.db"
    export_dir: str = "."


@dataclass(slots=True)
class ExportConfig:
    formats: list[str] = field(default_factory=lambda: ["csv"])


@dataclass(slots=True)
class LoggingConfig:
    level: str = "INFO"
    json: bool = True


@dataclass(slots=True)
class Config:
    backloggd: BackloggdConfig
    hltb: HLTBConfig = field(default_factory=HLTBConfig)
    match: MatchConfig = field(default_factory=MatchConfig)
    paths: PathsConfig = field(default_factory=PathsConfig)
    export: ExportConfig = field(default_factory=ExportConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    raw_path: Path | None = None

    def db_path(self) -> Path:
        return self.resolve_path(self.paths.db_path)

    def cache_path(self) -> Path:
        return self.resolve_path(self.paths.cache_dir)

    def export_path(self) -> Path:
        return self.resolve_path(self.paths.export_dir)

    def resolve_path(self, value: str) -> Path:
        base = self.raw_path.parent if self.raw_path else Path.cwd()
        return (base / value).expanduser().resolve()


def default_config_path() -> Path:
    return Path("config.toml")


def load_config(path: Path | None = None) -> Config:
    config_path = path or default_config_path()
    if not config_path.exists():
        raise ConfigError(f"Configuration file not found: {config_path}")

    data = _read_toml(config_path)
    cfg = _build_config(data, config_path)
    _validate_config(cfg)
    return cfg


def _read_toml(path: Path) -> Mapping[str, Any]:
    with path.open("rb") as handle:
        return tomllib.load(handle)


def _build_config(data: Mapping[str, Any], path: Path) -> Config:
    backloggd = data.get("backloggd")
    if not isinstance(backloggd, Mapping):
        raise ConfigError("Missing [backloggd] section.")
    username = backloggd.get("username")
    if not isinstance(username, str) or not username.strip():
        raise ConfigError("backloggd.username must be a non-empty string.")

    collection = str(backloggd.get("collection", "games")).strip()
    collection = collection.strip("/") or "games"

    cfg = Config(
        backloggd=BackloggdConfig(
            username=username.strip(),
            public_only=bool(backloggd.get("public_only", True)),
            collection=collection,
            host_override_ip=_optional_str(backloggd.get("host_override_ip")),
        ),
        hltb=_load_section(HLTBConfig, data.get("hltb", {})),
        match=_load_section(MatchConfig, data.get("match", {})),
        paths=_load_section(PathsConfig, data.get("paths", {})),
        export=_load_section(ExportConfig, data.get("export", {})),
        logging=_load_section(LoggingConfig, data.get("logging", {})),
        raw_path=path.resolve(),
    )
    return cfg


def _load_section(cls: type[Any], values: Mapping[str, Any] | None) -> Any:
    if not values:
        return cls()  # type: ignore[call-arg]
    init_args: dict[str, Any] = {}
    for field_name in cls.__dataclass_fields__:  # type: ignore[attr-defined]
        if field_name in values:
            init_args[field_name] = values[field_name]
    return cls(**init_args)  # type: ignore[arg-type]


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        trimmed = value.strip()
        return trimmed or None
    return str(value).strip() or None


def _validate_config(cfg: Config) -> None:
    if cfg.hltb.rate_limit_per_sec <= 0:
        raise ConfigError("hltb.rate_limit_per_sec must be positive.")
    if cfg.hltb.max_retries < 1:
        raise ConfigError("hltb.max_retries must be at least 1.")
    if not 0 <= cfg.match.fuzzy_queue_min <= cfg.match.fuzzy_auto <= 100:
        raise ConfigError("match.fuzzy_queue_min <= match.fuzzy_auto <= 100 must hold.")
    if not cfg.backloggd.collection:
        raise ConfigError("backloggd.collection must be a non-empty slug.")
    if cfg.backloggd.host_override_ip:
        cfg.backloggd.host_override_ip = cfg.backloggd.host_override_ip.strip()
        if not cfg.backloggd.host_override_ip:
            cfg.backloggd.host_override_ip = None


def ensure_directories(cfg: Config) -> None:
    for path in (cfg.cache_path(), cfg.export_path()):
        path.mkdir(parents=True, exist_ok=True)


def config_from_mapping(data: Mapping[str, Any]) -> Config:
    cfg_path = default_config_path()
    cfg = _build_config(data, cfg_path)
    _validate_config(cfg)
    return cfg


def apply_overrides(cfg: Config, overrides: Iterable[tuple[str, Any]]) -> Config:
    for key, value in overrides:
        top, _, sub = key.partition(".")
        if not sub:
            raise ConfigError(f"Invalid override key: {key}")
        section = getattr(cfg, top, None)
        if section is None:
            raise ConfigError(f"Unknown config section: {top}")
        if not hasattr(section, sub):
            raise ConfigError(f"Unknown config key: {key}")
        setattr(section, sub, value)
    _validate_config(cfg)
    return cfg
