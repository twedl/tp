from pathlib import Path

from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
    YamlConfigSettingsSource,
)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="MT_",
        env_file=".env",
        yaml_file="config.yaml",
        extra="ignore",
    )

    microtrade_yaml: Path
    workbooks_dir: Path
    raw_dir: Path
    specs_dir: Path
    processed_dir: Path
    spec_manifests_dir: Path
    raw_manifests_dir: Path
    upstream_raw_dir: Path
    raw_remote_dir: Path

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls,
        init_settings,
        env_settings,
        dotenv_settings,
        file_secret_settings,
    ):
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            YamlConfigSettingsSource(settings_cls),
            file_secret_settings,
        )


def load_config(yaml_path: Path | None = None) -> Settings:
    if yaml_path is None:
        return Settings()  # type: ignore[call-arg]

    class _FromPath(Settings):
        model_config = SettingsConfigDict(
            env_prefix="MT_",
            env_file=".env",
            yaml_file=str(yaml_path),
            extra="ignore",
        )

    return _FromPath()  # type: ignore[call-arg]
