"""Functionality for working with projects."""

from __future__ import annotations

import dataclasses
import pathlib

import cattrs
from cattrs.preconf.pyyaml import make_converter
from typing_extensions import Self

from hpclb import cli_tools as ct
from hpclb.aiida.data import jobspec

__all__ = ["Auth", "Config", "Machine", "Project", "Site"]


@dataclasses.dataclass
class Auth:
    """Authentication information for one or more compute resoureces."""

    client_id: str
    billing_account: str
    client_secret: str


@dataclasses.dataclass
class Machine:
    """Compute resource as configured for a site."""

    auth: str | None = None


@dataclasses.dataclass
class Site:
    """Compute Site config for a project."""

    docs: str = ""
    machines: dict[str, Machine] = dataclasses.field(default_factory=dict)
    auths: dict[str, Auth] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass
class Config:
    """Project Configuration."""

    name: str
    sites: dict[str, Site] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass
class Project:
    """Represents a hpclb project."""

    path: pathlib.Path
    converter: cattrs.preconf.pyyaml.PyyamlConverter = dataclasses.field(
        default_factory=make_converter
    )
    offline_mode: bool = False

    @property
    def config_file(self: Self) -> pathlib.Path:
        """Location of the config file of this project."""
        return self.path / "hpclb.yaml"

    @property
    def config(self: Self) -> Config:
        """Read config object from file."""
        return self.converter.loads(self.config_file.read_text(), Config)

    @config.setter
    def config(self: Self, config: Config) -> None:
        """Store the config object to file."""
        self.config_file.write_text(self.converter.dumps(config))

    @property
    def aiida_dir(self: Self) -> pathlib.Path:
        """Location of the aiida config dir inside this project."""
        return self.path / ".aiida"

    def site_dir(self: Self, name: str) -> pathlib.Path:
        """Find site infrastructure directory for a particular site in this project."""
        return self.path / name

    @property
    def uv(self: Self) -> ct.Uv:
        """UV instance configured for this project."""
        return ct.Uv(project=self.path.absolute().resolve(), offline=self.offline_mode)

    @property
    def verdi(self: Self) -> ct.Verdi:
        """Verdi instance configured for this project."""
        return ct.Verdi(project=self.path)

    @property
    def spec_dir(self: Self) -> pathlib.Path:
        """Where the job specs are kept."""
        return self.path / "specs"

    def load_spec(self: Self, relpath: pathlib.Path) -> jobspec.Generic:
        """Load a job spec."""
        return self.converter.loads(
            (self.spec_dir / relpath).read_text(), jobspec.Generic
        )
