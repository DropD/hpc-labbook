"""Data nodes for the AiiDA components of hpclb."""

from __future__ import annotations

import dataclasses
import pathlib
import typing

from aiida import engine
from cattrs.preconf.json import make_converter
from typing_extensions import Self

if typing.TYPE_CHECKING:
    from aiida.common import folders


__all__ = ["RemotePath", "TargetDir", "UploadFile", "create_dirs", "create_triplets"]


CONVERTER = make_converter()


class JsonableMixin:
    """
    Defines API required by 'aiida.orm.JsonableData'.

    Can be used to augment dataclasses or 'attrs' classes.
    """

    def as_dict(self: Self) -> dict[str, str]:
        return CONVERTER.unstructure(self)

    @classmethod
    def from_dict(cls: type[Self], data: dict[str, str]) -> Self:
        return CONVERTER.structure(data, cls)


@dataclasses.dataclass
class UploadFile(JsonableMixin):
    """Local file which should be uploaded under the name 'name'."""

    source: pathlib.Path
    input_label: str
    tgt_name: str


@dataclasses.dataclass
class RemotePath(JsonableMixin):
    """Remote path which should be copied or linked."""

    src_path: pathlib.Path
    tgt_name: str
    copy: bool


@dataclasses.dataclass
@dataclasses.dataclass
class TargetDir(JsonableMixin):
    """Subdirectory of the work dir on the cluster to be created before running."""

    name: str
    subdirs: list[TargetDir] = dataclasses.field(default_factory=list)
    upload: list[UploadFile] = dataclasses.field(default_factory=list)
    remote: list[RemotePath] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class UploadTriplet:
    """Structured representation of the data required for AiiDA to upload a file."""

    uuid: str
    src_name: str
    tgt_path: str


@dataclasses.dataclass
class RemoteTriplet:
    """Structured representation of the data required to copy or link a remote file."""

    uuid: str
    src_path: str
    tgt_path: str


def create_triplets(
    target_dir: TargetDir,
    calcjob: engine.CalcJob,
    path: list[str] | None = None,
    is_root: bool = True,
) -> tuple[list[UploadTriplet], list[RemoteTriplet], list[RemoteTriplet]]:
    """Create copy- and link list triplets for calcjob prep from target workdir."""
    path = path or []
    if not is_root:
        path.append(target_dir.name)
    local_copy: list[UploadTriplet] = []
    remote_copy: list[RemoteTriplet] = []
    remote_link: list[RemoteTriplet] = []
    for subdir in target_dir.subdirs:
        lc, rc, rl = create_triplets(
            target_dir=subdir, path=path, is_root=False, calcjob=calcjob
        )
        local_copy.extend(lc)
        remote_copy.extend(rc)
        remote_link.extend(rl)

    local_copy.extend(
        [
            UploadTriplet(
                uuid=calcjob.inputs.uploaded.get(file.input_label).uuid,
                src_name=file.source.name,
                tgt_path="/".join([*path, file.tgt_name]),
            )
            for file in target_dir.upload
        ]
    )

    remote_triplets = [
        (
            file.copy,
            RemoteTriplet(
                uuid=calcjob.inputs.code.computer.uuid,
                src_path=str(file.src_path),
                tgt_path="/".join([*path, file.tgt_name]),
            ),
        )
        for file in target_dir.remote
    ]

    remote_copy.extend([i[1] for i in remote_triplets if i[0]])
    remote_link.extend([i[1] for i in remote_triplets if not i[0]])

    return local_copy, remote_copy, remote_link


def create_dirs(
    target_dir: TargetDir,
    folder: folders.Folder,
    path: list[str] | None = None,
    is_root: bool = True,
) -> None:
    """Map the TargetDir hierarchy into the staging folder of a CalcJob."""
    path = path or []
    if not is_root:
        path.append(target_dir.name)
    for subdir in target_dir.subdirs:
        subfolder = folder.get_subfolder("/".join([*path, subdir.name]), create=True)
        print(f"created {subfolder.abspath}")
        create_dirs(target_dir=subdir, folder=subfolder, path=path, is_root=False)
