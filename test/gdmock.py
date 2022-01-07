import json
import re
from collections import defaultdict
from csv import reader as csv_reader
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from random import choices as random_choices
from random import randint
from string import ascii_letters, digits
from typing import Any
from urllib.parse import parse_qs, urlparse

from faker import Faker
from fieldparser import ALL_FIELDS, parse_fields
from googleapiclient.discovery import build
from httplib2 import Http, Response

from blabgddatalake.config import GoogleDriveConfig
from blabgddatalake.remote.gd import GoogleDriveService

fake = Faker()


def fake_string(chars: str, length: int) -> str:
    return ''.join(random_choices(chars, k=length))


def fake_file_id(length: int = 33) -> str:
    return fake_string(ascii_letters + digits + '_-', length)


def fake_revision_id() -> str:
    return fake_string(ascii_letters + digits + '_-', 51)


def fake_md5() -> str:
    return fake_string(digits + 'abcdef', 32)


@dataclass
class GDFileCapabilitiesMock:

    canAddChildren: bool = False
    canAddFolderFromAnotherDrive: bool = False
    canChangeCopyRequiresWriterPermission: bool = False
    canChangeSecurityUpdateEnabled: bool = False
    canChangeViewersCanCopyContent: bool = False
    canComment: bool = False
    canCopy: bool = True
    canDelete: bool = False
    canDeleteChildren: bool = False
    canDownload: bool = True
    canEdit: bool = False
    canListChildren: bool = True
    canModifyContent: bool = False
    canMoveChildrenOutOfTeamDrive: bool = False
    canMoveChildrenOutOfDrive: bool = False
    canMoveChildrenWithinTeamDrive: bool = False
    canMoveChildrenWithinDrive: bool = False
    canMoveItemIntoTeamDrive: bool = False
    canMoveItemOutOfTeamDrive: bool = False
    canMoveItemOutOfDrive: bool = False
    canMoveItemWithinTeamDrive: bool = False
    canMoveItemWithinDrive: bool = False
    canMoveTeamDriveItem: bool = False
    canReadRevisions: bool = False
    canReadTeamDrive: bool = True
    canReadDrive: bool = True
    canRemoveChildren: bool = False
    canRename: bool = False
    canShare: bool = False
    canTrash: bool = False
    canTrashChildren: bool = False
    canUntrash: bool = False


def timestamp(dt: datetime | None = None) -> str:
    if dt is None:
        dt = datetime.utcnow()
    else:
        dt = dt.astimezone().astimezone(timezone.utc)
    return dt.replace(tzinfo=None).isoformat(timespec='milliseconds') + 'Z'


@dataclass
class GDLastModifyingUserMock:
    kind: str = 'drive#user'
    displayName: str = field(default_factory=fake.name)
    photoLink: str = 'https://lh3.googleusercontent.com/a/default-user=s64'
    me: bool = False
    permissionId: str = field(default_factory=lambda: fake_string(digits, 20))


@dataclass
class GDFileMock:
    name: str = field(
        default_factory=lambda: fake_string(ascii_letters, randint(1, 15)))
    kind: str = 'drive#file'
    id: str = field(default_factory=fake_file_id)
    mimeType: str = 'application/octet-stream'
    parents: list[str] = field(default_factory=list)
    webViewLink: str = field(init=False)
    iconLink: str = field(init=False)
    createdTime: str = field(default_factory=lambda: timestamp())
    modifiedTime: str = field(default_factory=lambda: timestamp())
    lastModifyingUser: GDLastModifyingUserMock = field(
        default_factory=GDLastModifyingUserMock)
    capabilities: GDFileCapabilitiesMock = field(
        default_factory=GDFileCapabilitiesMock)

    _web_view_pattern: str = 'https://drive.google.com/file/d/{}/view'

    def __post_init__(self) -> None:
        self.iconLink = ('https://drive-thirdparty.googleusercontent.com'
                         f'/16/type/{self.mimeType}')
        self.webViewLink = self._web_view_pattern.format(self.id)


@dataclass
class GDDirectoryMock(GDFileMock):
    mimeType: str = 'application/vnd.google-apps.folder'

    _web_view_pattern: str = 'https://drive.google.com/drive/folders/{}'

    def __post_init__(self) -> None:
        super().__post_init__()


@dataclass
class GDRegularFileMock(GDFileMock):
    mimeType: str = 'application/octet-stream'
    size: str = field(default_factory=lambda: str(randint(1, 2000)))
    md5Checksum: str = field(default_factory=fake_md5)
    headRevisionId: str = field(default_factory=fake_revision_id)


@dataclass
class GDGoogleWorkspaceFileMock(GDFileMock):
    id: str = field(default_factory=lambda: fake_file_id(44))
    _export_extensions: list[str] = field(default_factory=list)
    exportLinks: dict[str, str] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        super().__post_init__()
        print(self.mimeType)
        self.exportLinks: dict[str, str] = {
            extension_to_mime_type[extension]:
            'https://docs.google.com/feeds/download/documents/export/'
            f'Export?id={self.id}&exportFormat={extension}'
            for extension in self._export_extensions
        }


_gw_files_prefix = 'application/vnd.google-apps.'

extension_to_mime_type: dict[str, str]
formats_csv = Path(__file__).parent.parent / 'blabgddatalake' / 'formats.csv'
with open(formats_csv) as csvfile:
    reader = csv_reader(csvfile)
    next(reader)
    extension_to_mime_type = {
        extension: mime_type
        for extension, mime_type in reader
    }


@dataclass
class GDGoogleDocsFileMock(GDGoogleWorkspaceFileMock):
    mimeType: str = _gw_files_prefix + 'document'
    _web_view_pattern: str = 'https://docs.google.com/document/d/{}/edit'
    _export_extensions: list[str] = field(
        default_factory=lambda: 'rtf,odt,html,pdf,epub,html.zip,docx,txt'.
        split(','))


@dataclass
class GDGoogleDrawingsFileMock(GDGoogleWorkspaceFileMock):
    mimeType: str = _gw_files_prefix + 'drawing'
    _web_view_pattern: str = 'https://docs.google.com/drawings/d/{}/edit'
    _export_extensions: list[str] = field(
        default_factory=lambda: 'svg,png,pdf,jpg'.split(','))


@dataclass
class GDGoogleFormsFileMock(GDGoogleWorkspaceFileMock):
    mimeType: str = _gw_files_prefix + 'form'
    _web_view_pattern: str = 'https://docs.google.com/forms/d/{}/edit'
    _export_extensions: list[str] = field(default_factory=list)


@dataclass
class GDGoogleJamboardFileMock(GDGoogleWorkspaceFileMock):
    mimeType: str = _gw_files_prefix + 'jam'
    _web_view_pattern: str = 'https://jamboard.google.com/d/{}/edit'
    _export_extensions: list[str] = field(default_factory=lambda: ['pdf'])


@dataclass
class GDGoogleSlidesFileMock(GDGoogleWorkspaceFileMock):
    mimeType: str = _gw_files_prefix + 'presentation'
    _web_view_pattern: str = 'https://docs.google.com/presentation/d/{}/edit'
    _export_extensions: list[str] = field(
        default_factory=lambda: 'odp,pdf,pptx,txt'.split(','))


@dataclass
class GDGoogleMyMapsFileMock(GDGoogleWorkspaceFileMock):
    id: str = field(default_factory=fake_file_id)  # 33 chars
    mimeType: str = _gw_files_prefix + 'map'
    _web_view_pattern: str = 'https://www.google.com/maps/d/edit?mid={}'
    _export_extensions: list[str] = field(default_factory=list)


@dataclass
class GDGoogleSheetsFileMock(GDGoogleWorkspaceFileMock):
    mimeType: str = _gw_files_prefix + 'spreadsheet'
    _web_view_pattern: str = 'https://docs.google.com/spreadsheets/d/{}/edit'
    _export_extensions: list[str] = field(
        default_factory=lambda: 'ots,tsv,pdf,xlsx,csv,html.zip,ods'.split(','))


@dataclass
class GDGoogleSitesFileMock(GDGoogleWorkspaceFileMock):
    id: str = field(default_factory=fake_file_id)  # 33 chars
    mimeType: str = _gw_files_prefix + 'site'
    _web_view_pattern: str = 'https://sites.google.com/d/{}/edit'
    _export_extensions: list[str] = field(default_factory=lambda: ['txt'])


@dataclass
class GDGoogleAppsScriptFileMock(GDGoogleWorkspaceFileMock):
    id: str = field(default_factory=lambda: fake_file_id(57))
    mimeType: str = _gw_files_prefix + 'script'
    _web_view_pattern: str = 'https://script.google.com/d/{}/edit'
    _export_extensions: list[str] = field(default_factory=list)


def to_dict(obj: object, fields: list[...]) -> dict[str, ...]:
    d: dict[str, Any] = defaultdict(dict)
    for k, field_list in fields:
        try:
            v = getattr(obj, k)
        except AttributeError:
            continue
        if field_list == ALL_FIELDS:
            d[k] = {
                key: value
                for key, value in asdict(v).items() if not key.startswith('_')
            } if is_dataclass(v) else v
        else:
            d[k].update(to_dict(v, field_list))
    return d


class GDHttpMock():
    """Mock of :cls:`httplib2.Http`."""

    def __init__(self, state: dict[str, GDFileMock] | None = None):
        self.state: dict[str, GDFileMock] = state or {}

    def request(
        self,
        uri: str,
        method: str = "GET",
        body: str | None = None,
        headers: dict[str, str] | None = None,
        redirections: int = 5,
        connection_type: str | None = None,
    ) -> tuple[Response, bytes]:
        o = urlparse(uri)
        q = parse_qs(o.query)
        fields = parse_fields(GoogleDriveService.FILE_FIELDS)
        if o.path == '/discovery/v1/apis/drive/v3/rest':
            with open('drive.v3.json') as f:
                return Response({}), f.read().encode('utf-8')
        if o.path.startswith('/drive/v3/files/'):
            file_id = o.path.rsplit('/', 1)[-1]
            try:
                file = self.state[file_id]
            except KeyError:
                return Response({'status': 404}), b''
            return Response({}), json.dumps(to_dict(file,
                                                    fields)).encode('utf-8')
        if o.path == '/drive/v3/files':
            m = re.match("not trashed and '([^']+)' in parents", q['q'][0])
            if not m:
                raise NotImplementedError
            parent_id = m.group(1)
            if parent_id not in self.state:
                return Response({'status': 404}), b''
            return Response({}), json.dumps({
                'files': [
                    to_dict(f, fields) for f in self.state.values()
                    if (p := f.parents) and p[0] == parent_id
                ]
            }).encode('utf-8')
        return Response({}), b'{}'


class GDServiceMock(GoogleDriveService):

    def __init__(self, gd_config: GoogleDriveConfig, http: Http):
        service = build('drive', 'v3', http=http, static_discovery=False)
        super().__init__(gd_config, http, service)
