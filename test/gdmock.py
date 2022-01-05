from csv import reader as csv_reader
from dataclasses import dataclass, field
from datetime import datetime, timezone
from faker import Faker
from pathlib import Path
from random import choices as random_choices, randint
from string import ascii_letters, digits

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

    kind: str = 'drive#file'
    id: str = field(default_factory=fake_file_id)
    name: str = field(
        default_factory=lambda: fake_string(ascii_letters, randint(1, 15)))
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
        self.exportLinks: dict[str, str] = {
            extension_to_mime_type[extension]:
            'https://docs.google.com/feeds/download/documents/export/'
            f'Export?id={self.id}&exportFormat={extension}'
            for extension in self._export_extensions
        }


_gw_files_prefix = 'application/vnd.google-apps.'

extension_to_mime_type: dict[str, str]
formats_csv = Path(__file__).parent.parent / 'blabgddatalake' / 'formats.csv'
with open(formats_csv, 'r') as csvfile:
    reader = csv_reader(csvfile)
    next(reader)
    extension_to_mime_type = {
        extension: mime_type
        for extension, mime_type in reader
    }


class GDGoogleDocsFileMock(GDGoogleWorkspaceFileMock):
    mimeType = _gw_files_prefix + 'document'
    _web_view_pattern: str = 'https://docs.google.com/document/d/{}/edit'
    _export_extensions = field(
        default_factory=lambda: 'rtf,odt,html,pdf,epub,html.zip,docx,txt'.
        split(','))


class GDGoogleDrawingsFileMock(GDGoogleWorkspaceFileMock):
    mimeType = _gw_files_prefix + 'drawing'
    _web_view_pattern: str = 'https://docs.google.com/drawings/d/{}/edit'
    _export_extensions = field(
        default_factory=lambda: 'svg,png,pdf,jpg'.split(','))


class GDGoogleFormsFileMock(GDGoogleWorkspaceFileMock):
    mimeType = _gw_files_prefix + 'form'
    _web_view_pattern: str = 'https://docs.google.com/forms/d/{}/edit'
    _export_extensions = field(default_factory=list)


class GDGoogleJamboardFileMock(GDGoogleWorkspaceFileMock):
    mimeType = _gw_files_prefix + 'jam'
    _web_view_pattern: str = 'https://jamboard.google.com/d/{}/edit'
    _export_extensions = field(default_factory=lambda: ['pdf'])


class GDGoogleSlidesFileMock(GDGoogleWorkspaceFileMock):
    mimeType = _gw_files_prefix + 'presentation'
    _web_view_pattern: str = 'https://docs.google.com/presentation/d/{}/edit'
    _export_extensions = field(
        default_factory=lambda: 'odp,pdf,pptx,txt'.split(','))


class GDGoogleMyMapsFileMock(GDGoogleWorkspaceFileMock):
    id: str = field(default_factory=fake_file_id)  # 33 chars
    mimeType = _gw_files_prefix + 'map'
    _web_view_pattern: str = 'https://www.google.com/maps/d/edit?mid={}'
    _export_extensions = field(default_factory=list)


class GDGoogleSheetsFileMock(GDGoogleWorkspaceFileMock):
    mimeType = _gw_files_prefix + 'spreadsheet'
    _web_view_pattern: str = 'https://docs.google.com/spreadsheets/d/{}/edit'
    _export_extensions = field(
        default_factory=lambda: 'ots,tsv,pdf,xlsx,csv,html.zip,ods'.split(','))


class GDGoogleSitesFileMock(GDGoogleWorkspaceFileMock):
    id: str = field(default_factory=fake_file_id)  # 33 chars
    mimeType = _gw_files_prefix + 'site'
    _web_view_pattern: str = 'https://sites.google.com/d/{}/edit'
    _export_extensions = field(default_factory=lambda: ['txt'])


class GDGoogleAppsScriptFileMock(GDGoogleWorkspaceFileMock):
    id: str = field(default_factory=lambda: fake_file_id(57))
    mimeType = _gw_files_prefix + 'script'
    _web_view_pattern: str = 'https://script.google.com/d/{}/edit'
    _export_extensions = field(default_factory=list)
